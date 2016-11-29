# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 14:52:08 2016

@author: tenz
"""
import threading
import time
from subprocess import call
from pds.Dequeue import Dequeue

class DataWorkflow:
    
    """sample ops:
        ls
        ls -l
        ls -lh
        ps aux | grep python
    """
    
    """
    busy waiting:
        process repeatedly checks to see if a condition is true,
        -if true, run it
    """
                
    class WorkNode:
        """simple work node class
            nid: id for node
            op: operation to execute
            dep: dependencies
        """

        def __init__(self, nid, op, dep):
            self.nid = nid
            self.op = op
            self.dep = dep
            self.next = set()
            self.depth = 1
        
        def add_child(self, node):
            self.next.add(node)
            
        def remove_child(self, node):
            self.next.remove(node)

    def __init__(self):
        self.head = []
        self.work_queue = Dequeue()
        #replace with sqlite3 db 
        #extend to have a online page to check status of db
        self.work_db = {}
        self.max_depth = 0

        """
        work db: | node_id | status | last_run (x) |
        status:
            - success
            - running
            - failure
            - ready
        """

    def addNode(self, nid, op, dep = None):
        """
        adds a node with dep where dep is a list of ids
        """
        node = self.WorkNode(nid, op, dep)
        self.updateDB(node, 'ready')
        
        if not dep:
            self.head.append(node)
        else:
            for parent_node_id in dep:
                print "searching for parent node %s" % parent_node_id
                for head_node in self.head:
                    print "starting with node %s" % head_node.nid
                    # .WorkNode instance
                    parent_node = self.searchNodeID(parent_node_id, head_node)
                    if parent_node:
                        parent_node.add_child(node)
                        
                        #use parent nodes depth to calculate current node depth
                        node.depth = parent_node.depth + 1
                        self.max_depth = max(self.max_depth, node.depth)

    def updateDB(self, node, status):
        if node.nid not in self.work_db.keys():
            self.work_db[node.nid] = status
        else:
            self.work_db[node.nid] = status

    def getStatus(self, node):
        return self.work_db[node.nid]

    def searchNodeID(self, nid, node):
        """given the starting node, find child node with the same nid"""
        if node.nid == nid:
            return node
        else:
            for child_node in node.next:
                search_node = self.searchNodeID(nid, child_node)
                if search_node:
                    return search_node
        return None

    def searchHeadNode(self, nid):
        for node in self.head:
            return self.searchNodeID(nid, node)

    def clearWorkFlow(self):
        self.head = []

    def getNodesFromDepth(self, depth):
        """returns all nodes at a certain depth
        """
        nodes = set()
        for node in self.head:
            (self._getNodesFromDepth(node, depth, nodes))
        return nodes

    def _getNodesFromDepth(self, node, depth, node_set):
        if node.depth == depth:
            node_set.add(node)
        elif node.next:
            for child_node in node.next:
                self._getNodesFromDepth(child_node, depth, node_set)
        return node_set

    def _should_refresh(self, node):
        if node.dep:
            for dep in node.dep:
                parent_node = self.searchHeadNode(dep)
                if self.getStatus(parent_node) != 'success':
                    return False
        if self.getStatus(node) in ('ready', 'failure'):
            return True
        else:
            return False

    def _refresh(self, node):
        try:
            status = call(node.op, shell=True)
            if status == 0:
                self.updateDB(node, 'success')
                return 'success'
            elif status == 1:
                self.updateDB(node, 'failure')
                return 'failure'
        except:
            return 'failure'

    def refresh(self, nid):
        """returns when dataset is refreshed - might take hours to return
        """
        node = self.searchHeadNode(nid)
        try:
            status = call(node.op, shell=True)
            if status == 0:
                self.updateDB(node, 'success')
                return 'success'
            elif status == 1:
                self.updateDB(node, 'failure')
                return 'failure'
        except:
            return 'failure'

    def refresh_with_dependencies(self, nid):
        """returns when all dependencies of a dataset is refreshed"""
        return True

    def addNodesToWorkQueue(self):
        """
        uses node depth to carefully enqueue each WorkNode
        to the work_queue so that items are placed in some what sorted
        order
        """
        depth = 0
        while depth < self.max_depth:
            depth += 1
            work_nodes = self.getNodesFromDepth(depth)
            for node in work_nodes:
                self.work_queue.enqueue(node, node.nid)

    def clearWorkQueue(self):
        self.work_queue = Dequeue()

    def start(self):
        self.addNodesToWorkQueue()
        while self.work_queue.poll():
            queue_node = self.work_queue.dequeue()
            t = threading.Thread(target=self.process, args=(queue_node,))
            t.start()

    def process(self, queue_node):
        work_node = queue_node.data
        while self.getStatus(work_node) != 'success':
            print 'starting node ' + work_node.nid
            time.sleep(5)
            self.updateDB(work_node, 'success')
            print 'finished node ' + work_node.nid
        return
    

    def run(self):
        """runner func to run multiple threads in parallel
        """
        node = self.work_queue.poll()
        print "Starting work on node" + node.nid
        self.process(node)
        print "Finished work on node" + node.nid
