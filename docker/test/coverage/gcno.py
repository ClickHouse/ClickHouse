import os.path
import enum
from io import BytesIO
import base64
import operator
import os
import sys
import struct

gcno_dir = "/home/clickhouse/cov/src/CMakeFiles/dbms.dir/Core/"
file_name = "NamesAndTypes.cpp.gcno"

"""
__copyright__ = "Copyright 2012-2020, Myron W Walker"
__email__ = "myron.walker@automationmojo.com"
"""

class GCovInfoBranch:
    def __init__(self, lineno, blockno, branchno, taken=None):
        self.line_number = lineno
        self.block_number = blockno
        self.branch_number = branchno
        self.taken_counter = taken

    def increment_taken_count(self, incVal):
        if self.taken_counter is None:
            self.taken_counter = incVal
        else:
            self.taken_counter = self.taken_counter + incVal
        return

class GCovInfoFunction:
    def __init__(self, funcname, lineno, executionCount=0):
        self.function_name = funcname
        self.line_number = lineno
        self.execution_count = executionCount

    def increment_execution_count(self, incVal):
        self.execution_count = self.execution_count + incVal
        return

class GCovInfoLine:
    def __init__(self, lineno, executionCount=0, checkSum=None):
        self.line_number = lineno
        self.execution_count = executionCount
        self.check_sum = checkSum

    def increment_execution_count(self, incVal):
        self.execution_count = self.execution_count + incVal
        return

    def set_check_sum(self, checkSum):
        self.check_sum = checkSum

class GCovInfoFileWriter:
    def __init__(self, traceFile, testname=None):
        """
            Valid 'testname' values consist of letters, numerical digits, and the underscore character.
        """
        self.test_name = ""
        self.trace_file = traceFile

        if testname is not None:
            self.test_name = testname

        return

    @staticmethod
    def get_sorted_keys(coverageMapItem):
        sortedKeys = []

        keyLookupDictionary = {}

        funcGraphs = coverageMapItem.function_graphs

        for fgraph in funcGraphs.values():
            origSrc = fgraph.orig_source
            fullSrc = fgraph.source

            keyLookupDictionary[origSrc] = fullSrc

        keyList = [k for k in keyLookupDictionary.keys()]
        keyList.sort()

        for okey in keyList:
            fkey = keyLookupDictionary[okey]
            sortedKeys.append(fkey)

        return sortedKeys

    TraceList = ["_sigbits",
                 "_sputc", 
                 "graph_loop_for",
                 "graph_nestedbranch_simple"]

    @staticmethod
    def trace_graph(funcGraph, dumpinfo=False):

        funcName = funcGraph.name

        if (GCovInfoFileWriter.TraceList is not None) and operator.contains(GCovInfoFileWriter.TraceList, funcName):
            dumpinfo = True

        if dumpinfo is not None:
            if dumpinfo:
                vprint ("")
                vprint ("Function (%s : %d): Solved=%s" % (funcGraph.name, funcGraph.indent, funcGraph.solved))
                vprint ("+--------------------------------------------------------------------------------------------------------+")
                vprint ("")

                lastBlockIndex = len(funcGraph.blocks)
                blockIndex = 0

                while blockIndex < lastBlockIndex:
                    block = funcGraph.blocks[blockIndex]

                    vprint ("    Block (%d): LineNo=%s IsBranchLanding=%s IsCallSite=%s IsExceptionLanding=%s IsReturnLanding=%s HasRelevantBranches=%s" % \
                        (blockIndex, block.line_no, block.is_branch_landing, block.is_call_site, block.is_exception_landing, block.is_return_landing, block.has_relevant_branches))

                    vprint ("        PRED ARCS:")

                    collectionLen = len(block.arcs_predecessors)
                    cindex = 0

                    if collectionLen > 0:
                        while cindex < collectionLen:
                            arc = block.arcs_predecessors[cindex]
                            vprint ("            (%d) - DestBlockNo=%d Fake=%s FallThru=%s OnTree=%s IsRelevantBranch=%s IsExceptionBranch=%s" % \
                                    (cindex, arc.dest_block, arc.has_flag_fake, arc.has_flag_fall_through, arc.has_flag_on_tree, arc.is_relevant_branch, arc.is_exception_branch))
                            cindex += 1
                    else:
                        vprint ("            (EMPTY)")

                    vprint ("        SUCC ARCS:")

                    collectionLen = len(block.arcs_successors)
                    cindex = 0

                    if collectionLen > 0:
                        while cindex < collectionLen:
                            arc = block.arcs_successors[cindex]
                            vprint ("            (%d) - DestBlockNo=%d Fake=%s FallThru=%s OnTree=%s IsRelevantBranch=%s IsReturnBranch=%s IsExceptionBranch=%s" % \
                                    (cindex, arc.dest_block, arc.has_flag_fake, arc.has_flag_fall_through, arc.has_flag_on_tree, arc.is_relevant_branch, arc.is_return_branch, arc.is_exception_branch))
                            cindex += 1
                    else:
                        vprint ("            (EMPTY)")

                    vprint ("        LINES:")

                    collectionLen = 0
                    if block.lines is not None:
                        collectionLen = len(block.lines)
                    else:
                        vprint ("            (EMPTY)")

                    cindex = 0

                    while(cindex < collectionLen):
                        line = block.lines[cindex]
                        vprint ("            (%d) - %s" % (line.number, line.content))
                        cindex += 1

                    vprint("")

                    blockIndex += 1
                    #end while(blockIndex < lastBlockIndex)

                blockIndex = 0

                vprint ("|  BLOCK  |  ARC  |  COUNTER  |")

                while blockIndex < lastBlockIndex:
                    block = funcGraph.blocks[blockIndex]

                    collectionLen = len(block.arcs_successors)
                    cindex = 0

                    if collectionLen > 0:
                        while cindex < collectionLen:
                            arc = block.arcs_successors[cindex]
                            arcIndexStr = ("%d" % cindex).center(7)
                            if arc.counter is None:
                                arcCounterStr = "     -     "
                            else:
                                arcCounterStr = ("%d" % arc.counter).center(11)
                            if cindex == 0:
                                blkIndexStr = ("%d" % blockIndex).center(9)
                            else:
                                blkIndexStr = "         "
                            vprint ("|%s|%s|%s|" % (blkIndexStr, arcIndexStr, arcCounterStr))
                            cindex += 1

                    blockIndex += 1
                    #end while(blockIndex < lastBlockIndex)

                vprint ("")
                vprint ("+--------------------------------------------------------------------------------------------------------+")
                vprint ("")
                vprint ("")
            #end if dumpinfo:
            else:
                vprint ("Function (%s : %d): Solved=%s" % (funcGraph.name, funcGraph.indent, funcGraph.solved))

        return

    def write_sourcefile_section(self, coverageMapItem, testname=None):
        """
            Takes a coverage map item and writes a source file section to the output tracefile. 
        """

        sectionTestname = self.test_name
        if testname is not None:
            sectionTestname = testname

        sourcesMap = coverageMapItem.sources_map

        sourcesMapKeys = GCovInfoFileWriter.get_sorted_keys(coverageMapItem)

        writableFileCount = 0
        for sourcefile in sourcesMapKeys:
            if os.path.isabs(sourcefile):
                writableFileCount += 1

        if writableFileCount > 0:
            # --------------------------------------------------
            # Write out the Testcase name
            # --------------------------------------------------
            self.trace_file.write("TN:%s\n" % self.test_name)

            for sourcefile in sourcesMapKeys:
                if not os.path.isabs(sourcefile):
                    continue

                sourceGraphs = sourcesMap[sourcefile]

                sourcefileWriter = GCovInfoSourcefileWriter(sourcefile, sectionTestname)

                for funcGraph in sourceGraphs.values():

                    if funcGraph.solved:
                        GCovInfoFileWriter.trace_graph(funcGraph)

                        sourcefileWriter.add_function_trace(funcGraph.name, funcGraph.line_no, funcGraph.execution_count)

                        blockWithLinesCounter = 0

                        lastBlockIndex = len(funcGraph.blocks)
                        blockIndex = 0
                        relevantBlockIndex = 0
                        relevantBranchIndex = 0

                        branchLineNo = None

                        while blockIndex < lastBlockIndex:
                            block = funcGraph.blocks[blockIndex]

                            hasLines = False
                            if (block.lines is not None) and (len(block.lines) > 0):
                                hasLines = True

                            blockExecCount = 0
                            for arc in block.arcs_successors:
                                taken = arc.counter
                                blockExecCount += taken

                            if hasLines:
                                for line in block.lines:
                                    lineNumber = line.number
                                    if lineNumber > 0:
                                        branchLineNo = lineNumber
                                        checksum = None
                                        sourcefileWriter.add_line_trace(line.number, blockExecCount, checksum)

                                relevantBlockIndex = 0
                                relevantBranchIndex = 0

                            if block.has_relevant_branches:
                                if block.is_call_site:
                                    for arc in block.arcs_successors:
                                        if not arc.has_flag_fake:
                                            relevantBranchIndex += 1
                                else:
                                    for arc in block.arcs_successors:
                                        takenCounter = arc.counter
                                        if blockExecCount > 0:
                                            sourcefileWriter.add_branch_trace(branchLineNo, relevantBlockIndex, relevantBranchIndex, takenCounter)
                                        else:
                                            sourcefileWriter.add_branch_trace(branchLineNo, relevantBlockIndex, relevantBranchIndex, None)

                                        relevantBranchIndex += 1
                                    relevantBlockIndex += 1
                            else:
                                if block.is_loop:
                                    loopToBlockNo = None
                                    for arc in block.arcs_successors:
                                        if arc.dest_block < blockIndex:
                                            loopToBlockNo = arc.dest_block - 1
                                            break

                                    if loopToBlockNo is not None:
                                        loopToBlock = funcGraph.blocks[loopToBlockNo]
                                        for line in loopToBlock.lines:
                                            branchLineNo = line.number
                                        for arc in block.arcs_successors:
                                            takenCounter = arc.counter
                                            if blockExecCount > 0:
                                                sourcefileWriter.add_branch_trace(branchLineNo, relevantBlockIndex, relevantBranchIndex, takenCounter)
                                            else:
                                                sourcefileWriter.add_branch_trace(branchLineNo, relevantBlockIndex, relevantBranchIndex, None)

                                            relevantBranchIndex += 1
                                        relevantBlockIndex += 1

                            blockIndex += 1
                            #end while(blockIndex < lastBlockIndex)
                    else:
                        GCovInfoFileWriter.trace_graph(funcGraph, True)

                sourcefileWriter.write_to(self.trace_file)

        return

GCOVIO_STRINGPADDING = ['\x00\x00\x00\x00', '\x00\x00\x00', '\x00\x00', '\x00']

class Magic:
    BE = b"gcno"
    LE = b"oncg"
    BBG = b"gbbg" #BBG_FILE_MAGIC

class Tag(enum.IntEnum):
    FUNCTION         = 0x01000000
    BLOCKS           = 0x01410000
    ARCS             = 0x01430000
    LINES            = 0x01450000
    COUNTER_BASE     = 0x01a10000
    OBJECT_SUMMARY   = 0xa1000000 # Obsolete
    PROGRAM_SUMMARY  = 0xa3000000

class GCovIOConst:

    GCOVIO_TAGTYPE_STR = { Tag.FUNCTION: "GCOV_TAG_FUNCTION", 
                           Tag.BLOCKS: "GCOV_TAG_BLOCKS", 
                           Tag.ARCS: "GCOV_TAG_ARCS", 
                           Tag.LINES: "GCOV_TAG_LINES", 
                           Tag.COUNTER_BASE: "GCOV_TAG_COUNTER_BASE", 
                           Tag.OBJECT_SUMMARY: "GCOV_TAG_OBJECT_SUMMARY", 
                           Tag.PROGRAM_SUMMARY: "GCOV_TAG_PROGRAM_SUMMARY" }

    PACKUINT32="<I"
    PACKUINT32_BIGENDIAN=">I"

    LOWORDERMASK =  0x00000000ffffffff
    HIGHORDERMASK = 0xffffffff00000000

    GCOV_FLAG_ARC_ON_TREE = 1
    GCOV_FLAG_ARC_FAKE = 2
    GCOV_FLAG_ARC_FALLTHROUGH = 4

class GCovCoverageMapItem:
    def __init__(self, dataLeaf, funcGraphs):
        self.data_leaf = dataLeaf
        self.function_graphs = {}
        self.sources_map = {}
        for fgraph in funcGraphs:
            self.function_graphs.update({fgraph.indent: fgraph})

            funcSource = fgraph.source

            if funcSource in self.sources_map:
                srcFuncGraphDict = self.sources_map[funcSource]
                srcFuncGraphDict.update({fgraph.indent: fgraph})
            else:
                srcFuncGraphDict = {fgraph.indent: fgraph}
                self.sources_map.update({funcSource: srcFuncGraphDict})

        self.object_traces = None
        self.program_traces = None

class GCovTraceSummary:
    def __init__(self, funcTraces, objectTraces, programTraces):
        self.func_traces = funcTraces
        self.object_traces = objectTraces
        self.program_traces = programTraces

class GCovFunctionTrace:
    def __init__(self, ident, checksum, counters = None):
        self.indent = ident
        self.check_sum = checksum
        self.counters = counters
        return

    def apply_arc_counters(self, counters):
        self.counters = counters
        return

class GraphArc():
    """
        uint32:dest_block uint32:flags
    """
    def __init__(self, dest_block, flags):
        self.dest_block = dest_block
        self.flags = flags
        self.arc_id = None
        self.counter = None

        self.has_flag_fake = flags & GCovIOConst.GCOV_FLAG_ARC_FAKE
        self.has_flag_fall_through = flags & GCovIOConst.GCOV_FLAG_ARC_FALLTHROUGH
        self.has_flag_on_tree = flags & GCovIOConst.GCOV_FLAG_ARC_ON_TREE

        self.is_relevant_branch = False
        self.is_return_branch = False
        self.is_exception_branch = False

class GraphBlock():
    def __init__(self, block_number):
        self.block_number = block_number
        self.lines = None
        self.line_no = 0

        self.arcs_successors = []
        self.arcs_predecessors = []

        self.is_branch_landing = False
        self.is_call_site = False
        self.is_loop = False
        self.is_exception_landing = False
        self.is_return_landing = False
        self.has_relevant_branches = False
        return

    def Print(self):
        print("    GCovGraphBlock (%d):" % self.block_number)
        print("        HasRelevantBranches=%r" % self.has_relevant_branches)

        if self.lines is not None:
            for line in self.lines:
                line.Print()
            print("")

        return

class GraphFunction():
    def __init__(self, ident, name, filename, start_line):
        self.ident = ident
        self.name = name
        self.filename = filename
        self.start_line = start_line

        self.blocks = None
        self.block_count = 0
        self.execution_count = 0

        self.has_catch = False

        self.solved = False

    def Print(self):
        print("===================================================================")
        print("GCovGraphFunction:")
        print("    Ident=%d" % self.ident)
        print("    Name=%s" % self.name)
        print("    Source=%s" % self.filename)
        print("    LineNo=%d" % self.start_line)
        print("")

        for blk in self.blocks:
            blk.Print()

        print("===================================================================")
        print("")

    def set_arcs_for_bb(self, next_arc_id, block_no, arcs):
        block = self.blocks[block_no]
        block.arcs_successors = arcs

        for arc in arcs:
            arc.arc_id = next_arc_id
            next_arc_id += 1

            block_no = arc.dest_block
            block = self.blocks[block_no]
            block.arcs_predecessors.append(arc)

        return next_arc_id

    def set_lineset_for_bb(self, block_no, lines):
        self.blocks[block_no].lines = lines

    def set_basic_blocks(self, bb_count):
        self.blocks = [GraphBlock(i + 1) for i in range(bb_count)]

    WALKSTATE_NONE = 0
    WALKSTATE_BRANCHCHAIN = 1

    WALKSTATE_STRINGS = ["WALKSTATE_NONE", "WALKSTATE_BRANCHCHAIN"]

    def set_post_processed_fields(self):
        print("Post Processing Graph for '%s'" % self.name)

        if self.name == "_sputc":
            pass

        blocksList = self.blocks
        blocksLen = len(self.blocks)

        lastBlockNo = blocksLen - 1
        lastBlock = blocksList[lastBlockNo]

        blockIndex = 0

        fakePredCount = 0
        for parc in lastBlock.arcs_predecessors:
            if parc.has_flag_fake:
                fakePredCount = fakePredCount + 1

        exceptionBlock = None
        exceptionBlockNo = None

        if fakePredCount >= (len(lastBlock.arcs_predecessors) - 1):
            exceptionBlock = lastBlock
            exceptionBlock.is_exception_landing = True
            exceptionBlockNo = lastBlockNo

        returnBlock = None
        returnBlockNo = None

        if fakePredCount == 0:
            returnBlock = lastBlock
            returnBlock.is_return_landing = True
            returnBlockNo = lastBlockNo
        else:
            returnBlockNo = lastBlockNo - 1
            returnBlock = blocksList[returnBlockNo]
            returnBlock.is_return_landing = True

        # -------------------------------------------------------------------------
        # Find and mark all the call site blocks and all the branches that are 
        # return branches
        # -------------------------------------------------------------------------
        while(blockIndex < blocksLen):
            nxtBlock = blocksList[blockIndex]

            succLen = len(nxtBlock.arcs_successors)
            predLen = len(nxtBlock.arcs_predecessors)

            setCallBranch = False

            for sarc in nxtBlock.arcs_successors:
                destBlockNo = sarc.dest_block
                if sarc.has_flag_fake and (exceptionBlock is not None) and (destBlockNo == exceptionBlock.block_number):
                    sarc.is_exception_branch = True
                    nxtBlock.is_call_site = True
                    setCallBranch = True

                if sarc.dest_block == returnBlockNo:
                    sarc.is_return_branch = True

            noFakePredArcs = True
            for parc in nxtBlock.arcs_predecessors:
                if parc.has_flag_fake:
                    noFakePredArcs = False

            if (predLen > 1) and noFakePredArcs:
                nxtBlock.is_branch_landing = True

            blockIndex += 1

        # -------------------------------------------------------------------------
        # Find and mark all the blocks that have relevant branches
        # -------------------------------------------------------------------------
        blockIndex = 0

        walkStack = []
        walkState = GraphFunction.WALKSTATE_NONE

        nxtBlock = None
        prevBlock = None

        while(blockIndex < blocksLen):
            prevBlock = nxtBlock
            nxtBlock = blocksList[blockIndex]

            print ("Block(%d) - %s" % (blockIndex, GraphFunction.WALKSTATE_STRINGS[walkState]))

            hasLines = False
            if (nxtBlock.lines is not None) and (len(nxtBlock.lines) > 0):
                hasLines = True

            successorCount = len(nxtBlock.arcs_successors)

            #-------------------------------------------------------------------------------
            # This path is for processing sequences that began with a call-site
            #-------------------------------------------------------------------------------
            if walkState == GraphFunction.WALKSTATE_BRANCHCHAIN:
                if hasLines or successorCount < 2:
                    if len(walkStack) > 0:
                        if not prevBlock.is_call_site:
                            will_branch = len(prevBlock.arcs_successors) > 1

                            if will_branch:
                                while(True):
                                    if len(walkStack) == 0:
                                        break

                                    sidx, sblk = walkStack.pop()

                                    sblk.has_relevant_branches = True
                                    for arc in sblk.arcs_successors:
                                        arc.is_relevant_branch = True
                                    #end while(True):
                        else:
                            while(True):
                                if len(walkStack) == 0:
                                    break

                                sidx, sblk = walkStack.pop()
                                #end while(True):
                    walkStack = []
                    walkState = GraphFunction.WALKSTATE_NONE
                else:
                    walkStack.append((blockIndex, nxtBlock))

            #-------------------------------------------------------------------------------
            # This path initiates the start of the processing of a decision chain
            #-------------------------------------------------------------------------------
            if walkState == GraphFunction.WALKSTATE_NONE:
                #---------------------------------------------------------------------------------
                # If a block has lines and has more than one successor arc, it must be the start
                # of a relevant branch chain.
                #---------------------------------------------------------------------------------
                if hasLines:
                    if (successorCount > 1):
                            walkState = GraphFunction.WALKSTATE_BRANCHCHAIN
                            walkStack.append((blockIndex, nxtBlock))
                    else:
                        walkState = GraphFunction.WALKSTATE_NONE
                else:
                    if (successorCount > 1):
                        isloop = False
                        for arc in nxtBlock.arcs_successors:
                            if arc.dest_block < blockIndex:
                                isloop = True

                        if isloop:
                            nxtBlock.is_loop = True
                    else:
                        walkState = GraphFunction.WALKSTATE_NONE

            blockIndex += 1


    def solve_graph(self):
        """
                 Solves the function graph for the missing arc counts using the counts provided to apply_counters.  solve_graph 
            requires that at least one call to apply_counters has been made with the correct counter data.  You can make reapeated
            calls to apply_counters to apply multiple counter data sets.  Then call solve_graph to solve the graph.  To empty the
            graph counters use reset_counters.
        """

        block = None

        # If the there is a counter value for the first block, then the block was entered
        # and we can solve the graph
        zeroBlock = self.blocks[0]
        if zeroBlock.arcs_successors[0].counter is not None:

            #Next solve the graph for the unknown counters
            revisitList = []
            revisitList.extend(self.blocks)

            lastRevisitCount = 0
            blocksToSolve = []

            while(True):

                if (len(blocksToSolve) == 0):
                    revisitCount = len(revisitList)

                    if (revisitCount > 0):
                        if (revisitCount == lastRevisitCount):
                            raise IndexError("ERROR: Unsolvable function graph.")

                        lastRevisitCount = revisitCount

                        blocksToSolve = revisitList
                        revisitList = []
                    else:
                        break

                block = blocksToSolve.pop()

                predecessorUnknown = 0
                successorUnknown = 0

                predecessorCount = len(block.arcs_predecessors)
                successorCount = len(block.arcs_successors)

                for arc in block.arcs_predecessors:
                    if arc.counter is None:
                        predecessorUnknown += 1

                for arc in block.arcs_successors:
                    if arc.counter is None:
                        successorUnknown += 1

                # If a block has no predecessors or no successors we have to solve its arc counts from another block, no need to revisit
                if (predecessorCount == 0) or (successorCount == 0):
                    pass

                # If a block has an unknown on both sides then put the block in the revisit list
                elif ((predecessorUnknown > 0) and (successorUnknown > 0)):
                    revisitList.append(block)

                # If a block only has one unknown and it is an unknown predecessor then solve it here
                elif (predecessorUnknown == 1):
                    succSum = 0
                    for arc in block.arcs_successors:
                        succSum += arc.counter

                    predSum = 0

                    unknownArc = None
                    for arc in block.arcs_predecessors:
                        if arc.counter is None:
                            unknownArc = arc
                        else:
                            predSum += arc.counter

                    unknownArc.counter = succSum - predSum

                # If a block only has one unknown and it is an unknown successsor then solve it here
                elif successorUnknown == 1:
                    predSum = 0
                    for arc in block.arcs_predecessors:
                        predSum += arc.counter

                    succSum = 0

                    unknownArc = None
                    for arc in block.arcs_successors:
                        if arc.counter is None:
                            unknownArc = arc
                        else:
                            succSum += arc.counter

                    unknownArc.counter = predSum - succSum

                # Otherwise the block has two unsolved arcs on one side of the block, push it to the revisit list
                else:
                    revisitList.append(block)

                #end while(True)
            blocksLen = len(self.blocks)
            if blocksLen > 0:
                self.execution_count = 0

                executionCount = 0

                entryBlock = self.blocks[0]
                for arc in entryBlock.arcs_successors:
                    arcCounter = arc.counter
                    executionCount += arcCounter

                self.execution_count = executionCount
        
        else:
            # If there was no counter on the zeroBlock then the method has not been entered
            # so we can just zero all the couters.
            for nxtBlock in self.blocks:
                for arc in nxtBlock.arcs_predecessors:
                    arc.counter = 0
                for arc in nxtBlock.arcs_successors:
                    arc.counter = 0


        

        self.solved = True

        return sum

    @staticmethod
    def has_relevant_descendant_block_call_chain(blockList, blockListLen, block, blockIndex):

        isRelevant = False

        followOnBlock = None
        followOnBlockNo = None

        for sarc in block.arcs_successors:
            if not sarc.has_flag_fake:
                followOnBlockNo = sarc.dest_block
                followOnBlock = blockList[followOnBlockNo]

        if (followOnBlock is not None):
            if followOnBlock.is_call_site:
                if (followOnBlock.lines is None) or (len(followOnBlock.lines) == 0):
                    isRelevant = GCovGraphFunction.has_relevant_descendant_block_call_chain(blockList, blockListLen, followOnBlock, followOnBlockNo)
            else:
                if len(followOnBlock.arcs_successors) > 1:
                    isRelevant = True

            if isRelevant:
                followOnBlock.has_relevant_branches = True
                for sarc in followOnBlock.arcs_successors:
                    sarc.is_relevant_branch = True

        return isRelevant

    @staticmethod
    def has_relevant_descendant_block_else(blockList, blockListLen, block, blockIndex):

        isRelevant = False

        followOnBlock = None
        followOnBlockNo = None

        for sarc in block.arcs_successors:
            if not sarc.has_flag_fake:
                followOnBlockNo = sarc.dest_block
                followOnBlock = blockList[followOnBlockNo]

        if (followOnBlock is not None):
            if followOnBlock.is_call_site:
                if (followOnBlock.lines is None) or (len(followOnBlock.lines) == 0):
                    isRelevant = GCovGraphFunction.has_relevant_descendant_block_call_chain(blockList, blockListLen, followOnBlock, followOnBlockNo)
            else:
                if len(followOnBlock.arcs_successors) > 1:
                    isRelevant = True

            if isRelevant:
                followOnBlock.has_relevant_branches = True
                for sarc in followOnBlock.arcs_successors:
                    sarc.is_relevant_branch = True

        return isRelevant

class GCovNotesFile:
    def __init__(self, filename):
        self.filename = filename

        self.graphs = []

        with open(self.filename, "rb") as f:
            self.size = os.fstat(f.fileno()).st_size

            self.load_file_header(f)
            self.load_records_and_graphs(f)

    def parse_version(self, version_bin):
        if self.is_le:
            version_bin = version_bin[::-1]

        v = version_bin.decode('utf-8')

        if v[0] >= 'A':
            ver = (ord(v[0]) - ord('A')) * 100 + (ord(v[1]) - ord('0')) * 10 + ord(v[2]) - ord('0');
        else:
            ver = (ord(v[0]) - ord('0')) * 10 + ord(v[2]) - ord('0');

        if ver != 48:
            raise Exception("Invalid version")

    def load_file_header(self, f):
        magic = self.read_quad_char(f)

        if magic == Magic.BE:
            self.pack_str32 = GCovIOConst.PACKUINT32_BIGENDIAN
            self.is_le = False
        elif magic == Magic.LE:
            self.pack_str32 = GCovIOConst.PACKUINT32
            self.is_le = True
        else:
            print("Unknown endianess")
            sys.exit(1)

        self.parse_version(self.read_quad_char(f))
        self.read_uint32(f) # stamp

    def load_records_and_graphs(self, f):
        pos = f.tell()

        current_graph = None
        next_arc_id = 0

        while pos < self.size:
            if self.size - pos < 8:
                raise Exception("Invalid size")

            tag, word_len = self.read_uint32(f), self.read_uint32(f)
            record_buf = BytesIO(f.read(word_len * 4))

            if tag == Tag.FUNCTION:
                next_arc_id = 0

                ident, name, filename, start_line = self.read_func_record(record_buf)
                self.graphs.append(GraphFunction(ident, name, filename, start_line))

                current_graph = self.graphs[-1]
            elif tag == Tag.BLOCKS:
                for _ in range(word_len):
                    self.read_uint32(record_buf) # ignored block flags

                current_graph.set_basic_blocks(word_len)
            elif tag == Tag.ARCS:
                block_no, arcs = self.read_arcs_record(record_buf, word_len)

                next_arc_id = current_graph.set_arcs_for_bb(next_arc_id, block_no, arcs)
            elif tag == Tag.LINES:
                block_no, lines = self.read_lines_record(record_buf)
                current_graph.set_lineset_for_bb(block_no, lines)
            else:
                break

            pos = f.tell()

        for g in self.graphs:
            g.set_post_processed_fields()

    @staticmethod
    def read_quad_char(f):
        data = f.read(4)
        assert len(data) == 4
        return data

    def read_uint32(self, f):
        data = f.read(4)
        assert len(data) == 4
        return struct.unpack(self.pack_str32, data)[0]

    def read_uint64(self, f):
        lo, hi = read_uint32(f), read_uint32(f)
        return (hi << 32) | lo

    def read_string(self, f):
        word_len = self.read_uint32(f)
        return f.read(word_len * 4).rstrip(b"\0")

    def read_func_record(self, f):
        ident = self.read_uint32(f)
        _, _ = self.read_uint32(f), self.read_uint32(f) # ignore line and cgs checksum
        name, filename = self.read_string(f), self.read_string(f)
        start_line = self.read_uint32(f)

        return ident, name, filename, start_line

    def read_lines_record(self, f):
        block_no = self.read_uint32(f)

        line_set = []

        while True:
            line = self.read_uint32(f)

            if line != 0:
                line_set.append(line)
            else:
                self.read_string(f) #line_str
                break

        return block_no, line_set

    def read_arcs_record(self, f, record_len):
        block_no = self.read_uint32(f)
        arc_set = []

        for _ in range(int((record_len - 1) / 2)):
            dest_block, flags = self.read_uint32(f), self.read_uint32(f)
            arc_set.append(GraphArc(dest_block, flags))

        return block_no, arc_set

class GCovProcessor:
    def __init__(self, basePathDataList = None, basePathGraphList=None, basePathCodeList=None, 
                 basePathIncludeList=None, includeFilter=None, excludeFilter=None):
        self.base_path_data_list = basePathDataList
        self.base_path_graph_list = basePathGraphList
        self.base_path_code_list = basePathCodeList
        self.base_path_include_list = basePathIncludeList

        self.include_filters = []
        self.exclude_filters = []

        if includeFilter is not None:
            self.include_filters = includeFilter.split(':')

        if excludeFilter is not None:
            self.exclude_filters = excludeFilter.split(':')

        self.coverage_map = {}

        return

    def add_base_path_code(self, path):
        if self.base_path_code_list is None:
            self.base_path_code_list = []

        self.base_path_code_list.append(path)

        return

    def add_base_path_data(self, path):
        if self.base_path_data_list is None:
            self.base_path_data_list = []

        self.base_path_data_list.append(path)

        return

    def add_base_path_graph(self, path):
        if self.base_path_graph_list is None:
            self.base_path_graph_list = []

        self.base_path_graph_list.append(path)

        return

    def add_base_path_include(self, path):
        if self.base_path_include_list is None:
            self.base_path_include_list = []

        self.base_path_include_list.append(path)

        return

    def generate_trace_file(self, traceFilename, testname=None):
        """
            TODO: Code the generate output method
        """
        traceFile = None

        try:
            traceFile = open(traceFilename, 'w')

            infoWriter = GCovInfoFileWriter(traceFile, testname)

            for coverageMapItem in self.coverage_map.values():
                infoWriter.write_sourcefile_section(coverageMapItem)

        finally:
            if traceFile is not None:
                traceFile.close()

        return

    def populate_coverage_map(self, reset=True):

        if reset:
            self.reset()

        for basePathData in self.base_path_data_list:
            absBasePathData = os.path.abspath(basePathData)

            scanContext = GCovScanContext(self.include_filters, self.exclude_filters)

            for currentDir, dirList, fileList in os.walk(absBasePathData):
                self._scan_for_data_files(scanContext, currentDir, fileList)

            dataFileList = scanContext.files_found

            #Process all the data files
            for dataFilePath, dataFileName in dataFileList:
                self._process_data_file(absBasePathData, dataFilePath, dataFileName)

            #Go through all of the coverage map items and resolve the unknown counts
            for key in self.coverage_map.keys():
                coverageitem = self.coverage_map[key]
                for funcGraph in coverageitem.function_graphs.values():
                    try:
                        funcGraph.solve_graph()
                    except Exception as xcpt:
                        err_msg = str(xcpt)
                        print(err_msg, file=sys.stderr)

        return

    def _export_trace_file(self, dataFileLeaf):
        """
            TODO: Continue implementing this method
        """
        df_leaf, df_ext = os.path.splitext(dataFileLeaf)

        #Open the .info file
        for base_path in self.base_path_data_list:
            infoFilePath = os.path.join(base_path, df_leaf + ".info")
            info_fh = None

            coverageMap = None

            with open(infoFilePath, 'w') as info_fh:
                #Go through the coverage map items and write the information to the info file
                info_writer = GCovInfoFileWriter("NA")
                info_writer.write_sourcefile_section(coverageMap)

        return

    def _process_data_file(self, basePathData, dataFileDir, dataFileName):

        #Break the dataFileLeaf path into its components
        df_base, df_ext = os.path.splitext(dataFileName)

        df_fullpathfile = os.path.join(dataFileDir, dataFileName)
        df_leafpathfile = df_fullpathfile[len(basePathData) + 1:]

        df_fullbasefile = os.path.join(dataFileDir, df_base)
        df_leafbasefile = df_fullbasefile[len(basePathData) + 1:]

        coverageItem = None

        if df_leafpathfile not in self.coverage_map:
            #Pull in the GCNO graph for the data file
            graphFilename = self._locate_graph_file(df_fullbasefile, df_leafbasefile)
            if graphFilename is not None:
                gf = GCovNotesFile(graphFilename)
                gf.load()

                funcGraphs = gf.pull_graphs()

                #Go through the function graphs and locate the source files
                for fgraph in funcGraphs:
                    sourceFile = self._locate_source_file(df_fullbasefile, df_leafbasefile, fgraph.source)
                    if (sourceFile is not None) and os.path.exists(sourceFile):
                        fgraph.source = sourceFile

                coverageItem = GCovCoverageMapItem(df_leafpathfile, funcGraphs)

                self.coverage_map[df_leafpathfile] = coverageItem
            else:
                #raise IOError("Could not locate gcno file for datafile=%s" % df_leafpathfile)
                print("Could not locate gcno file for datafile=%s" % df_leafpathfile)

        else:
            coverageItem = self.coverage_map[df_leafpathfile]

        if coverageItem is not None:
            #Process the data file and store the counters in coverage map
            df = GCovDataFile(df_fullpathfile)
            df.load()

            traceSummary = df.create_trace_summary()

            functionGraphs = coverageItem.function_graphs

            for funcTrace in traceSummary.func_traces:
                funcIdent = funcTrace.indent

                if funcIdent in functionGraphs:
                    funcGraph = functionGraphs[funcIdent]
                    funcGraph.apply_counters(funcTrace.counters)
                else:
                    print("Function Identity (%s) not found..." % funcIdent)

            coverageItem.object_traces = traceSummary.object_traces
            coverageItem.program_traces = traceSummary.program_traces

        return

def parse_file(parseFile):
    fileRoot, fileExt = os.path.splitext(parseFile)
    filePath = os.path.abspath(parseFile)

    print("Processing .gcno file...")

    gcno = GCovNotesFile(filePath)

    for g in gcno.graphs:
        g.Print()

if __name__ == "__main__":
    parse_file(gcno_dir + '/' + file_name)
