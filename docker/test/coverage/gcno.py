import os.path
import enum
from io import BytesIO


gcno_dir = "/home/clickhouse/cov/src/CMakeFiles/dbms.dir/Core/"
file_name = "NamesAndTypes.cpp.gcno"

"""
Records are not nested, but there is a record hierarchy.  Tag numbers reflect this hierarchy.  Tags are unique
across note and data files.  Some record types have a varying amount of data.  The LENGTH is the number of 4bytes
that follow and is usually used to determine how much data.  The tag value is split into 4 8-bit fields, one for
each of four possible levels.  The most significant is allocated first.  Unused levels are zero.  Active levels are
odd-valued, so that the LSB of the level is one.  A sub-level incorporates the values of its superlevels.  This
formatting allows you to determine the tag hierarchy, without understanding the tags themselves, and is similar
to the standard section numbering used in technical documents.  Level values [1..3f] are used for common tags, 
values [41..9f] for the notes file and [a1..ff] for the data
file.

   The basic block graph file contains the following records
       note: unit function-graph*
          unit: header uint32:checksum string:source
          function-graph: announce_function basic_blocks {arcs | lines}*
              announce_function: header uint32:ident uint32:checksum string:name string:source uint32:lineno
              basic_block: header uint32:flags*
          arcs: header uint32:block_no arc*
              arc:  uint32:dest_block uint32:flags
          lines: header uint32:block_no line*
               uint32:0 string:NULL
              line:  uint32:line_no | uint32:0 string:filename

The BASIC_BLOCK record holds per-bb flags.  The number of blocks can be inferred from its data length.  There is
one ARCS record per basic block.  The number of arcs from a bb is implicit from the data length.  It enumerates the 
destination bb and per-arc flags. There is one LINES record per basic block, it enumerates the source lines which 
belong to that basic block.  Source file names are introduced by a line number of 0, following lines are from the new
source file.  The initial source file for the function is NULL, but the current source file should be remembered from one
LINES record to the next.  The end of a block is indicated by an empty filename - this does not reset the current source
file.  Note there is no ordering of the ARCS and LINES records: they may be in any order, interleaved in any manner.
The current filename follows the order the LINES records are stored in the file, *not* the ordering of the blocks they
are for.

   The data file contains the following records.
        data: {unit function-data* summary:object summary:program*}*
    unit: header uint32:checksum

        function-data:    announce_function arc_counts
    announce_function: header uint32:ident uint32:checksum
    arc_counts: header uint64:count*

    summary: uint32:checksum {count-summary} GCOV_COUNTERS
    count-summary:    uint32:num uint32:runs uint64:sum
            uint64:max uint64:sum_max

The ANNOUNCE_FUNCTION record is the same as that in the note file, but without the source location.  The ARC_COUNTS 
gives the counter values for those arcs that are instrumented.  The SUMMARY records give information about the whole
object file and about the whole program.  The checksum is used for whole program summaries, and disambiguates different
programs which include the same instrumented object file.  There may be several program summaries, each with a unique
checksum.  The object summary's checksum is zero.  Note that the data file might contain information from several runs
concatenated, or the data might be merged.

This file is included by both the compiler, gcov tools and the runtime support library libgcov. IN_LIBGCOV and IN_GCOV
are used to distinguish which case is which.  If IN_LIBGCOV is nonzero, libgcov is being built. If IN_GCOV is nonzero,
the gcov tools are being built. Otherwise the compiler is being built. IN_GCOV may be positive or negative. If positive,
we are compiling a tool that requires additional functions (see the code for knowledge of what those functions are).

__copyright__ = "Copyright 2012-2020, Myron W Walker"
__email__ = "myron.walker@automationmojo.com"
"""

import base64
import operator
import os
import sys
import struct

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

class GCovInfoSourcefileWriter:
    def __init__(self, sourcefile, testname=None):
        self.test_name = ""
        self.sourcefile = sourcefile
        self.branch_traces = []
        self.function_traces = []
        self.line_traces = []

        if testname is not None:
            self.test_name = testname

        return

    def add_branch_trace(self, lineno, blockno, branchno, taken=None):
        branchTrace = GCovInfoBranch(lineno, blockno, branchno, taken)
        self.branch_traces.append(branchTrace)
        return branchTrace

    def add_function_trace(self, funcname, lineno, executionCount=0):
        funcTrace = GCovInfoFunction(funcname, lineno, executionCount)
        self.function_traces.append(funcTrace)

        return funcTrace

    def add_line_trace(self, lineno, executionCount=0, checkSum=None):
        lineTrace = GCovInfoLine(lineno, executionCount, checkSum)
        self.line_traces.append(lineTrace)
        return lineTrace

    def write_to(self, fileHandle):
        """
        """
        # --------------------------------------------------
        # Write out the Sourcefile name
        # --------------------------------------------------
        sourcefile = self.sourcefile
        fileHandle.write("SF:%s\n" % sourcefile)

        # --------------------------------------------------
        # Write out the Function traces
        # --------------------------------------------------
        functionsCount = len(self.function_traces)
        functionsHit = 0

        for ftrace in self.function_traces:
            fileHandle.write("FN:%d,%s\n" % (ftrace.line_number, ftrace.function_name))

        for ftrace in self.function_traces:
            if (ftrace.execution_count > 0):
                functionsHit = functionsHit + 1
            fileHandle.write("FNDA:%d,%s\n" % (ftrace.execution_count, ftrace.function_name))

        fileHandle.write("FNF:%d\n" % functionsCount)
        fileHandle.write("FNH:%d\n" % functionsHit)

        # --------------------------------------------------
        # Write out the Branch traces
        # --------------------------------------------------
        branchesCount = len(self.branch_traces)
        branchesHit = 0

        if branchesCount > 0:
            for btrace in self.branch_traces:
                if (btrace.taken_counter is not None):
                    branchesHit = branchesHit + 1
                    fileHandle.write("BRDA:%d,%d,%d,%d\n" % (btrace.line_number, btrace.block_number, btrace.branch_number, btrace.taken_counter))
                else:
                    fileHandle.write("BRDA:%d,%d,%d,-\n" % (btrace.line_number, btrace.block_number, btrace.branch_number))

            fileHandle.write("BRF:%d\n" % branchesCount)
            fileHandle.write("BRH:%d\n" % branchesHit)

        # --------------------------------------------------
        # Write out the Line traces
        # --------------------------------------------------
        linesCount = len(self.line_traces)
        linesHit = 0

        if linesCount > 0:
            for ltrace in self.line_traces:
                if ltrace.execution_count > 0:
                    linesHit = linesHit + 1
                if ltrace.check_sum is not None:
                    fileHandle.write("DA:%d,%d,%d\n" % (ltrace.line_number, ltrace.execution_count, ltrace.check_sum))
                else:
                    fileHandle.write("DA:%d,%d\n" % (ltrace.line_number, ltrace.execution_count))

            fileHandle.write("LF:%d\n" % linesCount)
            fileHandle.write("LH:%d\n" % linesHit)

        # --------------------------------------------------
        # Write out the end of record marker
        # --------------------------------------------------
        fileHandle.write("end_of_record\n")

        return

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
    def __init__(self, record):
        self.ident = record.ident
        self.lineno_checksum = record.lineno_checksum
        self.cfg_checksum = record.cfg_checksum

        self.name = record.name
        self.filename = record.filename

        self.start_line = record.start_line

        self.blocks = None
        self.block_count = 0
        self.execution_count = 0

        self.has_catch = False

        self.solved = False

    def Print(self):
        print("===================================================================")
        print("GCovGraphFunction:")
        print("    Ident=%d" % self.indent)
        print("    LineNoCheckSum=0x%x" % self.lineno_checksum)
        print("    CfgCheckSum=0x%x" % self.cfg_checksum)
        print("    Name=%s" % self.name)
        print("    Source=%s" % self.source)
        print("    LineNo=%d" % self.line_no)
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

    def apply_counters(self, counters):
        counterIndex = 0
        for block in self.blocks:
            for arc in block.arcs_successors:
                if (arc.flags & GCovIOConst.GCOV_FLAG_ARC_FAKE) == 0 and \
                  (arc.flags & GCovIOConst.GCOV_FLAG_ARC_ON_TREE) == 0:
                    if arc.counter is None:
                        arc.counter = 0
                    counterVal = counters[counterIndex]
                    arc.counter += counterVal
                    counterIndex += 1

        if counterIndex != len(counters):
            print("WARNING: Not all of the counters were used.")

        return



    def reset_counters(self):
        counterIndex = 0
        for block in self.blocks:
            for arc in block.arcs_successors:
                arc.counter = None
        return

    def reset_unknowns(self):
        counterIndex = 0
        for block in self.blocks:
            for arc in block.arcs_successors:
                if (arc.flags & GCovIOConst.GCOV_FLAG_ARC_FAKE) != 0 or \
                  (arc.flags & GCovIOConst.GCOV_FLAG_ARC_ON_TREE) != 0:
                    arc.counter = None
        return

    WALKSTATE_NONE = 0
    WALKSTATE_BRANCHCHAIN = 1

    WALKSTATE_STRINGS = ["WALKSTATE_NONE", "WALKSTATE_BRANCHCHAIN"]

    def set_post_processed_fields(self):

        vprint ("Post Processing Graph for '%s'" % self.name)

        if (self.name == "_sputc"):
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
        walkState = GCovGraphFunction.WALKSTATE_NONE

        nxtBlock = None
        prevBlock = None

        while(blockIndex < blocksLen):
            prevBlock = nxtBlock
            nxtBlock = blocksList[blockIndex]

            vprint ("Block(%d) - %s" % (blockIndex, GCovGraphFunction.WALKSTATE_STRINGS[walkState]))

            hasLines = False
            if (nxtBlock.lines is not None) and (len(nxtBlock.lines) > 0):
                hasLines = True

            successorCount = len(nxtBlock.arcs_successors)

            #-------------------------------------------------------------------------------
            # This path is for processing sequences that began with a call-site
            #-------------------------------------------------------------------------------
            if walkState == GCovGraphFunction.WALKSTATE_BRANCHCHAIN:
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
                    walkState = GCovGraphFunction.WALKSTATE_NONE
                else:
                    walkStack.append((blockIndex, nxtBlock))

            #-------------------------------------------------------------------------------
            # This path initiates the start of the processing of a decision chain
            #-------------------------------------------------------------------------------
            if walkState == GCovGraphFunction.WALKSTATE_NONE:
                #---------------------------------------------------------------------------------
                # If a block has lines and has more than one successor arc, it must be the start
                # of a relevant branch chain.
                #---------------------------------------------------------------------------------
                if hasLines:
                    if (successorCount > 1):
                            walkState = GCovGraphFunction.WALKSTATE_BRANCHCHAIN
                            walkStack.append((blockIndex, nxtBlock))
                    else:
                        walkState = GCovGraphFunction.WALKSTATE_NONE
                else:
                    if (successorCount > 1):
                        isloop = False
                        for arc in nxtBlock.arcs_successors:
                            if arc.dest_block < blockIndex:
                                isloop = True

                        if isloop:
                            nxtBlock.is_loop = True
                    else:
                        walkState = GCovGraphFunction.WALKSTATE_NONE

            blockIndex += 1

        vprint ("")
        vprint ("")

        return

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

class ArcsetRecord():
    def __init__(self, block_no, arcs):
        self.block_number = block_no
        self.arcs = arcs

class LinesetRecord():
    def __init__(self, block_no, lines):
        self.block_number = block_no
        self.lines = lines

class BBRecord():
    def __init__(self, block_count):
        self.block_count = block_count

class FuncRecord():
    def __init__(self, ident, lineno_checksum, cfg_checksum, name, filename, start_line):
        self.ident = ident
        self.lineno_checksum = lineno_checksum
        self.cfg_checksum = cfg_checksum
        self.name = name
        self.filename = filename
        self.start_line = start_line

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
                record = self.read_func_record(record_buf)

                next_arc_id = 0

                current_graph = GraphFunction(record)
                self.graphs.append(current_graph)

                print("Processing function '%s' ident '%d'" % (current_graph.name, current_graph.ident))
            elif tag == Tag.BLOCKS:
                bb_count = self.read_bb_record(record_buf)
                current_graph.set_basic_blocks(bb_count)

                print(bb_count, "blocks for function")
            elif tag == Tag.ARCS:
                print("Read arc")
                block_no, arcs = self.read_arcs_record(record_buf, word_len)

                print(block_no, arcs[0].dest_block)
                next_arc_id = current_graph.set_arcs_for_bb(next_arc_id, block_no, arcs)
            elif tag == Tag.LINES:
                print("Read lines")
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
        ident, lineno_checksum, cfg_checksum = self.read_uint32(f), self.read_uint32(f), self.read_uint32(f)
        name, filename = self.read_string(f), self.read_string(f)
        start_line = self.read_uint32(f)

        return FuncRecord(ident, lineno_checksum, cfg_checksum, name, filename, start_line)

    def read_bb_record(self, f):
        return self.read_uint32(f)

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

        for _ in range(int(record_len / 2)):
            dest_block, flags = self.read_uint32(f), self.read_uint32(f)
            arc_set.append(GraphArc(dest_block, flags))

        return block_no, arc_set

class GCovScanContext:
    def __init__(self, includeFilters, excludeFilters):
        self.include_filters = includeFilters
        self.exclude_filters = excludeFilters
        self.files_found = []

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

    def reset(self):

        for key in self.coverage_map.keys():
            item = self.coverage_map.pop(key)
            del(item)

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

    def _locate_graph_file(self, df_fullbase, df_leafbase):
        foundfile = None

        if self.base_path_graph_list is not None:
            for basePath in self.base_path_graph_list:
                absBasePath = os.path.abspath(basePath)
                nxt_filename = os.path.join(absBasePath, df_leafbase + ".gcno")

                if os.path.exists(nxt_filename) and os.path.isfile(nxt_filename):
                    foundfile = nxt_filename
                    break
        else:
            nxt_filename = df_fullbase + ".gcno"

            if os.path.exists(nxt_filename) and os.path.isfile(nxt_filename):
                foundfile = nxt_filename

        return foundfile

    def _locate_source_file(self, df_fullbase, df_leafbase, c_file):
        foundfile = None

        if os.path.isabs(c_file) and os.path.exists(c_file) and os.path.isfile(c_file):
            foundfile = c_file
        else:
            c_dir = os.path.dirname(c_file)

            if c_dir != "":
                # If c_file has a relative path then do a search as follows:
                #    1. Look in the code path list by appending c_file to each path
                #    2. Look in the data path list by appending c_file to each path 
                if self.base_path_code_list is not None:
                    for basePath in self.base_path_code_list:
                        nxt_filename = os.path.join(basePath, c_file)
                        nxt_filename = os.path.abspath(nxt_filename)

                        if os.path.exists(nxt_filename) and os.path.isfile(nxt_filename):
                            foundfile = nxt_filename
                            break

                if foundfile is None:
                    for basePath in self.base_path_data_list:
                        nxt_filename = os.path.join(basePath, c_file)
                        nxt_filename = os.path.abspath(nxt_filename)

                        if os.path.exists(nxt_filename) and os.path.isfile(nxt_filename):
                            foundfile = nxt_filename
                            break
            else:
                # If c_file is just a filename with no directory do a search as follows:
                #    1. Look in the code path list in the same leaf directory as the data file
                #    2. Look in the data path list in the same leaf directory as the data file
                #    3. Look in the include path list without appending the leaf of the data file
                df_dir = os.path.dirname(df_leafbase)

                chk_file = os.path.join(df_dir, c_file)

                if self.base_path_code_list is not None:
                    for basePath in self.base_path_code_list:
                        absBasePath = os.path.abspath(basePath)
                        nxt_filename = os.path.join(absBasePath, chk_file)
                        nxt_filename = os.path.abspath(nxt_filename)

                        if os.path.exists(nxt_filename) and os.path.isfile(nxt_filename):
                            foundfile = nxt_filename
                            break

                if foundfile is None:
                    for basePath in self.base_path_data_list:
                        absBasePath = os.path.abspath(basePath)
                        nxt_filename = os.path.join(absBasePath, chk_file)
                        nxt_filename = os.path.abspath(nxt_filename)

                        if os.path.exists(nxt_filename) and os.path.isfile(nxt_filename):
                            foundfile = nxt_filename
                            break

                if (foundfile is None) and (self.base_path_include_list is not None):
                    for basePath in self.base_path_include_list:
                        absBasePath = os.path.abspath(basePath)
                        nxt_filename = os.path.join(absBasePath, c_file)
                        nxt_filename = os.path.abspath(nxt_filename)

                        if os.path.exists(nxt_filename) and os.path.isfile(nxt_filename):
                            foundfile = nxt_filename
                            break

        # Remove any base relative '/base/../../file' references from the path. 'os.path.isabs' still considers these absolute.
        if foundfile is not None:
            foundfile = os.path.abspath(foundfile)

        return foundfile

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

    def _scan_for_data_files(self, scanContext, currentDir, fileList):

        filesFound = scanContext.files_found

        for filename in fileList:
            f_base, f_ext = os.path.splitext(filename)
            if f_ext == ".gcda":
                filesFound.append((currentDir, filename))

        return True

    def _scan_for_graph_files(self, scanContext, currentDir, fileList):

        filesFound = scanContext.files_found

        for filename in fileList:
            f_base, f_ext = os.path.splitext(filename)
            if f_ext == ".gcno":
                filesFound.append((currentDir, filename))

        return


def parse_file(parseFile):
    fileRoot, fileExt = os.path.splitext(parseFile)
    filePath = os.path.abspath(parseFile)

    print("Processing .gcno file...")

    gcno = GCovNotesFile(filePath)

if __name__ == "__main__":
    parse_file(gcno_dir + '/' + file_name)
