if (APPLE OR SANITIZE STREQUAL "undefined" OR SANITIZE STREQUAL "memory")
    # llvm-tblgen, that is used during LLVM build, doesn't work with UBSan.
    set (ENABLE_EMBEDDED_COMPILER_DEFAULT OFF)
    set (ENABLE_DWARF_PARSER_DEFAULT OFF)
else()
    set (ENABLE_EMBEDDED_COMPILER_DEFAULT ${ENABLE_LIBRARIES})
    set (ENABLE_DWARF_PARSER_DEFAULT ${ENABLE_LIBRARIES})
endif()

option (ENABLE_EMBEDDED_COMPILER "Enable support for JIT compilation during query execution (uses LLVM library)" ${ENABLE_EMBEDDED_COMPILER_DEFAULT})

option (ENABLE_DWARF_PARSER "Enable support for DWARF input format (uses LLVM library)" ${ENABLE_DWARF_PARSER_DEFAULT})

if (NOT ENABLE_EMBEDDED_COMPILER AND NOT ENABLE_DWARF_PARSER)
    message(STATUS "Not using LLVM")
    return()
endif()


set(LLVM_PROJECT_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project")

set (CMAKE_CXX_STANDARD 14)

# LLVM_TARGET_DEFINITIONS must contain the name of the .td file to process,
# while LLVM_TARGET_DEPENDS may contain additional file dependencies.
# Extra parameters for `tblgen' may come after `ofn' parameter.
# Adds the name of the generated file to TABLEGEN_OUTPUT.

function(tablegen ofn)
  cmake_parse_arguments(ARG "" "" "DEPENDS;EXTRA_INCLUDES" ${ARGN})

  # Use depfile instead of globbing arbitrary *.td(s) for Ninja.
  if(CMAKE_GENERATOR MATCHES "Ninja")
    # Make output path relative to build.ninja, assuming located on
    # ${CMAKE_BINARY_DIR}.
    # CMake emits build targets as relative paths but Ninja doesn't identify
    # absolute path (in *.d) as relative path (in build.ninja)
    # Note that tblgen is executed on ${CMAKE_BINARY_DIR} as working directory.
    file(RELATIVE_PATH ofn_rel
      ${CMAKE_BINARY_DIR} ${CMAKE_CURRENT_BINARY_DIR}/${ofn})
    set(additional_cmdline
      -o ${ofn_rel}
      -d ${ofn_rel}.d
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
      DEPFILE ${CMAKE_CURRENT_BINARY_DIR}/${ofn}.d
      )
    set(local_tds)
    set(global_tds)
  else()
    file(GLOB local_tds "*.td")
    file(GLOB_RECURSE global_tds "${LLVM_MAIN_INCLUDE_DIR}/llvm/*.td")
    set(additional_cmdline
      -o ${CMAKE_CURRENT_BINARY_DIR}/${ofn}
      )
  endif()

  if (IS_ABSOLUTE ${LLVM_TARGET_DEFINITIONS})
    set(LLVM_TARGET_DEFINITIONS_ABSOLUTE ${LLVM_TARGET_DEFINITIONS})
  else()
    set(LLVM_TARGET_DEFINITIONS_ABSOLUTE
      ${CMAKE_CURRENT_SOURCE_DIR}/${LLVM_TARGET_DEFINITIONS})
  endif()

  set(tblgen_change_flag "--write-if-changed")

  # We need both _TABLEGEN_TARGET and _TABLEGEN_PATH in the DEPENDS list
  # (both the target and the file) to have .inc files rebuilt on
  # a tablegen change, as cmake does not propagate file-level dependencies
  # of custom targets. See the following ticket for more information:
  # https://cmake.org/Bug/view.php?id=15858
  # The dependency on both, the target and the file, produces the same
  # dependency twice in the result file when
  # ("${LLVM_TABLEGEN_TARGET}" STREQUAL "${LLVM_TABLEGEN_PATH}")
  # but lets us having smaller and cleaner code here.
  get_directory_property(tblgen_includes INCLUDE_DIRECTORIES)
  list(APPEND tblgen_includes ${ARG_EXTRA_INCLUDES})
  # Filter out empty items before prepending each entry with -I
  list(REMOVE_ITEM tblgen_includes "")
  list(TRANSFORM tblgen_includes PREPEND -I)

  set(TABLEGEN_PATH ${LLVM_TABLEGEN_PATH})
  set(tablegen_depends ${LLVM_TABLEGEN_TARGET} ${TABLEGEN_PATH})

  add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${ofn}
    COMMAND ${TABLEGEN_PATH} ${ARG_UNPARSED_ARGUMENTS} -I ${CMAKE_CURRENT_SOURCE_DIR}
    ${tblgen_includes}
    ${LLVM_TABLEGEN_FLAGS}
    ${LLVM_TARGET_DEFINITIONS_ABSOLUTE}
    ${tblgen_change_flag}
    ${additional_cmdline}
    # The file in LLVM_TARGET_DEFINITIONS may be not in the current
    # directory and local_tds may not contain it, so we must
    # explicitly list it here:
    DEPENDS ${ARG_DEPENDS} ${tablegen_depends}
      ${local_tds} ${global_tds}
    ${LLVM_TARGET_DEFINITIONS_ABSOLUTE}
    ${LLVM_TARGET_DEPENDS}
    COMMENT "Building ${ofn}..."
    )

  # `make clean' must remove all those generated files:
  set_property(DIRECTORY APPEND PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${ofn})

  set(TABLEGEN_OUTPUT ${TABLEGEN_OUTPUT} ${CMAKE_CURRENT_BINARY_DIR}/${ofn} PARENT_SCOPE)
  set_source_files_properties(${CMAKE_CURRENT_BINARY_DIR}/${ofn} PROPERTIES GENERATED 1)
endfunction()


add_library(LLVMAggressiveInstCombine
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/AggressiveInstCombine/AggressiveInstCombine.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/AggressiveInstCombine/TruncInstCombine.cpp
)


add_library(LLVMAnalysis
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/AliasAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/AliasAnalysisEvaluator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/AliasAnalysisSummary.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/AliasSetTracker.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/Analysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/AssumeBundleQueries.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/AssumptionCache.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/BasicAliasAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/BlockFrequencyInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/BlockFrequencyInfoImpl.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/BranchProbabilityInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CFG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CFGPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CFLAndersAliasAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CFLSteensAliasAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CGSCCPassManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CallGraph.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CallGraphSCCPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CallPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CaptureTracking.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CmpInstAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CostModel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CodeMetrics.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ConstantFolding.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/CycleAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/DDG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/DDGPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ConstraintSystem.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/Delinearization.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/DemandedBits.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/DependenceAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/DependenceGraphBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/DevelopmentModeInlineAdvisor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/DivergenceAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/DomPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/DomTreeUpdater.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/DominanceFrontier.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/EHPersonalities.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/FunctionPropertiesAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/GlobalsModRef.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/GuardUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/HeatUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/IRSimilarityIdentifier.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/IVDescriptors.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/IVUsers.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ImportedFunctionsInliningStatistics.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/IndirectCallPromotionAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/InlineCost.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/InlineAdvisor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/InlineSizeEstimatorAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/InstCount.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/InstructionPrecedenceTracking.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/InstructionSimplify.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/Interval.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/IntervalPartition.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LazyBranchProbabilityInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LazyBlockFrequencyInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LazyCallGraph.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LazyValueInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LegacyDivergenceAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/Lint.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/Loads.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LoopAccessAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LoopAnalysisManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LoopCacheAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LoopNestAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LoopUnrollAnalyzer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LoopInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/LoopPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/MLInlineAdvisor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/MemDepPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/MemDerefPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/MemoryBuiltins.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/MemoryDependenceAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/MemoryLocation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/MemoryProfileInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/MemorySSA.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/MemorySSAUpdater.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ModelUnderTrainingRunner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ModuleDebugInfoPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ModuleSummaryAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/MustExecute.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/NoInferenceModelRunner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ObjCARCAliasAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ObjCARCAnalysisUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ObjCARCInstKind.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/OptimizationRemarkEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/OverflowInstAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/PHITransAddr.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/PhiValues.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/PostDominators.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ProfileSummaryInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/PtrUseVisitor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/RegionInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/RegionPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/RegionPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ReplayInlineAdvisor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ScalarEvolution.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ScalarEvolutionAliasAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ScalarEvolutionDivision.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ScalarEvolutionNormalization.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/StackLifetime.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/StackSafetyAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/SyncDependenceAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/SyntheticCountsUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/TFUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/TensorSpec.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/TargetLibraryInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/TargetTransformInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/Trace.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/TypeBasedAliasAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/TypeMetadataUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ScopedNoAliasAA.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ValueLattice.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ValueLatticeUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/ValueTracking.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/VectorUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Analysis/VFABIDemangling.cpp
)


add_library(LLVMAsmParser
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/AsmParser/LLLexer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/AsmParser/LLParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/AsmParser/Parser.cpp
)


add_library(LLVMAsmPrinter
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/AccelTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/AddressPool.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/AIXException.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/ARMException.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/AsmPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/AsmPrinterDwarf.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/AsmPrinterInlineAsm.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DbgEntityHistoryCalculator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DebugHandlerBase.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DebugLocStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DIE.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DIEHash.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DwarfCFIException.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DwarfCompileUnit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DwarfDebug.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DwarfExpression.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DwarfFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DwarfStringPool.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/DwarfUnit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/EHStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/ErlangGCPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/OcamlGCPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/PseudoProbePrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/WinCFGuard.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/WinException.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/CodeViewDebug.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AsmPrinter/WasmException.cpp
)


add_library(LLVMBinaryFormat
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/AMDGPUMetadataVerifier.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/COFF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/Dwarf.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/ELF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/MachO.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/Magic.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/Minidump.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/MsgPackDocument.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/MsgPackDocumentYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/MsgPackReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/MsgPackWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/Wasm.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/BinaryFormat/XCOFF.cpp
)


add_library(LLVMBitReader
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Bitcode/Reader/BitcodeAnalyzer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Bitcode/Reader/BitReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Bitcode/Reader/BitcodeReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Bitcode/Reader/MetadataLoader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Bitcode/Reader/ValueList.cpp
)


add_library(LLVMBitWriter
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Bitcode/Writer/BitWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Bitcode/Writer/BitcodeWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Bitcode/Writer/BitcodeWriterPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Bitcode/Writer/ValueEnumerator.cpp
)


add_library(LLVMBitstreamReader
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Bitstream/Reader/BitstreamReader.cpp
)


add_library(LLVMCFGuard
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/CFGuard/CFGuard.cpp
)


add_library(LLVMCodeGen
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AggressiveAntiDepBreaker.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AllocationOrder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/Analysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/AtomicExpandPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/BasicTargetTransformInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/BranchFolding.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/BranchRelaxation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/BreakFalseDeps.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/BasicBlockSections.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/BasicBlockSectionsProfileReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CalcSpillWeights.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CallingConvLower.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CFGuardLongjmp.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CFIFixup.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CFIInstrInserter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CodeGen.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CodeGenCommonISel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CodeGenPassBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CodeGenPrepare.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CommandFlags.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/CriticalAntiDepBreaker.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/DeadMachineInstructionElim.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/DetectDeadLanes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/DFAPacketizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/DwarfEHPrepare.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/EarlyIfConversion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/EdgeBundles.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/EHContGuardCatchret.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ExecutionDomainFix.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ExpandMemCmp.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ExpandPostRAPseudos.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ExpandReductions.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ExpandVectorPredication.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/FaultMaps.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/FEntryInserter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/FinalizeISel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/FixupStatepointCallerSaved.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/FuncletLayout.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GCMetadata.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GCMetadataPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GCRootLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalMerge.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/HardwareLoops.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/IfConversion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ImplicitNullChecks.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/IndirectBrExpandPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/InlineSpiller.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/InterferenceCache.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/InterleavedAccessPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/InterleavedLoadCombinePass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/IntrinsicLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/JMCInstrumenter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LatencyPriorityQueue.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LazyMachineBlockFrequencyInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LexicalScopes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveDebugVariables.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveIntervals.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveInterval.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveIntervalUnion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LivePhysRegs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveRangeCalc.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveIntervalCalc.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveRangeEdit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveRangeShrink.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveRegMatrix.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveRegUnits.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveStacks.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveVariables.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LLVMTargetMachine.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LocalStackSlotAllocation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LoopTraversal.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LowLevelType.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LowerEmuTLS.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineBasicBlock.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineBlockFrequencyInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineBlockPlacement.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineBranchProbabilityInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineCombiner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineCopyPropagation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineCSE.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineCheckDebugify.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineCycleAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineDebugify.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineDominanceFrontier.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineDominators.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineFrameInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineFunction.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineFunctionPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineFunctionPrinterPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineFunctionSplitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineInstrBundle.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineInstr.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineLICM.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineLoopInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineLoopUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineModuleInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineModuleInfoImpls.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineModuleSlotTracker.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineOperand.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineOptimizationRemarkEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineOutliner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachinePassManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachinePipeliner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachinePostDominators.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineRegionInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineRegisterInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineScheduler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineSink.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineSizeOpts.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineSSAContext.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineSSAUpdater.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineStripDebug.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineTraceMetrics.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineVerifier.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRFSDiscriminator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRSampleProfile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRYamlMapping.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MLRegallocEvictAdvisor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ModuloSchedule.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MultiHazardRecognizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/PatchableFunction.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MBFIWrapper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRPrintingPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MacroFusion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/NonRelocatableStringpool.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/OptimizePHIs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ParallelCG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/PeepholeOptimizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/PHIElimination.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/PHIEliminationUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/PostRAHazardRecognizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/PostRASchedulerList.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/PreISelIntrinsicLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ProcessImplicitDefs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/PrologEpilogInserter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/PseudoProbeInserter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/PseudoSourceValue.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RDFGraph.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RDFLiveness.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RDFRegisters.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ReachingDefAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegAllocBase.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegAllocBasic.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegAllocEvictionAdvisor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegAllocFast.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegAllocGreedy.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegAllocPBQP.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegAllocScore.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegisterClassInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegisterCoalescer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegisterPressure.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegisterScavenging.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RemoveRedundantDebugValues.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RenameIndependentSubregs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MachineStableHash.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRVRegNamerUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRNamerPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRCanonicalizerPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegisterUsageInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegUsageInfoCollector.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegUsageInfoPropagate.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ReplaceWithVeclib.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ResetMachineFunctionPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegisterBank.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/RegisterBankInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SafeStack.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SafeStackLayout.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ScheduleDAG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ScheduleDAGInstrs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ScheduleDAGPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ScoreboardHazardRecognizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectOptimize.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ShadowStackGCLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ShrinkWrap.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SjLjEHPrepare.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SlotIndexes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SpillPlacement.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SplitKit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/StackColoring.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/StackMapLivenessAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/StackMaps.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/StackProtector.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/StackSlotColoring.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SwiftErrorValueTracking.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SwitchLoweringUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TailDuplication.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TailDuplicator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TargetFrameLoweringImpl.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TargetInstrInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TargetLoweringBase.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TargetLoweringObjectFileImpl.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TargetOptionsImpl.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TargetPassConfig.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TargetRegisterInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TargetSchedule.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TargetSubtargetInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TypePromotion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/TwoAddressInstructionPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/UnreachableBlockElim.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/ValueTypes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/VLIWMachineScheduler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/VirtRegMap.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/WasmEHPrepare.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/WinEHPrepare.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/XRayInstrumentation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveDebugValues/LiveDebugValues.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveDebugValues/VarLocBasedImpl.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/LiveDebugValues/InstrRefBasedImpl.cpp
)


add_library(LLVMCore
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/AbstractCallSite.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/AsmWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Assumptions.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Attributes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/AutoUpgrade.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/BasicBlock.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/BuiltinGCs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Comdat.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/ConstantFold.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/ConstantRange.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Constants.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Core.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/DIBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/DataLayout.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/DebugInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/DebugInfoMetadata.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/DebugLoc.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/DiagnosticHandler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/DiagnosticInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/DiagnosticPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Dominators.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/FPEnv.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Function.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/GCStrategy.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/GVMaterializer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Globals.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/IRBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/IRPrintingPasses.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/SSAContext.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/InlineAsm.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Instruction.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Instructions.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/IntrinsicInst.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/LLVMContext.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/LLVMContextImpl.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/LLVMRemarkStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/LegacyPassManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/MDBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Mangler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Metadata.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Module.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/ModuleSummaryIndex.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Operator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/OptBisect.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Pass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/PassInstrumentation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/PassManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/PassRegistry.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/PassTimingInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/PrintPasses.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/SafepointIRVerifier.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/ProfileSummary.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/PseudoProbe.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/ReplaceConstant.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Statepoint.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/StructuralHash.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Type.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/TypeFinder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Use.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/User.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Value.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/ValueSymbolTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/VectorBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IR/Verifier.cpp
)


add_library(LLVMCoroutines
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Coroutines/Coroutines.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Coroutines/CoroCleanup.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Coroutines/CoroConditionalWrapper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Coroutines/CoroEarly.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Coroutines/CoroElide.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Coroutines/CoroFrame.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Coroutines/CoroSplit.cpp
)


add_library(LLVMCoverage
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/Coverage/CoverageMapping.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/Coverage/CoverageMappingWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/Coverage/CoverageMappingReader.cpp
)


add_library(LLVMDWARFLinker
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DWARFLinker/DWARFLinkerCompileUnit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DWARFLinker/DWARFLinkerDeclContext.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DWARFLinker/DWARFLinker.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DWARFLinker/DWARFStreamer.cpp
)


add_library(LLVMDWP
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DWP/DWP.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DWP/DWPError.cpp
)


add_library(LLVMDebugInfoCodeView
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/AppendingTypeTableBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/CodeViewError.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/CodeViewRecordIO.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/ContinuationRecordBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/CVSymbolVisitor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/CVTypeVisitor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugChecksumsSubsection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugCrossExSubsection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugCrossImpSubsection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugFrameDataSubsection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugInlineeLinesSubsection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugLinesSubsection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugStringTableSubsection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugSubsection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugSubsectionRecord.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugSubsectionVisitor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugSymbolRVASubsection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/DebugSymbolsSubsection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/EnumTables.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/Formatters.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/GlobalTypeTableBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/LazyRandomTypeCollection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/Line.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/MergingTypeTableBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/RecordName.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/RecordSerialization.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/SimpleTypeSerializer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/StringsAndChecksums.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/SymbolDumper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/SymbolRecordHelpers.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/SymbolRecordMapping.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/SymbolSerializer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/TypeDumpVisitor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/TypeIndex.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/TypeIndexDiscovery.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/TypeHashing.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/TypeRecordHelpers.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/TypeRecordMapping.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/TypeStreamMerger.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/CodeView/TypeTableCollection.cpp
)


add_library(LLVMDebugInfoDWARF
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFAbbreviationDeclaration.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFAddressRange.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFAcceleratorTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFCompileUnit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFContext.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDataExtractor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugAbbrev.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugAddr.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugArangeSet.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugAranges.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugFrame.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFTypePrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugInfoEntry.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugLine.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugLoc.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugMacro.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugPubTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugRangeList.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDebugRnglists.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFDie.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFExpression.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFFormValue.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFGdbIndex.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFListTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFLocationExpression.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFTypeUnit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFUnitIndex.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFUnit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/DWARF/DWARFVerifier.cpp
)


add_library(LLVMDebugInfoGSYM
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/DwarfTransformer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/Header.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/FileWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/FunctionInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/GsymCreator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/GsymReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/InlineInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/LineTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/LookupResult.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/ObjectFileTransformer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/GSYM/ExtractRanges.cpp
)


add_library(LLVMDebugInfoMSF
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/MSF/MappedBlockStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/MSF/MSFBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/MSF/MSFCommon.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/MSF/MSFError.cpp
)


add_library(LLVMDebugInfoPDB
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/GenericError.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/IPDBSourceFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDB.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBContext.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBExtras.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBInterfaceAnchors.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolAnnotation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolBlock.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolCompiland.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolCompilandDetails.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolCompilandEnv.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolCustom.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolData.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolExe.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolFunc.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolFuncDebugEnd.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolFuncDebugStart.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolLabel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolPublicSymbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolThunk.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeArray.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeBaseClass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeBuiltin.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeCustom.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeDimension.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeEnum.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeFriend.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeFunctionArg.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeFunctionSig.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeManaged.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypePointer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeTypedef.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeUDT.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeVTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolTypeVTableShape.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolUnknown.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymbolUsingNamespace.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/PDBSymDumper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/UDTLayout.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/DbiModuleDescriptor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/DbiModuleDescriptorBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/DbiModuleList.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/DbiStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/DbiStreamBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/EnumTables.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/FormatUtil.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/GlobalsStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/Hash.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/HashTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/InfoStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/InfoStreamBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/InjectedSourceStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/InputFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/LinePrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/ModuleDebugStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeCompilandSymbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeEnumGlobals.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeEnumInjectedSources.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeEnumLineNumbers.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeEnumModules.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeEnumTypes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeEnumSymbols.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeExeSymbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeFunctionSymbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeInlineSiteSymbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeLineNumber.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativePublicSymbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeRawSymbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeSourceFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeSymbolEnumerator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeTypeArray.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeTypeBuiltin.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeTypeEnum.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeTypeFunctionSig.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeTypePointer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeTypeTypedef.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeTypeUDT.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeTypeVTShape.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NamedStreamMap.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/NativeSession.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/PDBFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/PDBFileBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/PDBStringTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/PDBStringTableBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/PublicsStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/GSIStreamBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/RawError.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/SymbolCache.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/SymbolStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/TpiHashing.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/TpiStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/PDB/Native/TpiStreamBuilder.cpp
)


add_library(LLVMDebuginfod
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Debuginfod/Debuginfod.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Debuginfod/DIFetcher.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Debuginfod/HTTPClient.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Debuginfod/HTTPServer.cpp
)


add_library(LLVMDemangle
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Demangle/Demangle.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Demangle/ItaniumDemangle.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Demangle/MicrosoftDemangle.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Demangle/MicrosoftDemangleNodes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Demangle/RustDemangle.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Demangle/DLangDemangle.cpp
)


add_library(LLVMDlltoolDriver
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ToolDrivers/llvm-dlltool/DlltoolDriver.cpp
)


add_library(LLVMExecutionEngine
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/ExecutionEngine.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/ExecutionEngineBindings.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/GDBRegistrationListener.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/SectionMemoryManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/TargetSelect.cpp
)


add_library(LLVMExtensions
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Extensions/Extensions.cpp
)


add_library(LLVMFileCheck
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/FileCheck/FileCheck.cpp
)


add_library(LLVMFrontendOpenACC
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Frontend/OpenACC/ACC.cpp
)


add_library(LLVMFrontendOpenMP
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Frontend/OpenMP/OMP.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Frontend/OpenMP/OMPContext.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Frontend/OpenMP/OMPIRBuilder.cpp
)


add_library(LLVMFuzzMutate
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/FuzzMutate/IRMutator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/FuzzMutate/OpDescriptor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/FuzzMutate/Operations.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/FuzzMutate/RandomIRBuilder.cpp
)


add_library(LLVMFuzzerCLI
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/FuzzMutate/FuzzerCLI.cpp
)


add_library(LLVMGlobalISel
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/CSEInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/GISelKnownBits.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/CSEMIRBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/CallLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/GlobalISel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/Combiner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/CombinerHelper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/GISelChangeObserver.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/IRTranslator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/InlineAsmLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/InstructionSelect.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/InstructionSelector.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/LegalityPredicates.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/LegalizeMutations.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/Legalizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/LegalizerHelper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/LegalizerInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/LegacyLegalizerInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/LoadStoreOpt.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/Localizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/LostDebugLocObserver.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/MachineIRBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/RegBankSelect.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/GlobalISel/Utils.cpp
)


add_library(LLVMIRReader
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/IRReader/IRReader.cpp
)


add_library(LLVMInstCombine
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstructionCombining.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineAddSub.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineAtomicRMW.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineAndOrXor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineCalls.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineCasts.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineCompares.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineLoadStoreAlloca.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineMulDivRem.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineNegator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombinePHI.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineSelect.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineShifts.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineSimplifyDemanded.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/InstCombine/InstCombineVectorOps.cpp
)


add_library(LLVMInstrumentation
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/AddressSanitizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/BoundsChecking.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/CGProfile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/ControlHeightReduction.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/DataFlowSanitizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/GCOVProfiling.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/MemProfiler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/MemorySanitizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/IndirectCallPromotion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/Instrumentation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/InstrOrderFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/InstrProfiling.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/PGOInstrumentation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/PGOMemOPSizeOpt.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/PoisonChecking.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/SanitizerCoverage.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/ValueProfileCollector.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/ThreadSanitizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Instrumentation/HWAddressSanitizer.cpp
)


add_library(LLVMInterfaceStub
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/InterfaceStub/ELFObjHandler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/InterfaceStub/IFSHandler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/InterfaceStub/IFSStub.cpp
)


add_library(LLVMInterpreter
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Interpreter/Execution.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Interpreter/ExternalFunctions.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Interpreter/Interpreter.cpp
)


add_library(LLVMJITLink
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/DWARFRecordSectionSplitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/EHFrameSupport.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/JITLink.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/JITLinkGeneric.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/JITLinkMemoryManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/MemoryFlags.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/MachO.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/MachO_arm64.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/MachO_x86_64.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/MachOLinkGraphBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/ELF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/ELFLinkGraphBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/ELF_aarch64.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/ELF_riscv.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/ELF_x86_64.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/COFF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/COFFLinkGraphBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/COFF_x86_64.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/aarch64.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/riscv.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/JITLink/x86_64.cpp
)


add_library(LLVMLTO
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/LTO/LTO.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/LTO/LTOBackend.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/LTO/LTOModule.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/LTO/LTOCodeGenerator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/LTO/SummaryBasedOptimizations.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/LTO/UpdateCompilerUsed.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/LTO/ThinLTOCodeGenerator.cpp
)


add_library(LLVMLibDriver
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ToolDrivers/llvm-lib/LibDriver.cpp
)


add_library(LLVMLineEditor
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/LineEditor/LineEditor.cpp
)


add_library(LLVMLinker
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Linker/IRMover.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Linker/LinkModules.cpp
)


add_library(LLVMMC
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/ConstantPools.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/ELFObjectWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAsmBackend.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAsmInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAsmInfoCOFF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAsmInfoDarwin.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAsmInfoELF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAsmInfoGOFF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAsmInfoWasm.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAsmInfoXCOFF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAsmMacro.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAsmStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCAssembler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCCodeEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCCodeView.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCContext.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCDwarf.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCDXContainerStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCDXContainerWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCELFObjectTargetWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCELFStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCExpr.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCFragment.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCInst.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCInstPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCInstrAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCInstrDesc.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCInstrInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCLabel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCLinkerOptimizationHint.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCMachOStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCMachObjectTargetWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCNullStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCObjectFileInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCObjectStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCObjectWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCPseudoProbe.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCRegisterInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSchedule.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSection.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSectionCOFF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSectionDXContainer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSectionELF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSectionMachO.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSectionWasm.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSectionXCOFF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSPIRVStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSubtargetInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSymbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSymbolELF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCSymbolXCOFF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCTargetOptions.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCTargetOptionsCommandFlags.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCValue.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCWasmObjectTargetWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCWasmStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCWin64EH.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCWinCOFFStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCWinEH.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCXCOFFObjectTargetWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCXCOFFStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MachObjectWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/SPIRVObjectWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/StringTableBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/SubtargetFeature.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/TargetRegistry.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/WasmObjectWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/WinCOFFObjectWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/XCOFFObjectWriter.cpp
)


add_library(LLVMMCA
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/CodeEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Context.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/CustomBehaviour.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/HWEventListener.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/HardwareUnits/HardwareUnit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/HardwareUnits/LSUnit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/HardwareUnits/RegisterFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/HardwareUnits/ResourceManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/HardwareUnits/RetireControlUnit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/HardwareUnits/Scheduler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/IncrementalSourceMgr.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/InstrBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Instruction.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Pipeline.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Stages/DispatchStage.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Stages/EntryStage.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Stages/ExecuteStage.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Stages/InOrderIssueStage.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Stages/InstructionTables.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Stages/MicroOpQueueStage.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Stages/RetireStage.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Stages/Stage.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/Support.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MCA/View.cpp
)


add_library(LLVMMCDisassembler
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCDisassembler/Disassembler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCDisassembler/MCDisassembler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCDisassembler/MCExternalSymbolizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCDisassembler/MCRelocationInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCDisassembler/MCSymbolizer.cpp
)


add_library(LLVMMCJIT
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/MCJIT/MCJIT.cpp
)


add_library(LLVMMCParser
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/AsmLexer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/AsmParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/COFFAsmParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/COFFMasmParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/GOFFAsmParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/DarwinAsmParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/ELFAsmParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/MCAsmLexer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/MCAsmParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/MCAsmParserExtension.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/MCTargetAsmParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/MasmParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/WasmAsmParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/MC/MCParser/XCOFFAsmParser.cpp
)


add_library(LLVMMIRParser
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRParser/MILexer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRParser/MIParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/MIRParser/MIRParser.cpp
)


add_library(LLVMObjCARCOpts
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/ObjCARC/ObjCARC.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/ObjCARC/ObjCARCOpts.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/ObjCARC/ObjCARCExpand.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/ObjCARC/ObjCARCAPElim.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/ObjCARC/ObjCARCContract.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/ObjCARC/DependencyAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/ObjCARC/ProvenanceAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/ObjCARC/ProvenanceAnalysisEvaluator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/ObjCARC/PtrState.cpp
)


add_library(LLVMObjCopy
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/Archive.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/CommonConfig.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/ObjCopy.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/ConfigManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/COFF/COFFObjcopy.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/COFF/COFFObject.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/COFF/COFFReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/COFF/COFFWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/ELF/ELFObjcopy.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/ELF/ELFObject.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/MachO/MachOObjcopy.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/MachO/MachOReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/MachO/MachOWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/MachO/MachOLayoutBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/MachO/MachOObject.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/wasm/WasmObject.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/wasm/WasmReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/wasm/WasmWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/wasm/WasmObjcopy.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/XCOFF/XCOFFObjcopy.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/XCOFF/XCOFFReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjCopy/XCOFF/XCOFFWriter.cpp
)


add_library(LLVMObject
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/Archive.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/ArchiveWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/Binary.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/COFFImportFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/COFFModuleDefinition.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/COFFObjectFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/Decompressor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/DXContainer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/ELF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/ELFObjectFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/Error.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/FaultMapParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/IRObjectFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/IRSymtab.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/MachOObjectFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/MachOUniversal.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/Minidump.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/ModuleSymbolTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/Object.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/ObjectFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/OffloadBinary.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/RecordStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/RelocationResolver.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/SymbolicFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/SymbolSize.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/TapiFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/TapiUniversal.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/MachOUniversalWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/WasmObjectFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/WindowsMachineFlag.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/WindowsResource.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Object/XCOFFObjectFile.cpp
)


add_library(LLVMObjectYAML
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/ArchiveEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/ArchiveYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/CodeViewYAMLDebugSections.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/CodeViewYAMLSymbols.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/CodeViewYAMLTypeHashing.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/CodeViewYAMLTypes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/COFFEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/COFFYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/DWARFEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/DWARFYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/DXContainerEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/DXContainerYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/ELFEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/ELFYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/MachOEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/MachOYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/ObjectYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/MinidumpEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/MinidumpYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/OffloadEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/OffloadYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/WasmEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/WasmYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/XCOFFEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/XCOFFYAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/YAML.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ObjectYAML/yaml2obj.cpp
)


add_library(LLVMOption
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Option/Arg.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Option/ArgList.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Option/Option.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Option/OptTable.cpp
)


add_library(LLVMOrcJIT
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/CompileOnDemandLayer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/CompileUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/Core.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/DebugObjectManagerPlugin.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/DebuggerSupportPlugin.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/DebugUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/EPCDynamicLibrarySearchGenerator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/EPCDebugObjectRegistrar.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/EPCEHFrameRegistrar.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/EPCGenericDylibManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/EPCGenericJITLinkMemoryManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/EPCGenericRTDyldMemoryManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/EPCIndirectionUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/ExecutionUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/ObjectFileInterface.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/IndirectionUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/IRCompileLayer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/IRTransformLayer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/JITTargetMachineBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/LazyReexports.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/Layer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/LookupAndRecordAddrs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/LLJIT.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/MachOPlatform.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/MapperJITLinkMemoryManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/MemoryMapper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/ELFNixPlatform.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/Mangling.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/ObjectLinkingLayer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/ObjectTransformLayer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/OrcABISupport.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/OrcV2CBindings.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/SimpleRemoteEPC.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/Speculation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/SpeculateAnalyses.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/ExecutorProcessControl.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/TaskDispatch.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/ThreadSafeModule.cpp
)


add_library(LLVMOrcShared
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/Shared/AllocationActions.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/Shared/OrcError.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/Shared/OrcRTBridge.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/Shared/SimpleRemoteEPCUtils.cpp
)


add_library(LLVMOrcTargetProcess
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/TargetProcess/ExecutorSharedMemoryMapperService.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/TargetProcess/JITLoaderGDB.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/TargetProcess/OrcRTBootstrap.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/TargetProcess/RegisterEHFrames.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/TargetProcess/SimpleExecutorDylibManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/TargetProcess/SimpleExecutorMemoryManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/TargetProcess/SimpleRemoteEPCServer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/Orc/TargetProcess/TargetExecutionUtils.cpp
)


add_library(LLVMPasses
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Passes/OptimizationLevel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Passes/PassBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Passes/PassBuilderBindings.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Passes/PassBuilderPipelines.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Passes/PassPlugin.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Passes/StandardInstrumentations.cpp
)


add_library(LLVMProfileData
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/GCOV.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/InstrProf.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/InstrProfCorrelator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/InstrProfReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/InstrProfWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/MemProf.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/ProfileSummaryBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/SampleProf.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/SampleProfReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/SampleProfWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ProfileData/RawMemProfReader.cpp
)


add_library(LLVMRemarks
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/BitstreamRemarkParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/BitstreamRemarkSerializer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/Remark.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/RemarkFormat.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/RemarkLinker.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/RemarkParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/RemarkSerializer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/RemarkStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/RemarkStringTable.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/YAMLRemarkParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Remarks/YAMLRemarkSerializer.cpp
)


add_library(LLVMRuntimeDyld
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/RuntimeDyld/JITSymbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/RuntimeDyld/RTDyldMemoryManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/RuntimeDyld/RuntimeDyld.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldChecker.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldCOFF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldELF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldMachO.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/ExecutionEngine/RuntimeDyld/Targets/RuntimeDyldELFMips.cpp
)


add_library(LLVMScalarOpts
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/ADCE.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/AlignmentFromAssumptions.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/AnnotationRemarks.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/BDCE.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/CallSiteSplitting.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/ConstantHoisting.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/ConstraintElimination.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/CorrelatedValuePropagation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/DCE.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/DeadStoreElimination.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/DFAJumpThreading.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/DivRemPairs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/EarlyCSE.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/FlattenCFGPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/Float2Int.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/GuardWidening.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/GVN.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/GVNHoist.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/GVNSink.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/IVUsersPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/InductiveRangeCheckElimination.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/IndVarSimplify.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/InferAddressSpaces.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/InstSimplifyPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/JumpThreading.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LICM.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopAccessAnalysisPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopBoundSplit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopSink.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopDeletion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopDataPrefetch.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopDistribute.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopFuse.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopIdiomRecognize.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopInstSimplify.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopInterchange.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopFlatten.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopLoadElimination.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopPassManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopPredication.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopRerollPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopRotation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopSimplifyCFG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopStrengthReduce.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopUnrollPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopUnrollAndJamPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LoopVersioningLICM.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LowerAtomicPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LowerConstantIntrinsics.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LowerExpectIntrinsic.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LowerGuardIntrinsic.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LowerMatrixIntrinsics.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/LowerWidenableCondition.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/MakeGuardsExplicit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/MemCpyOptimizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/MergeICmps.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/MergedLoadStoreMotion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/NaryReassociate.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/NewGVN.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/PartiallyInlineLibCalls.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/PlaceSafepoints.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/Reassociate.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/Reg2Mem.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/RewriteStatepointsForGC.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/SCCP.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/SROA.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/Scalar.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/Scalarizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/ScalarizeMaskedMemIntrin.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/SeparateConstOffsetFromGEP.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/SimpleLoopUnswitch.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/SimplifyCFGPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/Sink.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/SpeculativeExecution.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/StraightLineStrengthReduce.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/StructurizeCFG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/TailRecursionElimination.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/TLSVariableHoist.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Scalar/WarnMissedTransforms.cpp
)


add_library(LLVMSelectionDAG
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/DAGCombiner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/FastISel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/FunctionLoweringInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/InstrEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/LegalizeDAG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/LegalizeFloatTypes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/LegalizeIntegerTypes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/LegalizeTypes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/LegalizeTypesGeneric.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/LegalizeVectorOps.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/LegalizeVectorTypes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/ResourcePriorityQueue.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/ScheduleDAGFast.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/ScheduleDAGRRList.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/ScheduleDAGSDNodes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/ScheduleDAGVLIW.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/SelectionDAGBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/SelectionDAG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/SelectionDAGAddressAnalysis.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/SelectionDAGDumper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/SelectionDAGISel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/SelectionDAGPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/SelectionDAGTargetInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/StatepointLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/CodeGen/SelectionDAG/TargetLowering.cpp
)


add_library(LLVMSupport
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/regcomp.c
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/regerror.c
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/regexec.c
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/regfree.c
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/regstrlcpy.c
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/AddressRanges.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/AArch64TargetParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ABIBreak.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ARMTargetParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/AMDGPUMetadata.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/APFixedPoint.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/APFloat.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/APInt.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/APSInt.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ARMBuildAttrs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ARMAttributeParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ARMWinEH.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Allocator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/AutoConvert.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BinaryStreamError.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BinaryStreamReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BinaryStreamRef.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BinaryStreamWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BlockFrequency.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BranchProbability.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BuryPointer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/CachePruning.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Caching.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/circular_raw_ostream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Chrono.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/COM.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/CodeGenCoverage.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/CommandLine.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Compression.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/CRC.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ConvertUTF.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ConvertUTFWrapper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/CrashRecoveryContext.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/CSKYAttributes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/CSKYAttributeParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/CSKYTargetParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/DataExtractor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Debug.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/DebugCounter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/DeltaAlgorithm.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/DivisionByConstantInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/DAGDeltaAlgorithm.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/DJB.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ELFAttributeParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ELFAttributes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Error.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ErrorHandling.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ExtensibleRTTI.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/FileCollector.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/FileUtilities.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/FileOutputBuffer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/FoldingSet.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/FormattedStream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/FormatVariadic.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/GlobPattern.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/GraphWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Hashing.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/InitLLVM.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/InstructionCost.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/IntEqClasses.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/IntervalMap.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ItaniumManglingCanonicalizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/JSON.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/KnownBits.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/LEB128.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/LineIterator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Locale.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/LockFileManager.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/LowLevelType.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ManagedStatic.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/MathExtras.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/MemAlloc.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/MemoryBuffer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/MemoryBufferRef.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/MD5.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/MSP430Attributes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/MSP430AttributeParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/NativeFormatting.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/OptimizedStructLayout.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Optional.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Parallel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/PluginLoader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/PrettyStackTrace.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/RandomNumberGenerator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Regex.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/RISCVAttributes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/RISCVAttributeParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/RISCVISAInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ScaledNumber.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ScopedPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/SHA1.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/SHA256.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Signposts.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/SmallPtrSet.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/SmallVector.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/SourceMgr.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/SpecialCaseList.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Statistic.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/StringExtras.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/StringMap.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/StringSaver.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/StringRef.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/SuffixTree.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/SymbolRemappingReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/SystemUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/TarWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/TargetParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ThreadPool.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/TimeProfiler.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Timer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ToolOutputFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/TrigramIndex.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Triple.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Twine.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/TypeSize.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Unicode.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/UnicodeCaseFold.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/UnicodeNameToCodepoint.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/UnicodeNameToCodepointGenerated.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/VersionTuple.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/VirtualFileSystem.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/WithColor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/X86TargetParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/YAMLParser.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/YAMLTraits.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/raw_os_ostream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/raw_ostream.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/xxhash.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Z3Solver.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Atomic.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/DynamicLibrary.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Errno.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Host.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Memory.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Path.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Process.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Program.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/RWMutex.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Signals.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/ThreadLocal.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Threading.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Valgrind.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/Watchdog.cpp
)


add_library(LLVMSupportBlake3
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BLAKE3/blake3_sse2_x86-64_unix.S
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BLAKE3/blake3_sse41_x86-64_unix.S
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BLAKE3/blake3_avx2_x86-64_unix.S
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BLAKE3/blake3_avx512_x86-64_unix.S
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BLAKE3/blake3.c
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BLAKE3/blake3_dispatch.c
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BLAKE3/blake3_portable.c
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Support/BLAKE3/blake3_neon.c
)


add_library(LLVMSymbolize
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/Symbolize/DIFetcher.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/Symbolize/DIPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/Symbolize/Markup.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/Symbolize/MarkupFilter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/Symbolize/SymbolizableObjectFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/DebugInfo/Symbolize/Symbolize.cpp
)


add_library(LLVMTableGenGlobalISel
${LLVM_PROJECT_SOURCE_DIR}/llvm/utils/TableGen/GlobalISel/CodeExpander.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/utils/TableGen/GlobalISel/GIMatchDag.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/utils/TableGen/GlobalISel/GIMatchDagEdge.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/utils/TableGen/GlobalISel/GIMatchDagInstr.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/utils/TableGen/GlobalISel/GIMatchDagOperands.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/utils/TableGen/GlobalISel/GIMatchDagPredicate.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/utils/TableGen/GlobalISel/GIMatchDagPredicateDependencyEdge.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/utils/TableGen/GlobalISel/GIMatchTree.cpp
)


add_library(LLVMTarget
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/Target.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/TargetIntrinsicInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/TargetLoweringObjectFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/TargetMachine.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/TargetMachineC.cpp
)


add_library(LLVMTextAPI
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/TextAPI/Architecture.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/TextAPI/ArchitectureSet.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/TextAPI/InterfaceFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/TextAPI/PackedVersion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/TextAPI/Platform.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/TextAPI/Symbol.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/TextAPI/Target.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/TextAPI/TextStub.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/TextAPI/TextStubCommon.cpp
)


add_library(LLVMTransformUtils
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/AddDiscriminators.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/AMDGPUEmitPrintf.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/ASanStackFrameLayout.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/AssumeBundleBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/BasicBlockUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/BreakCriticalEdges.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/BuildLibCalls.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/BypassSlowDivision.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/CallPromotionUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/CallGraphUpdater.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/CanonicalizeAliases.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/CanonicalizeFreezeInLoops.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/CloneFunction.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/CloneModule.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/CodeExtractor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/CodeLayout.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/CodeMoverUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/CtorUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/Debugify.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/DemoteRegToStack.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/EntryExitInstrumenter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/EscapeEnumerator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/Evaluator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/FixIrreducible.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/FlattenCFG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/FunctionComparator.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/FunctionImportUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/GlobalStatus.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/GuardUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/HelloWorld.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/InlineFunction.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/InjectTLIMappings.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/InstructionNamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/IntegerDivision.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LCSSA.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LibCallsShrinkWrap.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/Local.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LoopPeel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LoopRotationUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LoopSimplify.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LoopUnroll.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LoopUnrollAndJam.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LoopUnrollRuntime.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LoopUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LoopVersioning.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LowerAtomic.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LowerGlobalDtors.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LowerInvoke.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LowerMemIntrinsics.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/LowerSwitch.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/MatrixUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/MemoryOpRemark.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/MemoryTaggingSupport.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/Mem2Reg.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/MetaRenamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/MisExpect.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/ModuleUtils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/NameAnonGlobals.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/PredicateInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/PromoteMemoryToRegister.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/RelLookupTableConverter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/ScalarEvolutionExpander.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SCCPSolver.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/StripGCRelocates.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SSAUpdater.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SSAUpdaterBulk.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SampleProfileInference.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SampleProfileLoaderBaseUtil.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SanitizerStats.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SimplifyCFG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SimplifyIndVar.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SimplifyLibCalls.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SizeOpts.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SplitModule.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/StripNonLineTableDebugInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/SymbolRewriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/UnifyFunctionExitNodes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/UnifyLoopExits.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/Utils.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/ValueMapper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Utils/VNCoercion.cpp
)


add_library(LLVMVectorize
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/LoadStoreVectorizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/LoopVectorizationLegality.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/LoopVectorize.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/SLPVectorizer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/Vectorize.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/VectorCombine.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/VPlan.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/VPlanHCFGBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/VPlanRecipes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/VPlanSLP.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/VPlanTransforms.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/Vectorize/VPlanVerifier.cpp
)


add_library(LLVMWindowsDriver
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/WindowsDriver/MSVCPaths.cpp
)


add_library(LLVMWindowsManifest
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/WindowsManifest/WindowsManifestMerger.cpp
)


add_library(LLVMX86AsmParser
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/AsmParser/X86AsmParser.cpp
)


add_library(LLVMX86CodeGen
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86AsmPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86AvoidTrailingCall.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86CallFrameOptimization.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86CallingConv.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86CallLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86CmovConversion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86DomainReassignment.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86DiscriminateMemOps.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86LowerTileCopy.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86LowerAMXType.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86PreAMXConfig.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86LowerAMXIntrinsics.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86TileConfig.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86FastPreTileConfig.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86FastTileConfig.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86PreTileConfig.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86ExpandPseudo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86FastISel.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86FixupBWInsts.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86FixupLEAs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86AvoidStoreForwardingBlocks.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86DynAllocaExpander.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86FixupSetCC.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86FlagsCopyLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86FloatingPoint.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86FrameLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86InstructionSelector.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86ISelDAGToDAG.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86ISelLowering.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86IndirectBranchTracking.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86IndirectThunks.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86InterleavedAccess.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86InsertPrefetch.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86InstCombineIntrinsic.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86InstrFMA3Info.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86InstrFoldTables.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86InstrInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86EvexToVex.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86LegalizerInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86LoadValueInjectionLoadHardening.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86LoadValueInjectionRetHardening.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86MCInstLower.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86MachineFunctionInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86MacroFusion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86OptimizeLEAs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86PadShortFunction.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86PartialReduction.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86RegisterBankInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86RegisterInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86ReturnThunks.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86SelectionDAGInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86ShuffleDecodeConstantPool.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86SpeculativeLoadHardening.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86SpeculativeExecutionSideEffectSuppression.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86Subtarget.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86TargetMachine.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86TargetObjectFile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86TargetTransformInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86VZeroUpper.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86WinEHState.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/X86InsertWait.cpp
)


add_library(LLVMX86Desc
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86ATTInstPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86IntelInstPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86InstComments.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86InstPrinterCommon.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86InstrRelaxTables.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86ShuffleDecode.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86AsmBackend.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86MCTargetDesc.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86MCAsmInfo.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86MCCodeEmitter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86MachObjectWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86MnemonicTables.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86ELFObjectWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86WinCOFFObjectWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86WinCOFFStreamer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCTargetDesc/X86WinCOFFTargetStreamer.cpp
)


tablegen(X86GenAsmMatcher.inc -gen-asm-matcher)
tablegen(X86GenAsmWriter.inc -gen-asm-writer)
tablegen(X86GenAsmWriter1.inc -gen-asm-writer -asmwriternum=1)
tablegen(X86GenCallingConv.inc -gen-callingconv)
tablegen(X86GenDAGISel.inc -gen-dag-isel)
tablegen(X86GenDisassemblerTables.inc -gen-disassembler)
tablegen(X86GenEVEX2VEXTables.inc -gen-x86-EVEX2VEX-tables)
tablegen(X86GenExegesis.inc -gen-exegesis)
tablegen(X86GenFastISel.inc -gen-fast-isel)
tablegen(X86GenGlobalISel.inc -gen-global-isel)
tablegen(X86GenInstrInfo.inc -gen-instr-info -instr-info-expand-mi-operand-info=0)
tablegen(X86GenMnemonicTables.inc -gen-x86-mnemonic-tables -asmwriternum=1)
tablegen(X86GenRegisterBank.inc -gen-register-bank)
tablegen(X86GenRegisterInfo.inc -gen-register-info)
tablegen(X86GenSubtargetInfo.inc -gen-subtarget)
tablegen(X86GenFoldTables.inc -gen-x86-fold-tables -asmwriternum=1)


add_library(LLVMX86Disassembler
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/Disassembler/X86Disassembler.cpp
)


add_library(LLVMX86Info
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/TargetInfo/X86TargetInfo.cpp
)


add_library(LLVMX86TargetMCA
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Target/X86/MCA/X86CustomBehaviour.cpp
)


add_library(LLVMXRay
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/BlockIndexer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/BlockPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/BlockVerifier.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/FDRRecordProducer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/FDRRecords.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/FDRTraceExpander.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/FDRTraceWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/FileHeaderReader.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/InstrumentationMap.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/LogBuilderConsumer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/Profile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/RecordInitializer.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/RecordPrinter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/XRay/Trace.cpp
)


add_library(LLVMipo
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/AlwaysInliner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/Annotation2Metadata.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/ArgumentPromotion.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/Attributor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/AttributorAttributes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/BarrierNoopPass.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/BlockExtractor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/CalledValuePropagation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/ConstantMerge.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/CrossDSOCFI.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/DeadArgumentElimination.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/ElimAvailExtern.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/ExtractGV.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/ForceFunctionAttrs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/FunctionAttrs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/FunctionImport.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/FunctionSpecialization.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/GlobalDCE.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/GlobalOpt.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/GlobalSplit.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/HotColdSplitting.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/IPO.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/IROutliner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/InferFunctionAttrs.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/InlineSimple.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/Inliner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/Internalize.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/LoopExtractor.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/LowerTypeTests.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/MergeFunctions.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/ModuleInliner.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/OpenMPOpt.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/PartialInlining.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/PassManagerBuilder.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/PruneEH.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/SampleContextTracker.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/SampleProfile.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/SampleProfileProbe.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/SCCP.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/StripDeadPrototypes.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/StripSymbols.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/SyntheticCountsPropagation.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/ThinLTOBitcodeWriter.cpp
${LLVM_PROJECT_SOURCE_DIR}/llvm/lib/Transforms/IPO/WholeProgramDevirt.cpp
)


set (LLVM_VERSION "15.0.0bundled")
set (LLVM_INCLUDE_DIRS
    "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/llvm/include"
    "${ClickHouse_BINARY_DIR}/contrib/llvm-project/llvm/include"
)
set (LLVM_LIBRARY_DIRS "${ClickHouse_BINARY_DIR}/contrib/llvm-project/llvm")

include_directories("${LLVM_INCLUDE_DIRS}")

# This list was generated by listing all LLVM libraries, compiling the binary and removing all libraries while it still compiles.
set (REQUIRED_LLVM_LIBRARIES
    LLVMExecutionEngine
    LLVMRuntimeDyld
    LLVMAsmPrinter
    LLVMDebugInfoDWARF
    LLVMGlobalISel
    LLVMSelectionDAG
    LLVMMCDisassembler
    LLVMPasses
    LLVMCodeGen
    LLVMipo
    LLVMBitWriter
    LLVMInstrumentation
    LLVMScalarOpts
    LLVMAggressiveInstCombine
    LLVMInstCombine
    LLVMVectorize
    LLVMTransformUtils
    LLVMTarget
    LLVMAnalysis
    LLVMProfileData
    LLVMObject
    LLVMBitReader
    LLVMCore
    LLVMRemarks
    LLVMBitstreamReader
    LLVMMCParser
    LLVMMC
    LLVMBinaryFormat
    LLVMDebugInfoCodeView
    LLVMSupport
    LLVMDemangle
)

if (ARCH_AMD64)
    set (LLVM_TARGETS_TO_BUILD "X86" CACHE INTERNAL "")
    list(APPEND REQUIRED_LLVM_LIBRARIES LLVMX86Info LLVMX86Desc LLVMX86CodeGen)
elseif (ARCH_AARCH64)
    set (LLVM_TARGETS_TO_BUILD "AArch64" CACHE INTERNAL "")
    list(APPEND REQUIRED_LLVM_LIBRARIES LLVMAArch64Info LLVMAArch64Desc LLVMAArch64CodeGen)
elseif (ARCH_PPC64LE)
    set (LLVM_TARGETS_TO_BUILD "PowerPC" CACHE INTERNAL "")
    list(APPEND REQUIRED_LLVM_LIBRARIES LLVMPowerPCInfo LLVMPowerPCDesc LLVMPowerPCCodeGen)
elseif (ARCH_S390X)
    set (LLVM_TARGETS_TO_BUILD "SystemZ" CACHE INTERNAL "")
    list(APPEND REQUIRED_LLVM_LIBRARIES LLVMSystemZInfo LLVMSystemZDesc LLVMSystemZCodeGen)
elseif (ARCH_RISCV64)
    set (LLVM_TARGETS_TO_BUILD "RISCV" CACHE INTERNAL "")
    list(APPEND REQUIRED_LLVM_LIBRARIES LLVMRISCVInfo LLVMRISCVDesc LLVMRISCVCodeGen)
endif ()

message (STATUS "LLVM TARGETS TO BUILD ${LLVM_TARGETS_TO_BUILD}")

add_library (_llvm INTERFACE)
target_link_libraries (_llvm INTERFACE ${REQUIRED_LLVM_LIBRARIES})
target_include_directories (_llvm SYSTEM BEFORE INTERFACE ${LLVM_INCLUDE_DIRS})
add_library(ch_contrib::llvm ALIAS _llvm)
