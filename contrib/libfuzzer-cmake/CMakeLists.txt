set(COMPILER_RT_FUZZER_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/compiler-rt/lib/fuzzer")

set(FUZZER_SRCS
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerCrossOver.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerDataFlowTrace.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerDriver.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerExtFunctionsDlsym.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerExtFunctionsWeak.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerExtFunctionsWindows.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerExtraCounters.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerExtraCountersDarwin.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerExtraCountersWindows.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerFork.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerIO.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerIOPosix.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerIOWindows.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerLoop.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerMerge.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerMutate.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerSHA1.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerTracePC.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerUtil.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerUtilDarwin.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerUtilFuchsia.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerUtilLinux.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerUtilPosix.cpp"
    "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerUtilWindows.cpp"
)

add_library(_fuzzer_no_main STATIC ${FUZZER_SRCS})
add_library(ch_contrib::fuzzer_no_main ALIAS _fuzzer_no_main)

add_library(_fuzzer STATIC ${FUZZER_SRCS} "${COMPILER_RT_FUZZER_SRC_DIR}/FuzzerMain.cpp")
add_library(ch_contrib::fuzzer ALIAS _fuzzer)

