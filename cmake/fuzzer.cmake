option (FUZZER "Enable fuzzer: libfuzzer")

if (FUZZER)
    if (FUZZER STREQUAL "libfuzzer")
        # NOTE: Eldar Zaitov decided to name it "libfuzzer" instead of "fuzzer" to keep in mind another possible fuzzer backends.
        # NOTE: no-link means that all the targets are built with instrumentation for fuzzer, but only some of them (tests) have entry point for fuzzer and it's not checked.
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} -fsanitize=fuzzer-no-link")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} -fsanitize=fuzzer-no-link")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=fuzzer-no-link")
        endif()

        # NOTE: oss-fuzz can change LIB_FUZZING_ENGINE variable
        if (NOT LIB_FUZZING_ENGINE)
            set (LIB_FUZZING_ENGINE "-fsanitize=fuzzer")
        endif ()

    else ()
        message (FATAL_ERROR "Unknown fuzzer type: ${FUZZER}")
    endif ()
endif()
