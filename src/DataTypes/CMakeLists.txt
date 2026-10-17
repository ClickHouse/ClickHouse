add_subdirectory (Serializations)

if (ENABLE_EXAMPLES)
    add_subdirectory (examples)
endif ()

if (ENABLE_FUZZER_TARGETS)
    add_subdirectory(fuzzers)
endif()
