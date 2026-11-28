if (ENABLE_FUZZING AND TARGET ch_contrib::nuraft)
    add_subdirectory(fuzzers)
endif()
