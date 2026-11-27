if (TARGET ch_contrib::grpc)
    add_subdirectory (grpc_protos)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()
