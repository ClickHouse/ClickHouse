if (USE_CUDA)
    add_subdirectory (Cuda)
endif ()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
