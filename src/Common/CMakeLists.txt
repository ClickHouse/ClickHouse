add_subdirectory(StringUtils)
# after common_io
#add_subdirectory(ZooKeeper)
#add_subdirectory(ConfigProcessor)

if (USE_CUDA)
    add_subdirectory (Cuda)
endif ()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
