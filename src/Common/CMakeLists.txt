add_subdirectory(StringUtils)
add_subdirectory(UatraitsFast)
# after common_io
#add_subdirectory(ZooKeeper)
#add_subdirectory(ConfigProcessor)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
