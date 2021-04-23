add_subdirectory(StringUtils)
# after common_io
#add_subdirectory(ZooKeeper)
#add_subdirectory(ConfigProcessor)

add_subdirectory(JSONParsers)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
