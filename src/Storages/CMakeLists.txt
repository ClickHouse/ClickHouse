add_subdirectory(MergeTree)
add_subdirectory(System)

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
