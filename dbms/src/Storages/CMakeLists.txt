add_subdirectory (System)

if (ENABLE_TESTS)
    add_subdirectory (tests)
    add_subdirectory (NextGenReplication/tests)
endif ()
