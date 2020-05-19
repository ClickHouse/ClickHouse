if(USE_HYPERSCAN)
    target_link_libraries(clickhouse_functions PRIVATE ${HYPERSCAN_LIBRARY})
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${HYPERSCAN_INCLUDE_DIR})
endif()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
