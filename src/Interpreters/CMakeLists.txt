if(USE_HYPERSCAN)
    link_libraries(${HYPERSCAN_LIBRARY})
    include_directories(${HYPERSCAN_INCLUDE_DIR})
endif()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
