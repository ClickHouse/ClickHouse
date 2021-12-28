add_subdirectory(StringUtils)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (USE_MYSQL)
    add_subdirectory (mysqlxx)
endif ()
