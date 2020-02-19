add_subdirectory (common)
add_subdirectory (loggers)
add_subdirectory (daemon)

if (USE_MYSQL)
    add_subdirectory (mysqlxx)
endif ()
