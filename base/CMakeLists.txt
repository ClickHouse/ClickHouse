if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

add_subdirectory (common)
add_subdirectory (loggers)
add_subdirectory (daemon)

if (USE_MYSQL)
    add_subdirectory (mysqlxx)
endif ()
