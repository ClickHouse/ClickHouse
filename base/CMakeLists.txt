if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

add_subdirectory (common)
add_subdirectory (daemon)
add_subdirectory (loggers)
add_subdirectory (pcg-random)
add_subdirectory (widechar_width)

if (USE_MYSQL)
    add_subdirectory (mysqlxx)
endif ()

if (USE_SENTRY)
    target_link_libraries (daemon PRIVATE curl)
    target_link_libraries (daemon PRIVATE ${SENTRY_LIBRARY})
endif ()