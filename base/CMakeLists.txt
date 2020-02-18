add_subdirectory (common)
add_subdirectory (loggers)
add_subdirectory (daemon)

if (USE_INTERNAL_MEMCPY)
    add_subdirectory (memcpy)
endif()

if (USE_MYSQL)
    add_subdirectory (mysqlxx)
endif ()

if (USE_INTERNAL_CONSISTENT_HASHING_LIBRARY)
    add_subdirectory (consistent-hashing)
endif ()
add_subdirectory (consistent-hashing-sumbur)
add_subdirectory (widechar_width)
