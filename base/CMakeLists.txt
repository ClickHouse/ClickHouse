if (USE_DEBUG_HELPERS)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/common/iostream_debug_helpers.h")
endif ()

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
