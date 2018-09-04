if (NOT NO_WERROR)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
endif ()

if (USE_DEBUG_HELPERS)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/libcommon/include/common/iostream_debug_helpers.h")
endif ()

add_subdirectory (libcommon)
add_subdirectory (libpocoext)
add_subdirectory (libdaemon)

if (USE_INTERNAL_MEMCPY)
    add_subdirectory (libmemcpy)
endif()

if (GLIBC_COMPATIBILITY)
    add_subdirectory (libglibc-compatibility)
endif ()

if (USE_MYSQL)
    add_subdirectory (libmysqlxx)
endif ()

if (USE_INTERNAL_CONSISTENT_HASHING_LIBRARY)
    add_subdirectory (consistent-hashing)
endif ()
add_subdirectory (consistent-hashing-sumbur)
