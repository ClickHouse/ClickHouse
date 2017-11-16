if (CMAKE_SYSTEM MATCHES "FreeBSD" OR ARCH_32)
    option (USE_INTERNAL_GPERFTOOLS_LIBRARY "Set to FALSE to use system gperftools (tcmalloc) library instead of bundled" OFF)
else ()
    option (USE_INTERNAL_GPERFTOOLS_LIBRARY "Set to FALSE to use system gperftools (tcmalloc) library instead of bundled" ${NOT_UNBUNDLED})
endif ()

if (CMAKE_SYSTEM MATCHES "FreeBSD")
    option (ENABLE_LIBTCMALLOC "Set to TRUE to enable libtcmalloc" OFF)
else ()
    option (ENABLE_LIBTCMALLOC "Set to TRUE to enable libtcmalloc" ON)
endif ()

option (DEBUG_LIBTCMALLOC "Set to TRUE to use debug version of libtcmalloc" OFF)

if (ENABLE_LIBTCMALLOC)
    #contrib/libtcmalloc doesnt build debug version, try find in system
    if (DEBUG_LIBTCMALLOC OR NOT USE_INTERNAL_GPERFTOOLS_LIBRARY)
        find_package (Gperftools)
    endif ()

    if (NOT (GPERFTOOLS_FOUND AND GPERFTOOLS_INCLUDE_DIR AND GPERFTOOLS_TCMALLOC_MINIMAL) AND NOT (CMAKE_SYSTEM MATCHES "FreeBSD" OR ARCH_32))
        set (USE_INTERNAL_GPERFTOOLS_LIBRARY 1)
        set (GPERFTOOLS_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libtcmalloc/include")
        set (GPERFTOOLS_TCMALLOC_MINIMAL tcmalloc_minimal_internal)
    endif ()

    if (GPERFTOOLS_FOUND OR USE_INTERNAL_GPERFTOOLS_LIBRARY)
        set (USE_TCMALLOC 1)
    endif ()

    message (STATUS "Using tcmalloc=${USE_TCMALLOC}: ${GPERFTOOLS_INCLUDE_DIR} : ${GPERFTOOLS_TCMALLOC_MINIMAL}")
endif ()
