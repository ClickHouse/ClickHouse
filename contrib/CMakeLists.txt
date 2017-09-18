set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-old-style-cast")

if (USE_INTERNAL_BOOST_LIBRARY)
    add_subdirectory (libboost)
endif ()

if (USE_INTERNAL_POCO_LIBRARY)
    add_subdirectory (libpoco)
endif ()

if (USE_INTERNAL_LZ4_LIBRARY)
    add_subdirectory (liblz4)
endif ()

if (USE_INTERNAL_ZSTD_LIBRARY)
    add_subdirectory (libzstd)
endif ()

if (USE_INTERNAL_RE2_LIBRARY)
    add_subdirectory (libre2)
endif ()

if (USE_INTERNAL_DOUBLE_CONVERSION_LIBRARY)
    add_subdirectory (libdouble-conversion)
endif ()

if (USE_INTERNAL_ZOOKEEPER_LIBRARY)
    add_subdirectory (libzookeeper)
endif ()

if (USE_INTERNAL_CITYHASH_LIBRARY)
    add_subdirectory (libcityhash)
endif ()

if (USE_INTERNAL_FARMHASH_LIBRARY)
    add_subdirectory (libfarmhash)
endif ()

if (USE_INTERNAL_METROHASH_LIBRARY)
    add_subdirectory (libmetrohash)
endif ()

if (USE_INTERNAL_BTRIE_LIBRARY)
    add_subdirectory (libbtrie)
endif ()

if (USE_INTERNAL_UNWIND_LIBRARY)
    add_subdirectory (libunwind)
endif ()

if (USE_INTERNAL_ZLIB_LIBRARY)
    add_subdirectory (libzlib-ng)
endif ()

if (USE_INTERNAL_CCTZ_LIBRARY)
    add_subdirectory (libcctz)
endif ()

if (ENABLE_LIBTCMALLOC AND USE_INTERNAL_GPERFTOOLS_LIBRARY)
    add_subdirectory (libtcmalloc)
endif ()

if (NOT ARCH_ARM)
    add_subdirectory (libcpuid)
endif ()
