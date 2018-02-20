set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-old-style-cast")

if (USE_INTERNAL_BOOST_LIBRARY)
    add_subdirectory (libboost)
endif ()

if (USE_INTERNAL_LZ4_LIBRARY)
    add_subdirectory (lz4-cmake)
endif ()

if (USE_INTERNAL_ZSTD_LIBRARY)
    add_subdirectory (zstd-cmake)
endif ()

if (USE_INTERNAL_RE2_LIBRARY)
    set(RE2_BUILD_TESTING 0 CACHE INTERNAL "")
    add_subdirectory (re2)
    add_subdirectory (re2_st)
endif ()

if (USE_INTERNAL_DOUBLE_CONVERSION_LIBRARY)
    set (BUILD_TESTING ${ENABLE_TESTS} CACHE INTERNAL "")
    add_subdirectory (double-conversion)
endif ()

if (USE_INTERNAL_ZOOKEEPER_LIBRARY)
    add_subdirectory (zookeeper/src/c)
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
    add_subdirectory (zlib-ng)
    # todo: make pull to Dead2/zlib-ng and remove:
    # We should use same defines when including zlib.h as used when zlib compiled
    target_compile_definitions (zlib PUBLIC ZLIB_COMPAT WITH_GZFILEOP)
    target_compile_definitions (zlibstatic PUBLIC ZLIB_COMPAT WITH_GZFILEOP)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "AMD64")
       target_compile_definitions (zlib PUBLIC X86_64)
       target_compile_definitions (zlibstatic PUBLIC X86_64)
    endif ()

    set_target_properties(example PROPERTIES EXCLUDE_FROM_ALL 1)
    if (TARGET example64)
        set_target_properties(example64 PROPERTIES EXCLUDE_FROM_ALL 1)
    endif ()

    set_target_properties(minigzip PROPERTIES EXCLUDE_FROM_ALL 1)
    if (TARGET minigzip64)
        set_target_properties(minigzip64 PROPERTIES EXCLUDE_FROM_ALL 1)
    endif ()
endif ()

if (USE_INTERNAL_CCTZ_LIBRARY)
    add_subdirectory (cctz-cmake)
endif ()

if (ENABLE_TCMALLOC AND USE_INTERNAL_GPERFTOOLS_LIBRARY)
    add_subdirectory (libtcmalloc)
endif ()

if (NOT ARCH_ARM)
    add_subdirectory (libcpuid)
endif ()

if (USE_INTERNAL_RDKAFKA_LIBRARY)
    set (RDKAFKA_BUILD_EXAMPLES OFF CACHE INTERNAL "")
    set (RDKAFKA_BUILD_TESTS OFF CACHE INTERNAL "")
    set (RDKAFKA_BUILD_STATIC ON CACHE INTERNAL "")
    mark_as_advanced (ZLIB_INCLUDE_DIR)
    add_subdirectory (librdkafka)
endif ()

if (USE_INTERNAL_CAPNP_LIBRARY)
    if (NOT APPLE) # tests never end
        set (BUILD_TESTING ${ENABLE_TESTS} CACHE INTERNAL "")
    endif ()
    set (_save ${CMAKE_CXX_EXTENSIONS})
    set (CMAKE_CXX_EXTENSIONS)
    add_subdirectory (capnproto/c++)
    set (CMAKE_CXX_EXTENSIONS ${_save})
    target_include_directories(${CAPNP_LIBRARY} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/capnproto/c++/src>)
endif ()


if (USE_INTERNAL_POCO_LIBRARY)
    set (save_CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
    set (save_CMAKE_C_FLAGS ${CMAKE_C_FLAGS})
    set (_save ${ENABLE_TESTS})
    set (ENABLE_TESTS 0)
    set (CMAKE_DISABLE_FIND_PACKAGE_ZLIB 1)
    add_subdirectory (poco)
    unset (CMAKE_DISABLE_FIND_PACKAGE_ZLIB)
    set (ENABLE_TESTS ${_save})
    set (CMAKE_CXX_FLAGS ${save_CMAKE_CXX_FLAGS})
    set (CMAKE_C_FLAGS ${save_CMAKE_C_FLAGS})
    if (OPENSSL_FOUND)
        # Bug in poco https://github.com/pocoproject/poco/pull/2100 found on macos
        target_include_directories(Crypto PUBLIC ${OPENSSL_INCLUDE_DIR})
    endif ()
endif ()
