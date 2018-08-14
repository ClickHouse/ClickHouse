# Third-party libraries may have substandard code.

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-function -Wno-unused-variable -Wno-unused-but-set-variable -Wno-unused-result -Wno-deprecated-declarations -Wno-maybe-uninitialized -Wno-format -Wno-misleading-indentation")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-old-style-cast -Wno-unused-function -Wno-unused-variable -Wno-unused-but-set-variable -Wno-unused-result -Wno-deprecated-declarations -Wno-non-virtual-dtor -Wno-maybe-uninitialized -Wno-format -Wno-misleading-indentation -Wno-implicit-fallthrough -std=c++1z")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-function -Wno-unused-variable -Wno-unused-result -Wno-deprecated-declarations -Wno-format")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-old-style-cast -Wno-unused-function -Wno-unused-variable -Wno-unused-result -Wno-deprecated-declarations -Wno-non-virtual-dtor -Wno-format -std=c++1z")
endif ()

if (USE_INTERNAL_BOOST_LIBRARY)
    add_subdirectory (boost-cmake)
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
    set (BUILD_TESTING 0 CACHE INTERNAL "")
    add_subdirectory (double-conversion)
endif ()

if (USE_INTERNAL_CITYHASH_LIBRARY)
    add_subdirectory (cityhash102)
endif ()

if (USE_INTERNAL_FARMHASH_LIBRARY)
    add_subdirectory (libfarmhash)
endif ()

if (USE_INTERNAL_METROHASH_LIBRARY)
    add_subdirectory (libmetrohash)
endif ()

add_subdirectory (murmurhash)

if (USE_INTERNAL_BTRIE_LIBRARY)
    add_subdirectory (libbtrie)
endif ()

if (USE_INTERNAL_UNWIND_LIBRARY)
    add_subdirectory (libunwind)
endif ()

if (USE_INTERNAL_ZLIB_LIBRARY)
    add_subdirectory (${INTERNAL_ZLIB_NAME})
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

if (ENABLE_JEMALLOC AND USE_INTERNAL_JEMALLOC_LIBRARY)
    add_subdirectory (jemalloc-cmake)
endif ()

if (USE_INTERNAL_CPUID_LIBRARY)
    add_subdirectory (libcpuid)
endif ()

if (USE_INTERNAL_SSL_LIBRARY)
    if (NOT MAKE_STATIC_LIBRARIES)
        set (BUILD_SHARED 1)
    endif ()
    set (USE_SHARED ${USE_STATIC_LIBRARIES})
    set (LIBRESSL_SKIP_INSTALL 1)
    add_subdirectory (ssl)
    target_include_directories(${OPENSSL_CRYPTO_LIBRARY} SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
    target_include_directories(${OPENSSL_SSL_LIBRARY} SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
endif ()

if (ENABLE_MYSQL AND USE_INTERNAL_MYSQL_LIBRARY)
    add_subdirectory (mariadb-connector-c-cmake)
    target_include_directories(mysqlclient PRIVATE BEFORE ${ZLIB_INCLUDE_DIR})
    target_include_directories(mysqlclient PRIVATE BEFORE ${OPENSSL_INCLUDE_DIR})
endif ()

if (USE_INTERNAL_RDKAFKA_LIBRARY)
    add_subdirectory (librdkafka-cmake)
    target_include_directories(rdkafka PRIVATE BEFORE ${ZLIB_INCLUDE_DIR})
    target_include_directories(rdkafka PRIVATE BEFORE ${OPENSSL_INCLUDE_DIR})
endif ()

if (ENABLE_ODBC AND USE_INTERNAL_ODBC_LIBRARY)
    add_subdirectory (unixodbc-cmake)
endif ()

if (USE_INTERNAL_CAPNP_LIBRARY)
    set (BUILD_TESTING 0 CACHE INTERNAL "")
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
    if (MSVC)
        set (ENABLE_DATA_ODBC 0 CACHE INTERNAL "") # TODO (build fail)
    endif ()
    add_subdirectory (poco)
    unset (CMAKE_DISABLE_FIND_PACKAGE_ZLIB)
    set (ENABLE_TESTS ${_save})
    set (CMAKE_CXX_FLAGS ${save_CMAKE_CXX_FLAGS})
    set (CMAKE_C_FLAGS ${save_CMAKE_C_FLAGS})

    if (OPENSSL_FOUND AND TARGET Crypto AND (NOT DEFINED ENABLE_POCO_NETSSL OR ENABLE_POCO_NETSSL))
        # Bug in poco https://github.com/pocoproject/poco/pull/2100 found on macos
        target_include_directories(Crypto SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
    endif ()
endif ()

if (USE_INTERNAL_LLVM_LIBRARY)
    # ld: unknown option: --color-diagnostics
    if (APPLE AND COMPILER_GCC)
        set (LINKER_SUPPORTS_COLOR_DIAGNOSTICS 0 CACHE INTERNAL "")
    endif ()
    add_subdirectory (llvm/llvm)
endif ()
