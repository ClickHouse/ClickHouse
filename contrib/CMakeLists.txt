# Third-party libraries may have substandard code.

# Put all targets defined here and in added subfolders under "contrib/" folder in GUI-based IDEs by default.
# Some of third-party projects may override CMAKE_FOLDER or FOLDER property of their targets, so they will
# appear not in "contrib/" as originally planned here.
get_filename_component (_current_dir_name "${CMAKE_CURRENT_LIST_DIR}" NAME)
if (CMAKE_FOLDER)
    set (CMAKE_FOLDER "${CMAKE_FOLDER}/${_current_dir_name}")
else ()
    set (CMAKE_FOLDER "${_current_dir_name}")
endif ()
unset (_current_dir_name)

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

set_property(DIRECTORY PROPERTY EXCLUDE_FROM_ALL 1)

add_subdirectory (boost-cmake)
add_subdirectory (cctz-cmake)
add_subdirectory (consistent-hashing-sumbur)
add_subdirectory (consistent-hashing)
add_subdirectory (croaring)
add_subdirectory (FastMemcpy)
add_subdirectory (hyperscan-cmake)
add_subdirectory (jemalloc-cmake)
add_subdirectory (libcpuid-cmake)
add_subdirectory (libdivide)
add_subdirectory (libmetrohash)
add_subdirectory (lz4-cmake)
add_subdirectory (murmurhash)
add_subdirectory (replxx-cmake)
add_subdirectory (ryu-cmake)
add_subdirectory (unixodbc-cmake)

add_subdirectory (poco-cmake)


# TODO: refactor the contrib libraries below this comment.

if (USE_INTERNAL_ZSTD_LIBRARY)
    add_subdirectory (zstd-cmake)
endif ()

if (USE_INTERNAL_RE2_LIBRARY)
    set(RE2_BUILD_TESTING 0 CACHE INTERNAL "")
    add_subdirectory (re2)
    add_subdirectory (re2_st)
endif ()

if (USE_INTERNAL_DOUBLE_CONVERSION_LIBRARY)
    add_subdirectory (double-conversion-cmake)
endif ()

if (USE_INTERNAL_CITYHASH_LIBRARY)
    add_subdirectory (cityhash102)
endif ()

if (USE_INTERNAL_FARMHASH_LIBRARY)
    add_subdirectory (libfarmhash)
endif ()

if (USE_INTERNAL_BTRIE_LIBRARY)
    add_subdirectory (libbtrie)
endif ()

if (USE_INTERNAL_ZLIB_LIBRARY)
    set (ZLIB_ENABLE_TESTS 0 CACHE INTERNAL "")
    set (SKIP_INSTALL_ALL 1 CACHE INTERNAL "")
    set (ZLIB_COMPAT 1 CACHE INTERNAL "") # also enables WITH_GZFILEOP
    set (WITH_NATIVE_INSTRUCTIONS ${ARCH_NATIVE} CACHE INTERNAL "")
    if (OS_FREEBSD OR ARCH_I386)
        set (WITH_OPTIM 0 CACHE INTERNAL "") # Bug in assembler
    endif ()
    if (ARCH_AARCH64)
        set(WITH_NEON 1 CACHE INTERNAL "")
        set(WITH_ACLE 1 CACHE INTERNAL "")
    endif ()

    add_subdirectory (${INTERNAL_ZLIB_NAME})
    # We should use same defines when including zlib.h as used when zlib compiled
    target_compile_definitions (zlib PUBLIC ZLIB_COMPAT WITH_GZFILEOP)
    if (TARGET zlibstatic)
        target_compile_definitions (zlibstatic PUBLIC ZLIB_COMPAT WITH_GZFILEOP)
    endif ()
    if (ARCH_AMD64 OR ARCH_AARCH64)
        target_compile_definitions (zlib PUBLIC X86_64 UNALIGNED_OK)
        if (TARGET zlibstatic)
            target_compile_definitions (zlibstatic PUBLIC X86_64 UNALIGNED_OK)
        endif ()
    endif ()
endif ()

if (USE_INTERNAL_H3_LIBRARY)
    add_subdirectory(h3-cmake)
endif ()

if (USE_INTERNAL_SSL_LIBRARY)
    add_subdirectory (openssl-cmake)

    add_library(OpenSSL::Crypto ALIAS ${OPENSSL_CRYPTO_LIBRARY})
    add_library(OpenSSL::SSL ALIAS ${OPENSSL_SSL_LIBRARY})
endif ()

if (USE_INTERNAL_LDAP_LIBRARY)
    add_subdirectory (openldap-cmake)
endif ()

# Should go before:
# - mariadb-connector-c
# - aws-s3-cmake
# - sentry-native
add_subdirectory (curl-cmake)

function(mysql_support)
    set(CLIENT_PLUGIN_CACHING_SHA2_PASSWORD STATIC)
    set(CLIENT_PLUGIN_SHA256_PASSWORD STATIC)
    set(CLIENT_PLUGIN_REMOTE_IO OFF)
    set(CLIENT_PLUGIN_DIALOG OFF)
    set(CLIENT_PLUGIN_AUTH_GSSAPI_CLIENT OFF)
    set(CLIENT_PLUGIN_CLIENT_ED25519 OFF)
    set(CLIENT_PLUGIN_MYSQL_CLEAR_PASSWORD OFF)
    set(SKIP_TESTS 1)
    if (GLIBC_COMPATIBILITY)
        set(LIBM glibc-compatibility)
    endif()
    if (USE_INTERNAL_ZLIB_LIBRARY)
        set(ZLIB_FOUND ON)
        set(ZLIB_LIBRARY ${ZLIB_LIBRARIES})
        set(WITH_EXTERNAL_ZLIB ON)
    endif()
    add_subdirectory (mariadb-connector-c)
endfunction()
if (ENABLE_MYSQL AND USE_INTERNAL_MYSQL_LIBRARY)
    mysql_support()
endif ()

if (USE_INTERNAL_RDKAFKA_LIBRARY)
    add_subdirectory (librdkafka-cmake)
    target_include_directories(rdkafka BEFORE PRIVATE ${ZLIB_INCLUDE_DIR})
    if(OPENSSL_INCLUDE_DIR)
        target_include_directories(rdkafka BEFORE PRIVATE ${OPENSSL_INCLUDE_DIR})
    endif()
endif ()

if (USE_RDKAFKA)
    add_subdirectory (cppkafka-cmake)
endif()

if (ENABLE_ICU AND USE_INTERNAL_ICU_LIBRARY)
    add_subdirectory (icu-cmake)
endif ()

if(USE_INTERNAL_SNAPPY_LIBRARY)
    set(SNAPPY_BUILD_TESTS 0 CACHE INTERNAL "")

    add_subdirectory(snappy)

    set (SNAPPY_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/snappy")
    if(SANITIZE STREQUAL "undefined")
        target_compile_options(${SNAPPY_LIBRARY} PRIVATE -fno-sanitize=undefined)
    endif()
endif()

if (USE_INTERNAL_PARQUET_LIBRARY)
if (USE_INTERNAL_PARQUET_LIBRARY_NATIVE_CMAKE)
    # We dont use arrow's cmakefiles because they uses too many depends and download some libs in compile time
    # But this mode can be used for updating auto-generated parquet files:
    # cmake -DUSE_INTERNAL_PARQUET_LIBRARY_NATIVE_CMAKE=1 -DUSE_STATIC_LIBRARIES=0
    # copy {BUILD_DIR}/contrib/arrow/cpp/src/parquet/*.cpp,*.h -> /contrib/arrow-cmake/cpp/src/parquet/

    # Also useful parquet reader:
    # cd contrib/arrow/cpp/build && mkdir -p build && cmake .. -DPARQUET_BUILD_EXECUTABLES=1 && make -j8
    # contrib/arrow/cpp/build/debug/parquet-reader some_file.parquet

    set (ARROW_COMPUTE ON CACHE INTERNAL "")
    set (ARROW_PARQUET ON CACHE INTERNAL "")
    set (ARROW_VERBOSE_THIRDPARTY_BUILD ON CACHE INTERNAL "")
    set (ARROW_BUILD_SHARED 1 CACHE INTERNAL "")
    set (ARROW_BUILD_UTILITIES OFF CACHE INTERNAL "")
    set (ARROW_BUILD_INTEGRATION OFF CACHE INTERNAL "")
    set (ARROW_BOOST_HEADER_ONLY ON CACHE INTERNAL "")
    set (Boost_FOUND 1 CACHE INTERNAL "")
    if (MAKE_STATIC_LIBRARIES)
        set (PARQUET_ARROW_LINKAGE "static" CACHE INTERNAL "")
        set (ARROW_TEST_LINKAGE "static" CACHE INTERNAL "")
        set (ARROW_BUILD_STATIC ${MAKE_STATIC_LIBRARIES} CACHE INTERNAL "")
    else ()
        set (PARQUET_ARROW_LINKAGE "shared" CACHE INTERNAL "")
        set (ARROW_TEST_LINKAGE "shared" CACHE INTERNAL "")
    endif ()

    if (CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO")
        set (_save_build_type ${CMAKE_BUILD_TYPE})
        set (CMAKE_BUILD_TYPE Release)
        string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)
    endif ()

    # Because Arrow uses CMAKE_SOURCE_DIR as a project path
    # Hopefully will be fixed in https://github.com/apache/arrow/pull/2676
    set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/cmake_modules")
    add_subdirectory (arrow/cpp)

    if (_save_build_type)
        set (CMAKE_BUILD_TYPE ${_save_build_type})
        unset (_save_build_type)
        string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)
    endif ()

else()
    add_subdirectory(arrow-cmake)

    # The library is large - avoid bloat.
    target_compile_options (${ARROW_LIBRARY} PRIVATE -g0)
    target_compile_options (${THRIFT_LIBRARY} PRIVATE -g0)
    target_compile_options (${PARQUET_LIBRARY} PRIVATE -g0)
endif()
endif()

if (USE_INTERNAL_AVRO_LIBRARY)
    add_subdirectory(avro-cmake)
endif()

if(USE_INTERNAL_GTEST_LIBRARY)
    set(GOOGLETEST_VERSION 1.10.0) # master
    # Google Test from sources
    add_subdirectory(${ClickHouse_SOURCE_DIR}/contrib/googletest/googletest ${CMAKE_CURRENT_BINARY_DIR}/googletest)
    # avoid problems with <regexp.h>
    target_compile_definitions (gtest INTERFACE GTEST_HAS_POSIX_RE=0)
elseif(GTEST_SRC_DIR)
    add_subdirectory(${GTEST_SRC_DIR}/googletest ${CMAKE_CURRENT_BINARY_DIR}/googletest)
    target_compile_definitions(gtest INTERFACE GTEST_HAS_POSIX_RE=0)
endif()

if (USE_EMBEDDED_COMPILER AND USE_INTERNAL_LLVM_LIBRARY)
    # ld: unknown option: --color-diagnostics
    if (APPLE)
        set (LINKER_SUPPORTS_COLOR_DIAGNOSTICS 0 CACHE INTERNAL "")
    endif ()
    set (LLVM_ENABLE_EH 1 CACHE INTERNAL "")
    set (LLVM_ENABLE_RTTI 1 CACHE INTERNAL "")
    set (LLVM_ENABLE_PIC 0 CACHE INTERNAL "")
    set (LLVM_TARGETS_TO_BUILD "X86;AArch64" CACHE STRING "")
    add_subdirectory (llvm/llvm)
    target_include_directories(LLVMSupport SYSTEM BEFORE PRIVATE ${ZLIB_INCLUDE_DIR})
endif ()

if (USE_INTERNAL_LIBGSASL_LIBRARY)
    add_subdirectory(libgsasl)
endif()

if (USE_INTERNAL_LIBXML2_LIBRARY)
    add_subdirectory(libxml2-cmake)
endif ()

if (USE_INTERNAL_BROTLI_LIBRARY)
    add_subdirectory(brotli-cmake)
    target_compile_definitions(brotli PRIVATE BROTLI_BUILD_PORTABLE=1)
endif ()

if (USE_INTERNAL_PROTOBUF_LIBRARY)
    add_subdirectory(protobuf-cmake)
endif ()

if (USE_INTERNAL_HDFS3_LIBRARY)
    add_subdirectory(libhdfs3-cmake)
endif ()

if (USE_INTERNAL_GRPC_LIBRARY)
    add_subdirectory(grpc-cmake)
endif ()

if (USE_INTERNAL_AWS_S3_LIBRARY)
    add_subdirectory(aws-s3-cmake)

    # The library is large - avoid bloat.
    target_compile_options (aws_s3 PRIVATE -g0)
    target_compile_options (aws_s3_checksums PRIVATE -g0)

endif ()

if (USE_BASE64)
    add_subdirectory (base64-cmake)
endif()

if (USE_SIMDJSON)
    add_subdirectory (simdjson-cmake)
endif()

if (USE_FASTOPS)
    add_subdirectory (fastops-cmake)
endif()

if (USE_AMQPCPP OR USE_CASSANDRA)
    add_subdirectory (libuv)
endif()
if (USE_AMQPCPP)
    add_subdirectory (amqpcpp-cmake)
endif()
if (USE_CASSANDRA)
    add_subdirectory (cassandra)
endif()

if (USE_SENTRY)
    add_subdirectory (sentry-native)
endif()

add_subdirectory (fmtlib-cmake)

if (USE_STATS)
    add_subdirectory (stats-cmake)
    add_subdirectory (gcem)
endif()
