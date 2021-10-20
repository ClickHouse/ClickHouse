# Third-party libraries may have substandard code.

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

if (WITH_COVERAGE)
  set (WITHOUT_COVERAGE_LIST ${WITHOUT_COVERAGE})
  separate_arguments(WITHOUT_COVERAGE_LIST)
  # disable coverage for contib files and build with optimisations
  if (COMPILER_CLANG)
      add_compile_options(-O3 -DNDEBUG -finline-functions -finline-hint-functions ${WITHOUT_COVERAGE_LIST})
  else()
      add_compile_options(-O3 -DNDEBUG -finline-functions ${WITHOUT_COVERAGE_LIST})
  endif()
endif()

if (SANITIZE STREQUAL "undefined")
    # 3rd-party libraries usually not intended to work with UBSan.
    add_compile_options(-fno-sanitize=undefined)
endif()

set_property(DIRECTORY PROPERTY EXCLUDE_FROM_ALL 1)

add_subdirectory (abseil-cpp-cmake)
add_subdirectory (magic-enum-cmake)
add_subdirectory (boost-cmake)
add_subdirectory (cctz-cmake)
add_subdirectory (consistent-hashing)
add_subdirectory (dragonbox-cmake)
add_subdirectory (hyperscan-cmake)
add_subdirectory (jemalloc-cmake)
add_subdirectory (libcpuid-cmake)
add_subdirectory (libdivide)
add_subdirectory (libmetrohash)
add_subdirectory (lz4-cmake)
add_subdirectory (murmurhash)
add_subdirectory (replxx-cmake)
add_subdirectory (unixodbc-cmake)
add_subdirectory (nanodbc-cmake)

if (USE_INTERNAL_CAPNP_LIBRARY AND NOT MISSING_INTERNAL_CAPNP_LIBRARY)
    add_subdirectory(capnproto-cmake)
endif ()

if (ENABLE_FUZZING)
    add_subdirectory (libprotobuf-mutator-cmake)
endif()

if (USE_YAML_CPP)
    add_subdirectory (yaml-cpp-cmake)
endif()

if (USE_INTERNAL_XZ_LIBRARY)
    add_subdirectory (xz)
endif()

add_subdirectory (poco-cmake)
add_subdirectory (croaring-cmake)

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
    if (ARCH_AMD64 OR ARCH_AARCH64)
        target_compile_definitions (zlib PUBLIC X86_64 UNALIGNED_OK)
    endif ()
endif ()

if (USE_INTERNAL_H3_LIBRARY)
    add_subdirectory(h3-cmake)
endif ()

if (USE_INTERNAL_SSL_LIBRARY)
    add_subdirectory (boringssl-cmake)

    add_library(OpenSSL::Crypto ALIAS crypto)
    add_library(OpenSSL::SSL ALIAS ssl)
endif ()

if (USE_INTERNAL_LDAP_LIBRARY)
    add_subdirectory (openldap-cmake)
endif ()

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
    set(WITH_CURL OFF)
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

    add_subdirectory(snappy-cmake)

    set (SNAPPY_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/snappy")
endif()

if (USE_INTERNAL_PARQUET_LIBRARY)
    # We dont use arrow's cmakefiles because they uses too many depends and download some libs in compile time
    # But you can update auto-generated parquet files manually:
    # cd {BUILD_DIR}/contrib/arrow/cpp/src/parquet && mkdir -p build && cd build
    # cmake .. -DARROW_COMPUTE=ON -DARROW_PARQUET=ON -DARROW_SIMD_LEVEL=NONE -DARROW_VERBOSE_THIRDPARTY_BUILD=ON
    #          -DARROW_BUILD_SHARED=1 -DARROW_BUILD_UTILITIES=OFF -DARROW_BUILD_INTEGRATION=OFF
    #          -DBoost_FOUND=1 -DARROW_TEST_LINKAGE="shared"
    # make -j8
    # copy {BUILD_DIR}/contrib/arrow/cpp/src/parquet/*.cpp,*.h -> {BUILD_DIR}/contrib/arrow-cmake/cpp/src/parquet/

    # Also useful parquet reader:
    # cd {BUILD_DIR}/contrib/arrow/cpp && mkdir -p build && cd build
    # cmake .. -DARROW_PARQUET=1 -DARROW_WITH_SNAPPY=1 -DPARQUET_BUILD_EXECUTABLES=1
    # make -j8
    # {BUILD_DIR}/contrib/arrow/cpp/build/release/parquet-reader some_file.parquet

    add_subdirectory(arrow-cmake)

    # The library is large - avoid bloat.
    target_compile_options (${ARROW_LIBRARY} PRIVATE -g0)
    target_compile_options (${THRIFT_LIBRARY} PRIVATE -g0)
    target_compile_options (${PARQUET_LIBRARY} PRIVATE -g0)
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

function(add_llvm)
    # ld: unknown option: --color-diagnostics
    if (APPLE)
        set (LINKER_SUPPORTS_COLOR_DIAGNOSTICS 0 CACHE INTERNAL "")
    endif ()

    # Do not adjust RPATH in llvm, since then it will not be able to find libcxx/libcxxabi/libunwind
    set (CMAKE_INSTALL_RPATH "ON")
    set (LLVM_COMPILER_CHECKED 1 CACHE INTERNAL "")
    set (LLVM_ENABLE_EH 1 CACHE INTERNAL "")
    set (LLVM_ENABLE_RTTI 1 CACHE INTERNAL "")
    set (LLVM_ENABLE_PIC 0 CACHE INTERNAL "")
    set (LLVM_TARGETS_TO_BUILD "X86;AArch64" CACHE STRING "")

    # Need to use C++17 since the compilation is not possible with C++20 currently, due to ambiguous operator != etc.
    # LLVM project will set its default value for the -std=... but our global setting from CMake will override it.
    set (CMAKE_CXX_STANDARD 17)

    add_subdirectory (llvm/llvm)
endfunction()
if (USE_EMBEDDED_COMPILER)
    add_llvm()
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
    add_subdirectory (libuv-cmake)
endif()
if (USE_AMQPCPP)
    add_subdirectory (amqpcpp-cmake)
endif()
if (USE_CASSANDRA)
    # Need to use C++17 since the compilation is not possible with C++20 currently.
    set (CMAKE_CXX_STANDARD_bak ${CMAKE_CXX_STANDARD})
    set (CMAKE_CXX_STANDARD 17)

    add_subdirectory (cassandra)

    set (CMAKE_CXX_STANDARD ${CMAKE_CXX_STANDARD_bak})
    unset (CMAKE_CXX_STANDARD_bak)
endif()

# Should go before:
# - sentry-native
add_subdirectory (curl-cmake)

if (USE_SENTRY)
    add_subdirectory (sentry-native)
endif()

add_subdirectory (fmtlib-cmake)

if (USE_STATS)
    add_subdirectory (stats-cmake)
    add_subdirectory (gcem)
endif()

if (USE_KRB5)
    add_subdirectory (krb5-cmake)
    if (USE_CYRUS_SASL)
        add_subdirectory (cyrus-sasl-cmake)
    endif()
endif()

if (USE_INTERNAL_ROCKSDB_LIBRARY)
    add_subdirectory(rocksdb-cmake)
endif()

if (USE_LIBPQXX)
    add_subdirectory (libpq-cmake)
    add_subdirectory (libpqxx-cmake)
endif()

if (USE_NURAFT)
  add_subdirectory(nuraft-cmake)
endif()

add_subdirectory(fast_float)

if (USE_NLP)
    add_subdirectory(libstemmer-c-cmake)
    add_subdirectory(wordnet-blast-cmake)
    add_subdirectory(lemmagen-c-cmake)
endif()

if (USE_BZIP2)
    add_subdirectory(bzip2-cmake)
endif()

if (USE_SQLITE)
    add_subdirectory(sqlite-cmake)
endif()

if (USE_S2_GEOMETRY)
    add_subdirectory(s2geometry-cmake)
endif()

# Put all targets defined here and in subdirectories under "contrib/<immediate-subdir>" folders in GUI-based IDEs.
# Some of third-party projects may override CMAKE_FOLDER or FOLDER property of their targets, so they would not appear
# in "contrib/..." as originally planned, so we workaround this by fixing FOLDER properties of all targets manually,
# instead of controlling it via CMAKE_FOLDER.

function (ensure_target_rooted_in _target _folder)
    # Skip INTERFACE library targets, since FOLDER property is not available for them.
    get_target_property (_target_type "${_target}" TYPE)
    if (_target_type STREQUAL "INTERFACE_LIBRARY")
        return ()
    endif ()

    # Read the original FOLDER property value, if any.
    get_target_property (_folder_prop "${_target}" FOLDER)

    # Normalize that value, so we avoid possible repetitions in folder names.

    if (NOT _folder_prop)
        set (_folder_prop "")
    endif ()

    if (CMAKE_FOLDER AND _folder_prop MATCHES "^${CMAKE_FOLDER}/(.*)\$")
        set (_folder_prop "${CMAKE_MATCH_1}")
    endif ()

    if (_folder AND _folder_prop MATCHES "^${_folder}/(.*)\$")
        set (_folder_prop "${CMAKE_MATCH_1}")
    endif ()

    if (_folder)
        set (_folder_prop "${_folder}/${_folder_prop}")
    endif ()

    if (CMAKE_FOLDER)
        set (_folder_prop "${CMAKE_FOLDER}/${_folder_prop}")
    endif ()

    # Set the updated FOLDER property value back.
    set_target_properties ("${_target}" PROPERTIES FOLDER "${_folder_prop}")
endfunction ()

function (ensure_own_targets_are_rooted_in _dir _folder)
    get_directory_property (_targets DIRECTORY "${_dir}" BUILDSYSTEM_TARGETS)
    foreach (_target IN LISTS _targets)
        ensure_target_rooted_in ("${_target}" "${_folder}")
    endforeach ()
endfunction ()

function (ensure_all_targets_are_rooted_in _dir _folder)
    ensure_own_targets_are_rooted_in ("${_dir}" "${_folder}")

    get_property (_sub_dirs DIRECTORY "${_dir}" PROPERTY SUBDIRECTORIES)
    foreach (_sub_dir IN LISTS _sub_dirs)
        ensure_all_targets_are_rooted_in ("${_sub_dir}" "${_folder}")
    endforeach ()
endfunction ()

function (organize_ide_folders_2_level _dir)
    get_filename_component (_dir_name "${_dir}" NAME)
    ensure_own_targets_are_rooted_in ("${_dir}" "${_dir_name}")

    # Note, that we respect only first two levels of nesting, we don't want to
    # reorganize target folders further within each third-party dir.

    get_property (_sub_dirs DIRECTORY "${_dir}" PROPERTY SUBDIRECTORIES)
    foreach (_sub_dir IN LISTS _sub_dirs)
        get_filename_component (_sub_dir_name "${_sub_dir}" NAME)
        ensure_all_targets_are_rooted_in ("${_sub_dir}" "${_dir_name}/${_sub_dir_name}")
    endforeach ()
endfunction ()

organize_ide_folders_2_level ("${CMAKE_CURRENT_LIST_DIR}")
