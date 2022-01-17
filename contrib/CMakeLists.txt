#"${folder}/CMakeLists.txt" Third-party libraries may have substandard code.

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

add_subdirectory (sparsehash-c11-cmake)
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
add_subdirectory (capnproto-cmake)

if (USE_YAML_CPP)
    add_subdirectory (yaml-cpp-cmake)
endif()

add_subdirectory (re2-cmake)
add_subdirectory (xz-cmake)
add_subdirectory (brotli-cmake)
add_subdirectory (double-conversion-cmake)
add_subdirectory (boringssl-cmake)
add_subdirectory (poco-cmake)
add_subdirectory (croaring-cmake)
add_subdirectory (zstd-cmake)
add_subdirectory (zlib-ng-cmake)
add_subdirectory (bzip2-cmake)
add_subdirectory (snappy-cmake)
add_subdirectory (rocksdb-cmake)
add_subdirectory (thrift-cmake)
# parquet/arrow/orc
add_subdirectory (arrow-cmake) # requires: snappy, thrift, double-conversion
add_subdirectory (avro-cmake) # requires: snappy
add_subdirectory (hive-metastore-cmake) # requires: thrift/avro/arrow
add_subdirectory (librdkafka-cmake)
add_subdirectory (cppkafka-cmake)
add_subdirectory (openldap-cmake)
add_subdirectory (grpc-cmake)
add_subdirectory (protobuf-cmake)
add_subdirectory (libhdfs3-cmake) # requires: protobuf
add_subdirectory (msgpack-c-cmake)

if (ENABLE_FUZZING)
    add_subdirectory (libprotobuf-mutator-cmake)
endif()

add_subdirectory (cityhash102)
add_subdirectory (libfarmhash)
add_subdirectory (icu-cmake)
add_subdirectory (h3-cmake)

# TODO: refactor the contrib libraries below this comment.

if (USE_INTERNAL_MYSQL_LIBRARY)
    add_subdirectory (mariadb-connector-c-cmake)
endif ()

if (ENABLE_TESTS)
    add_subdirectory (googletest-cmake)
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
    add_subdirectory(libgsasl-cmake)
endif()

if (USE_INTERNAL_LIBXML2_LIBRARY)
    add_subdirectory(libxml2-cmake)
endif ()

if (USE_INTERNAL_AWS_S3_LIBRARY)
    add_subdirectory(aws-s3-cmake)

    # The library is large - avoid bloat.
    target_compile_options (aws_s3 PRIVATE -g0)
    target_compile_options (aws_s3_checksums PRIVATE -g0)

endif ()

add_subdirectory (base64-cmake)
add_subdirectory (simdjson-cmake)
add_subdirectory (rapidjson-cmake)
add_subdirectory (fastops-cmake)
add_subdirectory (libuv-cmake)
add_subdirectory (amqpcpp-cmake) # requires: libuv
add_subdirectory (cassandra-cmake) # requires: libuv

# Should go before:
# - sentry-native
add_subdirectory (curl-cmake)

add_subdirectory (azure-cmake)

if (USE_SENTRY)
    add_subdirectory (sentry-native-cmake)
endif()

add_subdirectory (fmtlib-cmake)

if (USE_KRB5)
    add_subdirectory (krb5-cmake)
    if (USE_CYRUS_SASL)
        add_subdirectory (cyrus-sasl-cmake)
    endif()
endif()


if (USE_LIBPQXX)
    add_subdirectory (libpq-cmake)
    add_subdirectory (libpqxx-cmake)
endif()

add_subdirectory (nuraft-cmake)
add_subdirectory(fast_float-cmake)

if (USE_NLP)
    add_subdirectory(libstemmer-c-cmake)
    add_subdirectory(wordnet-blast-cmake)
    add_subdirectory(lemmagen-c-cmake)
endif()

add_subdirectory (sqlite-cmake)
add_subdirectory (s2geometry-cmake)

# Put all targets defined here and in subdirectories under "contrib/<immediate-subdir>" folders in GUI-based IDEs.
# Some of third-party projects may override CMAKE_FOLDER or FOLDER property of their targets, so they would not appear
# in "contrib/..." as originally planned, so we workaround this by fixing FOLDER properties of all targets manually,
# instead of controlling it via CMAKE_FOLDER.

function (ensure_target_rooted_in _target _folder)
    # Skip aliases and INTERFACE library targets, since FOLDER property is not available/writable for them.
    get_target_property (_target_aliased "${_target}" ALIASED_TARGET)
    get_target_property (_target_type "${_target}" TYPE)
    if (_target_aliased OR _target_type STREQUAL "INTERFACE_LIBRARY")
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
