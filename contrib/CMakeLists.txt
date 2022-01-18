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

macro(add_contrib folder)
    if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${folder}/CMakeLists.txt")
        message(STATUS "submodule ${folder} is missing (no CMakeLists.txt). to fix try run:")
        message(STATUS "    git submodule update --init")
    else()
        message(STATUS "Adding contrib module ${folder}")
        add_subdirectory (${folder})
    endif()
endmacro()

add_contrib (miniselect-cmake)
add_contrib (pdqsort-cmake)
add_contrib (sparsehash-c11-cmake)
add_contrib (abseil-cpp-cmake)
add_contrib (magic-enum-cmake)
add_contrib (boost-cmake)
add_contrib (cctz-cmake)
add_contrib (consistent-hashing)
add_contrib (dragonbox-cmake)
add_contrib (hyperscan-cmake)
add_contrib (jemalloc-cmake)
add_contrib (libcpuid-cmake)
add_contrib (libdivide)
add_contrib (libmetrohash)
add_contrib (lz4-cmake)
add_contrib (murmurhash)
add_contrib (replxx-cmake)
add_contrib (unixodbc-cmake)
add_contrib (nanodbc-cmake)
add_contrib (capnproto-cmake)
add_contrib (yaml-cpp-cmake)
add_contrib (re2-cmake)
add_contrib (xz-cmake)
add_contrib (brotli-cmake)
add_contrib (double-conversion-cmake)
add_contrib (boringssl-cmake)
add_contrib (poco-cmake)
add_contrib (croaring-cmake)
add_contrib (zstd-cmake)
add_contrib (zlib-ng-cmake)
add_contrib (bzip2-cmake)
add_contrib (snappy-cmake)
add_contrib (rocksdb-cmake)
add_contrib (thrift-cmake)
# parquet/arrow/orc
add_contrib (arrow-cmake) # requires: snappy, thrift, double-conversion
add_contrib (avro-cmake) # requires: snappy
add_contrib (hive-metastore-cmake) # requires: thrift/avro/arrow
add_contrib (librdkafka-cmake)
add_contrib (cppkafka-cmake)
add_contrib (openldap-cmake)
add_contrib (grpc-cmake)
add_contrib (protobuf-cmake)
add_contrib (libhdfs3-cmake) # requires: protobuf
add_contrib (msgpack-c-cmake)

if (ENABLE_FUZZING)
    add_contrib (libprotobuf-mutator-cmake)
endif()

add_contrib (cityhash102)
add_contrib (libfarmhash)
add_contrib (icu-cmake)
add_contrib (h3-cmake)
add_contrib (mariadb-connector-c-cmake)

if (ENABLE_TESTS)
    add_contrib (googletest-cmake)
endif()

add_contrib (llvm-cmake)
add_contrib (libgsasl-cmake)
add_contrib (libxml2-cmake)
add_contrib (aws-s3-cmake)
add_contrib (base64-cmake)
add_contrib (simdjson-cmake)
add_contrib (rapidjson-cmake)
add_contrib (fastops-cmake)
add_contrib (libuv-cmake)
add_contrib (amqpcpp-cmake) # requires: libuv
add_contrib (cassandra-cmake) # requires: libuv
add_contrib (curl-cmake)
add_contrib (azure-cmake)
add_contrib (sentry-native-cmake) # requires: curl
add_contrib (fmtlib-cmake)
add_contrib (krb5-cmake)
add_contrib (cyrus-sasl-cmake) # for krb5
add_contrib (libpqxx-cmake)
add_contrib (libpq-cmake)
add_contrib (nuraft-cmake)
add_contrib (fast_float-cmake)
add_contrib (datasketches-cpp-cmake)

option(ENABLE_NLP "Enable NLP functions support" ${ENABLE_LIBRARIES})
if (ENABLE_NLP)
    add_contrib (libstemmer-c-cmake) # ENABLE_NLP
    add_contrib (wordnet-blast-cmake) # ENABLE_NLP
    add_contrib (lemmagen-c-cmake) # ENABLE_NLP
endif()

add_contrib (sqlite-cmake)
add_contrib (s2geometry-cmake)

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
