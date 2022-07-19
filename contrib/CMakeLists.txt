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

# add_contrib cmake_folder[ base_folder1[, ...base_folderN]]
function(add_contrib cmake_folder)
    if (ARGN)
        set(base_folders ${ARGN})
    else()
        set(base_folders ${cmake_folder})
    endif()

    foreach (base_folder ${base_folders})
        # some typos in the code
        if (NOT IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${base_folder}")
            message(FATAL_ERROR "No such base folder '${base_folder}' (for '${cmake_folder}' cmake folder). Typo in the base folder name?")
        endif()

        file(GLOB contrib_files "${base_folder}/*")
        if (NOT contrib_files)
            # Checking out *all* submodules takes > 5 min. Therefore, the smoke build ("FastTest") in CI initializes only the set of
            # submodules minimally needed for a build and we cannot assume here that all submodules are populated.
            message(STATUS "submodule ${base_folder} is missing or empty. to fix try run:")
            message(STATUS "    git submodule update --init")
            return()
        endif()
    endforeach()

    message(STATUS "Adding contrib module ${base_folders} (configuring with ${cmake_folder})")
    add_subdirectory (${cmake_folder})
endfunction()

add_contrib (miniselect-cmake miniselect)
add_contrib (pdqsort-cmake pdqsort)
add_contrib (sparsehash-c11-cmake sparsehash-c11)
add_contrib (abseil-cpp-cmake abseil-cpp)
add_contrib (magic-enum-cmake magic_enum)
add_contrib (boost-cmake boost)
add_contrib (cctz-cmake cctz)
add_contrib (consistent-hashing)
add_contrib (dragonbox-cmake dragonbox)
add_contrib (vectorscan-cmake vectorscan)
add_contrib (jemalloc-cmake jemalloc)
add_contrib (libcpuid-cmake libcpuid)
add_contrib (libdivide)
add_contrib (libmetrohash)
add_contrib (lz4-cmake lz4)
add_contrib (murmurhash)
add_contrib (replxx-cmake replxx)
add_contrib (unixodbc-cmake unixodbc)
add_contrib (nanodbc-cmake nanodbc)
add_contrib (capnproto-cmake capnproto)
add_contrib (yaml-cpp-cmake yaml-cpp)
add_contrib (re2-cmake re2)
add_contrib (xz-cmake xz)
add_contrib (brotli-cmake brotli)
add_contrib (double-conversion-cmake double-conversion)
add_contrib (boringssl-cmake boringssl)
add_contrib (poco-cmake poco)
add_contrib (croaring-cmake croaring)
add_contrib (zstd-cmake zstd)
add_contrib (zlib-ng-cmake zlib-ng)
add_contrib (bzip2-cmake bzip2)
add_contrib (minizip-ng-cmake minizip-ng)
add_contrib (snappy-cmake snappy)
add_contrib (rocksdb-cmake rocksdb)
add_contrib (thrift-cmake thrift)
# parquet/arrow/orc
add_contrib (arrow-cmake arrow) # requires: snappy, thrift, double-conversion
add_contrib (avro-cmake avro) # requires: snappy
add_contrib (protobuf-cmake protobuf)
add_contrib (openldap-cmake openldap)
add_contrib (grpc-cmake grpc)
add_contrib (msgpack-c-cmake msgpack-c)

if (ENABLE_FUZZING)
    add_contrib (libprotobuf-mutator-cmake libprotobuf-mutator)
endif()

add_contrib (wyhash-cmake wyhash)
add_contrib (cityhash102)
add_contrib (libfarmhash)
add_contrib (icu-cmake icu)
add_contrib (h3-cmake h3)
add_contrib (mariadb-connector-c-cmake mariadb-connector-c)

if (ENABLE_TESTS)
    add_contrib (googletest-cmake googletest)
endif()

add_contrib (llvm-cmake llvm)
add_contrib (libxml2-cmake libxml2)
add_contrib (aws-s3-cmake
    aws
    aws-c-common
    aws-c-event-stream
    aws-checksums
)
add_contrib (base64-cmake base64)
add_contrib (simdjson-cmake simdjson)
add_contrib (rapidjson-cmake rapidjson)
add_contrib (fastops-cmake fastops)
add_contrib (libuv-cmake libuv)
add_contrib (amqpcpp-cmake AMQP-CPP) # requires: libuv
add_contrib (cassandra-cmake cassandra) # requires: libuv

if (ENABLE_CURL_BUILD)
    add_contrib (curl-cmake curl)
    add_contrib (azure-cmake azure)
    add_contrib (sentry-native-cmake sentry-native) # requires: curl
endif()

add_contrib (fmtlib-cmake fmtlib)
add_contrib (krb5-cmake krb5)
add_contrib (cyrus-sasl-cmake cyrus-sasl) # for krb5
add_contrib (libgsasl-cmake libgsasl) # requires krb5
add_contrib (librdkafka-cmake librdkafka) # requires: libgsasl
add_contrib (nats-io-cmake nats-io)
add_contrib (libhdfs3-cmake libhdfs3) # requires: protobuf, krb5
add_contrib (hive-metastore-cmake hive-metastore) # requires: thrift/avro/arrow/libhdfs3
add_contrib (cppkafka-cmake cppkafka)
add_contrib (libpqxx-cmake libpqxx)
add_contrib (libpq-cmake libpq)
add_contrib (nuraft-cmake NuRaft)
add_contrib (fast_float-cmake fast_float)
add_contrib (datasketches-cpp-cmake datasketches-cpp)
add_contrib (hashidsxx-cmake hashidsxx)

option(ENABLE_NLP "Enable NLP functions support" ${ENABLE_LIBRARIES})
if (ENABLE_NLP)
    add_contrib (libstemmer-c-cmake libstemmer_c)
    add_contrib (wordnet-blast-cmake wordnet-blast)
    add_contrib (lemmagen-c-cmake lemmagen-c)
    add_contrib (nlp-data-cmake nlp-data)
    add_contrib (cld2-cmake cld2)
endif()

add_contrib (sqlite-cmake sqlite-amalgamation)
add_contrib (s2geometry-cmake s2geometry)
add_contrib (base-x-cmake base-x)
add_contrib (qpl-cmake qpl)

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
