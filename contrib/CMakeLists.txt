#"${folder}/CMakeLists.txt" Third-party libraries may have substandard code.

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w -ffunction-sections -fdata-sections")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w -ffunction-sections -fdata-sections")

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

# Use the original launchers for the projects that are needed for source code generation:
# protoc and llvm-tlbgen.
disable_dummy_launchers_if_needed()
# We need to add OpenSSL first so that all our projects use our own version instead of the system one.
add_contrib (openssl-cmake openssl)  # Needed for delta-lake
add_contrib (abseil-cpp-cmake abseil-cpp)
add_contrib (zlib-ng-cmake zlib-ng)
add_contrib (google-protobuf-cmake google-protobuf)
add_contrib (grpc-cmake grpc)
add_contrib (libllvmlibc-cmake llvm-project)
add_contrib (llvm-project-cmake llvm-project)
enable_dummy_launchers_if_needed()

add_contrib (miniselect-cmake miniselect)
add_contrib (pdqsort-cmake pdqsort)
add_contrib (pocketfft-cmake pocketfft)
add_contrib (crc32-vpmsum-cmake crc32-vpmsum)
add_contrib (sparsehash-c11-cmake sparsehash-c11)
add_contrib (magic-enum-cmake magic_enum)
add_contrib (boost-cmake boost)
add_contrib (cctz-cmake cctz)
add_contrib (consistent-hashing)
add_contrib (dragonbox-cmake dragonbox)
add_contrib (vectorscan-cmake vectorscan)
add_contrib (jemalloc-cmake jemalloc)
add_contrib (libcpuid-cmake libcpuid)
add_contrib (libdivide-cmake)
add_contrib (libmetrohash)
add_contrib (lz4-cmake lz4)
add_contrib (murmurhash)
add_contrib (replxx-cmake replxx)
add_contrib (capnproto-cmake capnproto)
add_contrib (yaml-cpp-cmake yaml-cpp)
add_contrib (re2-cmake re2)
add_contrib (xz-cmake xz)
add_contrib (brotli-cmake brotli)
add_contrib (double-conversion-cmake double-conversion)
add_contrib (croaring-cmake croaring)
add_contrib (zstd-cmake zstd)
add_contrib (bzip2-cmake bzip2)
add_contrib (minizip-ng-cmake minizip-ng)
add_contrib (snappy-cmake snappy)
add_contrib (thrift-cmake thrift)
# parquet/arrow/orc
add_contrib (arrow-cmake arrow) # requires: snappy, thrift, double-conversion
add_contrib (avro-cmake avro) # requires: snappy
add_contrib (openldap-cmake openldap)
add_contrib (msgpack-c-cmake msgpack-c)
add_contrib (libarchive-cmake libarchive)

add_contrib (corrosion-cmake corrosion)
add_contrib (delta-kernel-rs-cmake delta-kernel-rs)
add_contrib (chdig-cmake chdig)

if (ENABLE_FUZZING)
    add_contrib (libprotobuf-mutator-cmake libprotobuf-mutator)
endif()

add_contrib (wyhash-cmake wyhash)
add_contrib (cityhash102)
add_contrib (libfarmhash)
add_contrib (icu-cmake icu)
add_contrib (h3-cmake h3)
add_contrib (mariadb-connector-c-cmake mariadb-connector-c)
add_contrib (libfiu-cmake libfiu)

if (ENABLE_TESTS)
    add_contrib (googletest-cmake googletest)
endif()

add_contrib (libfuzzer-cmake llvm-project)
add_contrib (gwpasan-cmake llvm-project)
add_contrib (libxml2-cmake libxml2)

add_contrib (aws-cmake
    aws
    aws-c-auth
    aws-c-cal
    aws-c-common
    aws-c-compression
    aws-c-event-stream
    aws-c-http
    aws-c-io
    aws-c-mqtt
    aws-c-s3
    aws-c-sdkutils
    aws-checksums
    aws-crt-cpp
    aws-cmake
)

add_contrib (aklomp-base64-cmake aklomp-base64)
add_contrib (simdjson-cmake simdjson)
add_contrib (rapidjson-cmake rapidjson)
add_contrib (fastops-cmake fastops)
add_contrib (libuv-cmake libuv)
add_contrib (liburing-cmake liburing)
add_contrib (amqpcpp-cmake AMQP-CPP) # requires: libuv
add_contrib (cassandra-cmake cassandra) # requires: libuv
add_contrib (curl-cmake curl)
add_contrib (azure-cmake azure) # requires: curl
add_contrib (fmtlib-cmake fmtlib)
add_contrib (krb5-cmake krb5)
add_contrib (cyrus-sasl-cmake cyrus-sasl) # for krb5
add_contrib (libgsasl-cmake libgsasl) # requires krb5
add_contrib (librdkafka-cmake librdkafka) # requires: libgsasl
add_contrib (nats-io-cmake nats-io)
add_contrib (isa-l-cmake isa-l)
add_contrib (libhdfs3-cmake libhdfs3) # requires: google-protobuf, krb5, isa-l
add_contrib (hive-metastore-cmake hive-metastore) # requires: thrift, avro, arrow, libhdfs3
add_contrib (cppkafka-cmake cppkafka)

option(ENABLE_LIBPQXX "Enable PostgreSQL" ${ENABLE_LIBRARIES})
if (ENABLE_LIBPQXX)
    add_contrib (postgres-cmake postgres)
    add_contrib (libpqxx-cmake libpqxx)
endif()

add_contrib (rocksdb-cmake rocksdb) # requires: jemalloc, snappy, zlib, lz4, zstd, liburing
add_contrib (nuraft-cmake NuRaft)
add_contrib (fast_float-cmake fast_float)
add_contrib (idna-cmake idna)
add_contrib (datasketches-cpp-cmake datasketches-cpp)
add_contrib (incbin-cmake incbin)
add_contrib (sqids-cpp-cmake sqids-cpp)

option(USE_MONGODB "Enable MongoDB support" ${ENABLE_LIBRARIES})
if (USE_MONGODB)
    add_contrib (mongo-c-driver-cmake mongo-c-driver) # requires: zlib
    add_contrib (mongo-cxx-driver-cmake mongo-cxx-driver) # requires: libmongoc, libbson
endif()

option(ENABLE_NLP "Enable NLP functions support" ${ENABLE_LIBRARIES})
if (ENABLE_NLP)
    add_contrib (libstemmer-c-cmake libstemmer_c)
    add_contrib (wordnet-blast-cmake wordnet-blast)
    add_contrib (lemmagen-c-cmake lemmagen-c)
    add_contrib (cld2-cmake cld2)
endif()

add_contrib (sqlite-cmake sqlite-amalgamation)
add_contrib (s2geometry-cmake s2geometry)
add_contrib (c-ares-cmake c-ares)

if (OS_LINUX AND ARCH_AMD64 AND ENABLE_SSE42)
    option (ENABLE_QPL "Enable Intel® Query Processing Library (QPL)" ${ENABLE_LIBRARIES})
elseif(ENABLE_QPL)
    message (${RECONFIGURE_MESSAGE_LEVEL} "QPL library is only supported on x86_64 with SSE 4.2 or higher")
endif()
if (ENABLE_QPL)
    add_contrib (idxd-config-cmake idxd-config)
    add_contrib (qpl-cmake qpl) # requires: idxd-config
else()
    message(STATUS "Not using QPL")
endif ()

if (OS_LINUX AND ARCH_AMD64 AND NOT NO_SSE3_OR_HIGHER)
    option (ENABLE_QATLIB "Enable Intel® QuickAssist Technology Library (QATlib)" ${ENABLE_LIBRARIES})
elseif(ENABLE_QATLIB)
    message (${RECONFIGURE_MESSAGE_LEVEL} "QATLib is only supported on x86_64")
endif()
if (ENABLE_QATLIB)
    option (ENABLE_QAT_USDM_DRIVER "A User Space DMA-able Memory (USDM) component which allocates/frees DMA-able memory" OFF)
    option (ENABLE_QAT_OUT_OF_TREE_BUILD "Using out-of-tree driver, user needs to customize ICP_ROOT variable" OFF)
    set(ICP_ROOT "" CACHE STRING "ICP_ROOT variable to define the path of out-of-tree driver package")
    if (ENABLE_QAT_OUT_OF_TREE_BUILD)
        if (ICP_ROOT STREQUAL "")
            message(FATAL_ERROR "Please define the path of out-of-tree driver package with -DICP_ROOT=xxx or disable out-of-tree build with -DENABLE_QAT_OUT_OF_TREE_BUILD=OFF; \
                                 If you want out-of-tree build but have no package available, please download and build ICP package from: https://www.intel.com/content/www/us/en/download/765501.html")
        endif ()
    else()
        add_contrib (qatlib-cmake qatlib) # requires: isa-l
    endif ()
    add_contrib (QAT-ZSTD-Plugin-cmake QAT-ZSTD-Plugin)
else()
    message(STATUS "Not using QATLib")
endif ()

add_contrib (morton-nd-cmake morton-nd)
if (ARCH_S390X)
    add_contrib(crc32-s390x-cmake crc32-s390x)
endif()

option(ENABLE_USEARCH "Enable USearch" ${ENABLE_LIBRARIES})
if (ENABLE_USEARCH)
    add_contrib (FP16-cmake FP16)
    add_contrib (SimSIMD-cmake SimSIMD)
    add_contrib (usearch-cmake usearch) # requires: FP16, SimdSIMD
else ()
    message(STATUS "Not using USearch")
endif ()

add_contrib (xxHash-cmake xxHash)

add_contrib (libbcrypt-cmake libbcrypt)

add_contrib (google-benchmark-cmake google-benchmark)
add_contrib (ulid-c-cmake ulid-c)

add_contrib (libssh-cmake libssh)

add_contrib (prometheus-protobufs-cmake prometheus-protobufs prometheus-protobufs-gogo)

add_contrib (numactl-cmake numactl)

add_contrib (google-cloud-cpp-cmake google-cloud-cpp) # requires grpc, protobuf, absl

add_contrib (jwt-cpp-cmake jwt-cpp)

add_contrib (sha3iuf-cmake SHA3IUF)

add_contrib (bech32)

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
