#
# Copyright 2009-2017 Alibaba Cloud All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Build glue for the ODPS C++ SDK, which lives in the git submodule at
# contrib/odps-sdk-cpp (unmodified aliyun-odps-sdk-cpp 0.50).
#
# Upstream builds with GCC 4.9.2 / 8 / 9 and fetches its own dependencies.
# ClickHouse builds with Clang against its own contrib copies of protobuf, curl,
# OpenSSL, Arrow, lz4, zstd, zlib and Boost, so the submodule's own CMake files
# are not used and this file replaces them.
#
# The option name is kept as ENABLE_ODPS_TUNNEL, and the produced target is kept
# as ch_contrib::odps, so that src/configure_config.cmake, src/Common/config.h.in
# and every `#if USE_ODPS_TUNNEL` keep working across the SDK change.

if (OS_LINUX)
        option(ENABLE_ODPS_TUNNEL "Enable ODPS TUNNEL" ${ENABLE_LIBRARIES})
else ()
        option(ENABLE_ODPS_TUNNEL "Enable ODPS TUNNEL" FALSE)
endif ()

if (NOT ENABLE_ODPS_TUNNEL)
        message (STATUS "Build without ODPS")
        return()
endif()

# The SDK needs protobuf for the Tunnel wire format and curl for HTTP. Neither
# has a fallback, so ODPS follows them instead of failing the whole configure.
if (NOT TARGET ch_contrib::protobuf)
        message (STATUS "Build without ODPS: protobuf is disabled")
        return()
endif()
if (NOT TARGET ch_contrib::curl)
        message (STATUS "Build without ODPS: curl is disabled")
        return()
endif()

# Real libcurl enters the binary only through this target (upstream dropped
# it in 26.8; the ODPS port restored it), while the Kafka HTTP shim
# rdhttp_poco.c defines four curl-named compat helpers for the same symbols.
# Having both linked at once is a duplicate-symbol error in programs/clickhouse,
# so tell the shim to skip them: rdkafka's OIDC code then binds to real
# libcurl (ABI-compatible for the four).
if (TARGET _rdkafka)
        target_compile_definitions(_rdkafka PRIVATE RDHTTP_POCO_HAVE_REAL_CURL)
        # `_rdkafka` still calls the four curl-compatible functions directly.
        # Keep standalone consumers of the static target link-complete after
        # the Poco shim stops defining those symbols.
        target_link_libraries(_rdkafka PRIVATE ch_contrib::curl)
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/odps-sdk-cpp")
set(COMPAT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/compat")

project(odps-sdk-cpp)

# 'typeof' (a GNU extension used across common/ and tunnel/) requires a gnu++
# dialect, and CMAKE_CXX_STANDARD set at the project root must be unset so that
# the -std flag below is the only one that applies. C++20, not 17: the Arrow we
# link enforces C++20 (arrow-cmake FATAL_ERRORs below 20), and since 0.50 the
# SDK's tunnel/arrow_*.cpp include Arrow headers whose main paths use C++20
# facilities (std::popcount from <bit>), so a gnu++17 TU fails to compile
# against the pinned Arrow.
unset(CMAKE_CXX_STANDARD)

list(APPEND ODPS_COMPILER_FLAGS "-std=gnu++20")
# Legacy feature macros the SDK historically compiled with. They are no longer
# referenced by the compiled sources in 0.50 (the apsara tree that needed
# APSARA_TARGET_64 is gone), but are kept as harmless no-ops.
list(APPEND ODPS_COMPILER_FLAGS "-DAPSARA_TARGET_64" "-D__STDC_LIMIT_MACROS" "-D__USE_XOPEN2K8")
# Not TUNNEL_USE_CRC_32C: that would switch tunnel/crc_32c.h onto util/crc32c.cpp's
# hardware CRC-32C (x86 `cpuid` asm + SSE4.2 intrinsics), which does not build on
# aarch64. Without it the SDK uses boost::crc_optimal<32, 0x1edc6f41, ...>, the same
# CRC-32C as before, so the wire format is unchanged. See the exclusion of
# util/crc32c.cpp below.
#
list(APPEND ODPS_COMPILER_FLAGS
        "-fPIC"
        "-fno-strict-aliasing")

# The SDK can expose a columnar Tunnel reader/writer built on Arrow.
# StorageMaxCompute consumes it through the `maxcompute_read_format` setting
# (guarded by the USE_ODPS_ARROW config macro), so the option defaults to ON
# whenever the Tunnel itself and Arrow are built. Since 0.50 every
# tunnel/arrow_*.cpp is internally guarded by `#ifdef ODPS_SDK_ENABLE_ARROW`, so
# the source-level filter below is a belt-and-suspenders step that also keeps the
# empty translation units out of the build when Arrow is off.
#
# Two things require the option to stay overridable:
#
#  - In odps_tunnel.h, ODPS_SDK_ENABLE_ARROW guards virtual members in the middle
#    of IDownload and IStreamUpload. A consumer compiled without the macro would
#    compute different vtable slots than the library was built with, and
#    `IDownload::Complete` would dispatch into `GetArrowSchema`. So the macro has
#    to be PUBLIC, which in turn puts <arrow/api.h> on the include path of every
#    consumer of ch_contrib::odps, which is why Arrow is then linked PUBLIC.
#  - The SDK's Arrow call sites carry ARROW_VERSION_MAJOR >= 13 adaptation
#    branches (tunnel/arrow_util.h), but a forced-off option remains the escape
#    hatch if an Arrow bump ever breaks them.
#
# Turn this on together with the code that uses the columnar path.
if (ENABLE_ODPS_TUNNEL AND TARGET ch_contrib::arrow AND NOT DEFINED ENABLE_ODPS_ARROW)
    set(ENABLE_ODPS_ARROW_INITIAL ON)
else()
    set(ENABLE_ODPS_ARROW_INITIAL FALSE)
endif()
option(ENABLE_ODPS_ARROW "Build the ODPS SDK with its Arrow-based Tunnel reader/writer" ${ENABLE_ODPS_ARROW_INITIAL})

if (ENABLE_ODPS_ARROW AND NOT TARGET ch_contrib::arrow)
        message (FATAL_ERROR "ENABLE_ODPS_ARROW requires Arrow, which is disabled")
endif()

# Upstream generates common/sdk_version.h with a Python script that reads git
# metadata. ClickHouse source archives intentionally contain no nested `.git`
# directories, so use a stable integration identifier here. The submodule
# gitlink is the single source of truth for the exact SDK revision.
set(ODPS_SDK_GIT_REVISION "clickhouse-submodule")
set(ODPS_SDK_GIT_BRANCH "0.50-clickhouse")
configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/sdk_version.h.in"
        "${CMAKE_CURRENT_BINARY_DIR}/sdk_version.h"
        @ONLY)

# Only an explicit list from common/, util/ and tunnel/ is compiled. core/
# (ODPS REST metadata API) and max_storage_api/ (MaxCompute Storage API) are
# intentionally absent. Updating the SDK therefore requires an audited source
# list change and cannot silently expand the linked code through a glob.
set(ODPS_COMMON_SRCS
        "${LIBRARY_DIR}/common/complex_type_schema.cpp"
        "${LIBRARY_DIR}/common/configuration.cpp"
        "${LIBRARY_DIR}/common/curl_connection_pool.cpp"
        "${LIBRARY_DIR}/common/curl_util.cpp"
        "${LIBRARY_DIR}/common/http_connection.cpp"
        "${LIBRARY_DIR}/common/http_message.cpp"
        "${LIBRARY_DIR}/common/odps_array.cpp"
        "${LIBRARY_DIR}/common/odps_exception.cpp"
        "${LIBRARY_DIR}/common/odps_map.cpp"
        "${LIBRARY_DIR}/common/odps_struct.cpp"
        "${LIBRARY_DIR}/common/odps_table_schema.cpp"
        "${LIBRARY_DIR}/common/odps_types.cpp"
        "${LIBRARY_DIR}/common/signer.cpp")

set(ODPS_UTIL_SRCS
        "${LIBRARY_DIR}/util/address.cpp"
        "${LIBRARY_DIR}/util/base64.cpp"
        "${LIBRARY_DIR}/util/gzip_util.cpp"
        "${LIBRARY_DIR}/util/hmac.cpp"
        "${LIBRARY_DIR}/util/md5.cpp"
        "${LIBRARY_DIR}/util/network.cpp"
        "${LIBRARY_DIR}/util/sha1.cpp"
        "${LIBRARY_DIR}/util/string_util.cpp"
        "${LIBRARY_DIR}/util/utils.cpp")

set(ODPS_TUNNEL_SRCS
        "${LIBRARY_DIR}/tunnel/connection_manager.cpp"
        "${LIBRARY_DIR}/tunnel/download.cpp"
        "${LIBRARY_DIR}/tunnel/lz4_stream.cpp"
        "${LIBRARY_DIR}/tunnel/odps_table_record.cpp"
        "${LIBRARY_DIR}/tunnel/odps_tunnel.cpp"
        "${LIBRARY_DIR}/tunnel/record_pack.cpp"
        "${LIBRARY_DIR}/tunnel/record_reader.cpp"
        "${LIBRARY_DIR}/tunnel/record_writer.cpp"
        "${LIBRARY_DIR}/tunnel/serialize.cpp"
        "${LIBRARY_DIR}/tunnel/stream_upload.cpp"
        "${LIBRARY_DIR}/tunnel/upload.cpp"
        "${LIBRARY_DIR}/tunnel/upsert.cpp"
        "${LIBRARY_DIR}/tunnel/upsert_record.cpp"
        "${LIBRARY_DIR}/tunnel/upsert_stream.cpp"
        "${LIBRARY_DIR}/tunnel/util.cpp"
        "${LIBRARY_DIR}/tunnel/volume_download.cpp"
        "${LIBRARY_DIR}/tunnel/volume_reader_writer.cpp"
        "${LIBRARY_DIR}/tunnel/volume_serialize.cpp"
        "${LIBRARY_DIR}/tunnel/volume_upload.cpp"
        "${LIBRARY_DIR}/tunnel/zstd_stream.cpp")

set(ODPS_TUNNEL_ARROW_SRCS
        "${LIBRARY_DIR}/tunnel/arrow_http_stream.cpp"
        "${LIBRARY_DIR}/tunnel/arrow_record_pack.cpp"
        "${LIBRARY_DIR}/tunnel/arrow_record_reader.cpp"
        "${LIBRARY_DIR}/tunnel/arrow_record_writer.cpp"
        "${LIBRARY_DIR}/tunnel/arrow_stream.cpp")

# util/crc32c.cpp is intentionally absent because it contains unconditional
# x86 code. Without TUNNEL_USE_CRC_32C, Tunnel uses the portable implementation.
if (ENABLE_ODPS_ARROW)
        list(APPEND ODPS_TUNNEL_SRCS ${ODPS_TUNNEL_ARROW_SRCS})
endif()

set(ODPS_UPSTREAM_SRCS
        ${ODPS_COMMON_SRCS}
        ${ODPS_UTIL_SRCS}
        ${ODPS_TUNNEL_SRCS})

add_library(_odps
        ${ODPS_UPSTREAM_SRCS}
        "${CMAKE_CURRENT_SOURCE_DIR}/odps_clickhouse_adapter.cpp")

target_compile_options(_odps PRIVATE ${ODPS_COMPILER_FLAGS})

# Keep compatibility workarounds owned by ClickHouse and scope every forced
# include to the exact SDK translation units that need it.
set_source_files_properties(
        "${LIBRARY_DIR}/tunnel/serialize.cpp"
        PROPERTIES COMPILE_OPTIONS "-include;${COMPAT_DIR}/odps_serialize_compat.h")
set_source_files_properties(
        "${LIBRARY_DIR}/util/gzip_util.cpp"
        PROPERTIES COMPILE_OPTIONS "-include;${COMPAT_DIR}/odps_zlib_compat.h")

if (ENABLE_ODPS_ARROW)
        set_source_files_properties(
                "${LIBRARY_DIR}/tunnel/download.cpp"
                "${LIBRARY_DIR}/tunnel/stream_upload.cpp"
                "${LIBRARY_DIR}/tunnel/upload.cpp"
                PROPERTIES COMPILE_OPTIONS "-include;${COMPAT_DIR}/odps_arrow_compat.h")
endif()

# The official SDK is not maintained by ClickHouse and is not clean under
# ClickHouse's Clang `-Weverything -Werror` policy. Suppress diagnostics only
# for upstream translation units. The ClickHouse-owned adapter remains under
# the normal project warning policy.
set_property(SOURCE ${ODPS_UPSTREAM_SRCS} APPEND PROPERTY COMPILE_OPTIONS "-w")

if (ENABLE_ODPS_ARROW)
        target_compile_definitions(_odps PUBLIC ODPS_SDK_ENABLE_ARROW)
endif()

target_include_directories(_odps PRIVATE
        # Shims for protobuf headers the SDK expects but modern protobuf no longer
        # ships. Ahead of protobuf's own directory, and holding only those files, so
        # everything else falls through. See compat/google/protobuf/.
        "${COMPAT_DIR}"
        # Cross-directory includes such as "common/http_connection.h",
        # "util/utils.h", "tunnel/record_reader.h", "include/configuration.h".
        "${LIBRARY_DIR}"
        # Bundled single-header deps, included as <nlohmann/json.hpp>.
        "${LIBRARY_DIR}/thirdparty"
        # Generated sdk_version.h, included as "sdk_version.h" from common/.
        "${CMAKE_CURRENT_BINARY_DIR}")

# Treat upstream public headers as system headers while compiling the
# ClickHouse-owned adapter, without suppressing diagnostics in the adapter.
target_include_directories(_odps SYSTEM PRIVATE "${LIBRARY_DIR}/include")

target_link_libraries(_odps PRIVATE
        ch_contrib::protobuf
        ch_contrib::curl
        ch_contrib::lz4
        ch_contrib::zstd
        ch_contrib::zlib
        boost::headers_only
        OpenSSL::Crypto
        OpenSSL::SSL)

if (ENABLE_ODPS_ARROW)
        # PUBLIC: ODPS_SDK_ENABLE_ARROW makes the SDK public headers include
        # <arrow/api.h>, so consumers need Arrow's include directories too.
        target_link_libraries(_odps PUBLIC ch_contrib::arrow)
endif()

# SYSTEM: the SDK public headers are not warning-clean under ClickHouse's flags,
# and dbms builds with -Werror. BEFORE, so that they win over any same-named
# header reachable from dbms include paths.
target_include_directories(_odps SYSTEM BEFORE INTERFACE "${LIBRARY_DIR}/include")
target_include_directories(_odps PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
add_library(ch_contrib::odps ALIAS _odps)
