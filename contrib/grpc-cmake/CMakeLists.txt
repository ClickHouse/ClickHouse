# GRPC global cmake file
# This currently builds C and C++ code.
# This file has been automatically generated from a template file.
# Please look at the templates directory instead.
# This file can be regenerated from the template by running
# ${GRPC_SOURCE_DIR}/tools/buildgen/generate_projects.sh
#
# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(GRPC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../grpc)

cmake_minimum_required(VERSION 3.5.1)

set(PACKAGE_NAME      "grpc")
set(PACKAGE_VERSION   "1.25.0")
set(gRPC_CORE_VERSION "8.0.0")
set(PACKAGE_STRING    "${PACKAGE_NAME} ${PACKAGE_VERSION}")
set(PACKAGE_TARNAME   "${PACKAGE_NAME}-${PACKAGE_VERSION}")
set(PACKAGE_BUGREPORT "https://github.com/grpc/grpc/issues/")
project(${PACKAGE_NAME} LANGUAGES C CXX)

set(gRPC_INSTALL_BINDIR "${GRPC_SOURCE_DIR}/bin" CACHE STRING "Installation directory for executables")
set(gRPC_INSTALL_LIBDIR "${GRPC_SOURCE_DIR}/ib" CACHE STRING "Installation directory for libraries")
set(gRPC_INSTALL_INCLUDEDIR "${GRPC_SOURCE_DIR}/include" CACHE STRING "Installation directory for headers")
set(gRPC_INSTALL_CMAKEDIR "${GRPC_SOURCE_DIR}/lib/cmake/${PACKAGE_NAME}" CACHE STRING "Installation directory for cmake config files")
set(gRPC_INSTALL_SHAREDIR "${GRPC_SOURCE_DIR}/share/grpc" CACHE STRING "Installation directory for root certificates")

# Options
option(gRPC_BUILD_TESTS "Build tests" OFF)
option(gRPC_BUILD_CODEGEN "Build codegen" ON)
option(gRPC_BACKWARDS_COMPATIBILITY_MODE "Build libraries that are binary compatible across a larger number of OS and libc versions" OFF)

set(gRPC_INSTALL_default ON)
if (NOT CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  # Disable gRPC_INSTALL by default if building as a submodule
  set(gRPC_INSTALL_default OFF)
endif()
set(gRPC_INSTALL ${gRPC_INSTALL_default} CACHE BOOL
    "Generate installation target: gRPC_ZLIB_PROVIDER, gRPC_CARES_PROVIDER, gRPC_SSL_PROVIDER and gRPC_PROTOBUF_PROVIDER must all be \"package\"")

# Providers for third-party dependencies (gRPC_*_PROVIDER properties):
# "module": build the dependency using sources from git submodule (under third_party)
# "package": use cmake's find_package functionality to locate a pre-installed dependency

set(gRPC_ZLIB_PROVIDER "module" CACHE STRING "Provider of zlib library")
set_property(CACHE gRPC_ZLIB_PROVIDER PROPERTY STRINGS "module" "package")

set(gRPC_CARES_PROVIDER "module" CACHE STRING "Provider of c-ares library")
set_property(CACHE gRPC_CARES_PROVIDER PROPERTY STRINGS "module" "package")

set(gRPC_SSL_PROVIDER "module" CACHE STRING "Provider of ssl library")
set_property(CACHE gRPC_SSL_PROVIDER PROPERTY STRINGS "module" "package")

set(gRPC_PROTOBUF_PROVIDER "module" CACHE STRING "Provider of protobuf library")
set_property(CACHE gRPC_PROTOBUF_PROVIDER PROPERTY STRINGS "module" "package")

set(gRPC_PROTOBUF_PACKAGE_TYPE "" CACHE STRING "Algorithm for searching protobuf package")
set_property(CACHE gRPC_PROTOBUF_PACKAGE_TYPE PROPERTY STRINGS "CONFIG" "MODULE")

set(gRPC_GFLAGS_PROVIDER "module" CACHE STRING "Provider of gflags library")
set_property(CACHE gRPC_GFLAGS_PROVIDER PROPERTY STRINGS "module" "package")

set(gRPC_BENCHMARK_PROVIDER "module" CACHE STRING "Provider of benchmark library")
set_property(CACHE gRPC_BENCHMARK_PROVIDER PROPERTY STRINGS "module" "package")

set(gRPC_USE_PROTO_LITE OFF CACHE BOOL "Use the protobuf-lite library")

if(UNIX)
  if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(_gRPC_PLATFORM_LINUX ON)
  elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(_gRPC_PLATFORM_MAC ON)
  elseif(${CMAKE_SYSTEM_NAME} MATCHES "iOS")
    set(_gRPC_PLATFORM_IOS ON)
  elseif(${CMAKE_SYSTEM_NAME} MATCHES "Android")
    set(_gRPC_PLATFORM_ANDROID ON)
  else()
    set(_gRPC_PLATFORM_POSIX ON)
  endif()
endif()
if(WIN32)
  set(_gRPC_PLATFORM_WINDOWS ON)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

if (MSVC)
  include(cmake/msvc_static_runtime.cmake)
  add_definitions(-D_WIN32_WINNT=0x600 -D_SCL_SECURE_NO_WARNINGS -D_CRT_SECURE_NO_WARNINGS -D_WINSOCK_DEPRECATED_NO_WARNINGS)
  # needed to compile protobuf
  set(_gRPC_C_CXX_FLAGS "${_gRPC_C_CXX_FLAGS} /wd4065 /wd4506")
  # TODO(jtattermusch): revisit warnings that were silenced as part of upgrade to protobuf3.6.0
  set(_gRPC_C_CXX_FLAGS "${_gRPC_C_CXX_FLAGS} /wd4200 /wd4291 /wd4244")
  # TODO(jtattermusch): revisit C4267 occurrences throughout the code
  set(_gRPC_C_CXX_FLAGS "${_gRPC_C_CXX_FLAGS} /wd4267")
  # TODO(jtattermusch): needed to build boringssl with VS2017, revisit later
  set(_gRPC_C_CXX_FLAGS "${_gRPC_C_CXX_FLAGS} /wd4987 /wd4774 /wd4819 /wd4996 /wd4619")
endif()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${_gRPC_C_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${_gRPC_C_CXX_FLAGS}")

if (gRPC_USE_PROTO_LITE)
  set(_gRPC_PROTOBUF_LIBRARY_NAME "libprotobuf-lite")
  add_definitions("-DGRPC_USE_PROTO_LITE")
else()
  set(_gRPC_PROTOBUF_LIBRARY_NAME "libprotobuf")
endif()

if(gRPC_BACKWARDS_COMPATIBILITY_MODE)
  add_definitions(-DGPR_BACKWARDS_COMPATIBILITY_MODE)
  if (_gRPC_PLATFORM_MAC)
    # some C++11 constructs not supported before OS X 10.9
    set(CMAKE_OSX_DEPLOYMENT_TARGET 10.9)
  endif()
endif()

if (_gRPC_PLATFORM_LINUX OR _gRPC_PLATFORM_MAC OR _gRPC_PLATFORM_IOS)
  set(_gRPC_CORE_NOSTDCXX_FLAGS -fno-exceptions -fno-rtti)
else()
  set(_gRPC_CORE_NOSTDCXX_FLAGS "")
endif()

# address_sorting.cmake
include(${GRPC_SOURCE_DIR}/cmake/address_sorting.cmake)
set(_gRPC_ADDRESS_SORTING_INCLUDE_DIR "${GRPC_SOURCE_DIR}/third_party/address_sorting/include")
set(_gRPC_ADDRESS_SORTING_LIBRARIES address_sorting)

# benchmark.cmake
set(BENCHMARK_ROOT_DIR ${GRPC_SOURCE_DIR}/third_party/benchmark)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "Turn off gTest in gBenchmark")
add_subdirectory(${BENCHMARK_ROOT_DIR} ${BENCHMARK_ROOT_DIR})
if(TARGET benchmark)
  set(_gRPC_BENCHMARK_LIBRARIES benchmark)
endif()

# cares.cmake
set(CARES_ROOT_DIR ${GRPC_SOURCE_DIR}/third_party/cares/cares)
set(CARES_SHARED OFF CACHE BOOL "disable shared library")
set(CARES_STATIC ON CACHE BOOL "link cares statically")
if(gRPC_BACKWARDS_COMPATIBILITY_MODE)
  # See https://github.com/grpc/grpc/issues/17255
  set(HAVE_LIBNSL OFF CACHE BOOL "avoid cares dependency on libnsl")
endif()
add_subdirectory(${CARES_ROOT_DIR} ${CARES_ROOT_DIR})
if(TARGET c-ares)
  set(_gRPC_CARES_LIBRARIES c-ares)
endif()

# gflags.cmake
set(GFLAGS_ROOT_DIR ${GRPC_SOURCE_DIR}/third_party/gflags)
add_subdirectory(${GFLAGS_ROOT_DIR} ${GFLAGS_ROOT_DIR})
set(_gRPC_GFLAGS_LIBRARIES gflags::gflags)

# protobuf.cmake
set(PROTOBUF_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../protobuf)
if(NOT protobuf_BUILD_TESTS)
  set(protobuf_BUILD_TESTS OFF CACHE BOOL "Build protobuf tests")
endif()
if(NOT protobuf_WITH_ZLIB)
  set(protobuf_WITH_ZLIB OFF CACHE BOOL "Build protobuf with zlib.")
endif()
set(protobuf_MSVC_STATIC_RUNTIME OFF CACHE BOOL "Link static runtime libraries")
if(NOT USE_INTERNAL_PROTOBUF_LIBRARY)
  add_subdirectory(${PROTOBUF_ROOT_DIR}/cmake ${GRPC_SOURCE_DIR}/third_party/protobuf)
endif()
if(TARGET ${_gRPC_PROTOBUF_LIBRARY_NAME})
  set(_gRPC_PROTOBUF_LIBRARIES ${_gRPC_PROTOBUF_LIBRARY_NAME})
endif()
if(TARGET libprotoc)
  set(_gRPC_PROTOBUF_PROTOC_LIBRARIES libprotoc)
endif()
if(TARGET protoc)
  set(_gRPC_PROTOBUF_PROTOC protoc)
  set(_gRPC_PROTOBUF_PROTOC_EXECUTABLE $<TARGET_FILE:protoc>)
endif()
# For well-known .proto files distributed with protobuf
set(_gRPC_PROTOBUF_WELLKNOWN_INCLUDE_DIR "${PROTOBUF_ROOT_DIR}/src")
if(gRPC_INSTALL)
  message(WARNING "gRPC_INSTALL will be forced to FALSE because gRPC_PROTOBUF_PROVIDER is \"module\"")
  set(gRPC_INSTALL FALSE)
endif()

# ssl.cmake
set(BORINGSSL_CMAKE_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../boringssl-cmake)
set(BORINGSSL_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../boringssl)
add_subdirectory(${BORINGSSL_CMAKE_ROOT_DIR} ${BORINGSSL_ROOT_DIR})
if(TARGET ssl)
  set(_gRPC_SSL_LIBRARIES ssl)
  set(_gRPC_SSL_INCLUDE_DIR ${BORINGSSL_ROOT_DIR}/include)
endif()

# upb.cmake
set(UPB_ROOT_DIR ${GRPC_SOURCE_DIR}/third_party/upb)
set(_gRPC_UPB_INCLUDE_DIR "${UPB_ROOT_DIR}")
set(_gRPC_UPB_GRPC_GENERATED_DIR "${GRPC_SOURCE_DIR}/src/core/ext/upb-generated")
set(_gRPC_UPB_LIBRARIES upb)

# zlib.cmake
set(ZLIB_ROOT_DIR ${GRPC_SOURCE_DIR}/third_party/zlib)
include_directories("${ZLIB_ROOT_DIR}")
add_subdirectory(${ZLIB_ROOT_DIR} ${ZLIB_ROOT_DIR})
set(_gRPC_ZLIB_LIBRARIES zlibstatic)
set(_gRPC_ZLIB_INCLUDE_DIR "${ZLIB_ROOT_DIR}")

if(NOT MSVC)
  set(CMAKE_C_FLAGS   "${CMAKE_C_FLAGS} -std=c99")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif()

if(_gRPC_PLATFORM_MAC OR _gRPC_PLATFORM_IOS)
  set(_gRPC_ALLTARGETS_LIBRARIES ${CMAKE_DL_LIBS} m pthread)
elseif(_gRPC_PLATFORM_ANDROID)
  set(_gRPC_ALLTARGETS_LIBRARIES ${CMAKE_DL_LIBS} m)
elseif(UNIX)
  set(_gRPC_ALLTARGETS_LIBRARIES ${CMAKE_DL_LIBS} rt m pthread)
endif()

if(WIN32 AND MSVC)
  set(_gRPC_BASELIB_LIBRARIES wsock32 ws2_32)
endif()

# Create directory for generated .proto files
set(_gRPC_PROTO_GENS_DIR ${CMAKE_BINARY_DIR}/gens)
file(MAKE_DIRECTORY ${_gRPC_PROTO_GENS_DIR})

#  protobuf_generate_grpc_cpp
#  --------------------------
#
#   Add custom commands to process ``.proto`` files to C++ using protoc and
#   GRPC plugin::
#
#     protobuf_generate_grpc_cpp [<ARGN>...]
#
#   ``ARGN``
#     ``.proto`` files
#
function(protobuf_generate_grpc_cpp)
  if(NOT ARGN)
    message(SEND_ERROR "Error: PROTOBUF_GENERATE_GRPC_CPP() called without any proto files")
    return()
  endif()

  set(_protobuf_include_path -I . -I ${_gRPC_PROTOBUF_WELLKNOWN_INCLUDE_DIR})
  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)
    file(RELATIVE_PATH REL_FIL ${CMAKE_CURRENT_SOURCE_DIR} ${ABS_FIL})
    get_filename_component(REL_DIR ${REL_FIL} DIRECTORY)
    set(RELFIL_WE "${REL_DIR}/${FIL_WE}")

    #if cross-compiling, find host plugin
    if(CMAKE_CROSSCOMPILING)
        find_program(_gRPC_CPP_PLUGIN grpc_cpp_plugin)
    else()
        set(_gRPC_CPP_PLUGIN $<TARGET_FILE:grpc_cpp_plugin>)
    endif()

    add_custom_command(
      OUTPUT "${_gRPC_PROTO_GENS_DIR}/${RELFIL_WE}.grpc.pb.cc"
             "${_gRPC_PROTO_GENS_DIR}/${RELFIL_WE}.grpc.pb.h"
             "${_gRPC_PROTO_GENS_DIR}/${RELFIL_WE}_mock.grpc.pb.h"
             "${_gRPC_PROTO_GENS_DIR}/${RELFIL_WE}.pb.cc"
             "${_gRPC_PROTO_GENS_DIR}/${RELFIL_WE}.pb.h"
      COMMAND ${_gRPC_PROTOBUF_PROTOC_EXECUTABLE}
      ARGS --grpc_out=generate_mock_code=true:${_gRPC_PROTO_GENS_DIR}
           --cpp_out=${_gRPC_PROTO_GENS_DIR}
           --plugin=protoc-gen-grpc=${_gRPC_CPP_PLUGIN}
           ${_protobuf_include_path}
           ${REL_FIL}
      DEPENDS ${ABS_FIL} ${_gRPC_PROTOBUF_PROTOC} grpc_cpp_plugin
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Running gRPC C++ protocol buffer compiler on ${FIL}"
      VERBATIM)

      set_source_files_properties("${_gRPC_PROTO_GENS_DIR}/${RELFIL_WE}.grpc.pb.cc" "${_gRPC_PROTO_GENS_DIR}/${RELFIL_WE}.grpc.pb.h"  "${_gRPC_PROTO_GENS_DIR}/${RELFIL_WE}_mock.grpc.pb.h" "${_gRPC_PROTO_GENS_DIR}/${RELFIL_WE}.pb.cc" "${_gRPC_PROTO_GENS_DIR}/${RELFIL_WE}.pb.h" PROPERTIES GENERATED TRUE)
  endforeach()
endfunction()

add_custom_target(plugins
  DEPENDS
  grpc_cpp_plugin
)

add_custom_target(tools_c
  DEPENDS
  check_epollexclusive
  grpc_create_jwt
  grpc_print_google_default_creds_token
  grpc_verify_jwt
)

add_custom_target(tools_cxx
  DEPENDS
  gen_hpack_tables
  gen_legal_metadata_characters
  gen_percent_encoding_tables
)

add_custom_target(tools
  DEPENDS tools_c tools_cxx)


add_library(address_sorting
  ${GRPC_SOURCE_DIR}/third_party/address_sorting/address_sorting.c
  ${GRPC_SOURCE_DIR}/third_party/address_sorting/address_sorting_posix.c
  ${GRPC_SOURCE_DIR}/third_party/address_sorting/address_sorting_windows.c
)



target_include_directories(address_sorting
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)
target_link_libraries(address_sorting
  ${_gRPC_BASELIB_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
)



if (gRPC_INSTALL)
  install(TARGETS address_sorting EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()


add_library(gpr
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/alloc.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/atm.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/cpu_iphone.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/cpu_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/cpu_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/cpu_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/env_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/env_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/env_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/log.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/log_android.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/log_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/log_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/log_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/murmur_hash.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/string_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/string_util_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/string_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/sync.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/sync_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/sync_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/time.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/time_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/time_precise.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/time_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/tls_pthread.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/tmpfile_msys.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/tmpfile_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/tmpfile_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/wrap_memcpy.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/arena.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/fork.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/global_config_env.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/host_port.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/mpscq.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/thd_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/thd_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/profiling/basic_timers.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/profiling/stap_timers.cc
)

target_include_directories(gpr
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)
target_link_libraries(gpr
  ${_gRPC_ALLTARGETS_LIBRARIES}
)
if (_gRPC_PLATFORM_ANDROID)
  target_link_libraries(gpr
    android
    log
  )
endif (_gRPC_PLATFORM_ANDROID)

foreach(_hdr
  ${GRPC_SOURCE_DIR}/include/grpc/support/alloc.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm_gcc_atomic.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm_gcc_sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/cpu.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/log.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/log_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/port_platform.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/string_util.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_custom.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_generic.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/thd_id.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/time.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_atomic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/fork.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/log.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/port_platform.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_custom.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_generic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_windows.h
)
  SET(_path ${_hdr})
  get_filename_component(_path ${_path} PATH)
  install(FILES ${_hdr}
    DESTINATION "${_path}"
  )
endforeach()


if (gRPC_INSTALL)
  install(TARGETS gpr EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()


add_library(grpc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/init.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/avl/avl.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/backoff/backoff.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_args.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_stack.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_stack_builder.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_trace.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channelz.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channelz_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/connected_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/handshaker_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/status_util.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression_args.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression_internal.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/message_compress.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression_gzip.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression_identity.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/stats.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/stats_data.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/format_request.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/httpcli.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/parser.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/buffer_list.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/call_combiner.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/cfstream_handle.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/combiner.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/error.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/error_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_epoll1_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_epollex_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_poll_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/exec_ctx.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor/mpmcqueue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor/threadpool.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/fork_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/fork_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_host_name_max.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_sysconf.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/grpc_if_nametoindex_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/grpc_if_nametoindex_unsupported.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/internal_errqueue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iocp_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_internal.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_posix_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/is_epollexclusive_available.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/load_file.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/lockfree_event.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/polling_entity.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resource_quota.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/sockaddr_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_factory_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_mutator.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_common_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_common.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_ifaddrs.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_noifaddrs.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/time_averaged_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_generic.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_heap.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_manager.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/udp_server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/unix_sockets_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/unix_sockets_posix_noop.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_eventfd.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_nospecial.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_pipe.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_reader.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_writer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/b64.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/percent_encoding.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_buffer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_intern.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_string_helpers.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/api_trace.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/byte_buffer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/byte_buffer_reader.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call_details.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call_log_batch.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_init.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_ping.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_stack_type.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/completion_queue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/completion_queue_factory.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/event_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/lame_client.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/metadata_array.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/validate_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/version.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/bdp_estimator.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/byte_stream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/connectivity_state.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/error_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/metadata_batch.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/pid_controller.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/static_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/status_conversion.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/status_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/timeout_encoding.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/transport.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/transport_op_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/uri/uri_parser.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/trace.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/secure/server_secure_chttp2.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/bin_decoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/bin_encoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/chttp2_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/chttp2_transport.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/context_list.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/flow_control.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_data.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_goaway.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_ping.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_rst_stream.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_settings.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_window_update.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_encoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_parser.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_table.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/http2_settings.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/huffsyms.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/incoming_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/parsing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/stream_lists.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/stream_map.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/varint.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/writing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/alpn/alpn.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/client/http_client_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/http_filters_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/message_compress/message_compress_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/server/http_server_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/httpcli_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/context/security_context.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/alts_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/composite/composite_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/credentials_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/fake/fake_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/google_default/credentials_generic.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/google_default/google_default_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/iam/iam_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/jwt/json_token.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/jwt/jwt_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/jwt/jwt_verifier.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/local/local_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/oauth2/oauth2_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/plugin/plugin_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/ssl/ssl_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/tls/grpc_tls_credentials_options.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/tls/spiffe_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/alts/alts_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/fake/fake_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/load_system_roots_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/load_system_roots_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/local/local_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/ssl/ssl_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/ssl_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/ssl_utils_config.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/tls/spiffe_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/client_auth_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/secure_endpoint.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/security_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/server_auth_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/target_authority_table.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/tsi_error.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/util/json_util.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/init_secure.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/crypt/aes_gcm.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/crypt/gsec.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_counter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_crypter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_frame_protector.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_record_protocol_crypter_common.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_seal_privacy_integrity_crypter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_unseal_privacy_integrity_crypter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/frame_handler.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_handshaker_client.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_shared_resource.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_tsi_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_grpc_integrity_only_record_protocol.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_grpc_privacy_integrity_record_protocol.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_grpc_record_protocol_common.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_iovec_record_protocol.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_zero_copy_grpc_protector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment_no_op.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/grpc_alts_credentials_client_options.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/grpc_alts_credentials_options.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/grpc_alts_credentials_server_options.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_tsi_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/transport_security_common_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/gcp/altscontext.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/gcp/handshaker.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/gcp/transport_security_common.upb.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/decode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/encode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/msg.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/port.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/table.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/upb.c
  ${GRPC_SOURCE_DIR}/src/core/tsi/transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/insecure/channel_create.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/insecure/channel_create_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/authority.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/chttp2_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/backend_metric.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/backup_poller.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/channel_connectivity.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_channelz.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_factory.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/connector.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/global_subchannel_pool.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/health/health_check_client.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/http_connect_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/http_proxy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/local_subchannel_pool.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/parse_address.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/proxy_mapper.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/proxy_mapper_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver_result_parsing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolving_lb_policy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/retry_throttle.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/server_address.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/service_config.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/subchannel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/subchannel_pool_interface.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/deadline/deadline_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/health/v1/health.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/udpa/data/orca/v1/orca_load_report.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/gogoproto/gogo.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/validate/validate.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/api/annotations.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/api/http.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/any.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/descriptor.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/duration.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/empty.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/struct.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/timestamp.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/wrappers.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/rpc/status.upb.c
  ${GRPC_SOURCE_DIR}/src/core/tsi/fake_transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/local_transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl/session_cache/ssl_session_boringssl.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl/session_cache/ssl_session_cache.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl/session_cache/ssl_session_openssl.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl_transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/transport_security_grpc.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/chttp2_server.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/secure/secure_channel_create.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/insecure/server_chttp2.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/insecure/server_chttp2_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/inproc/inproc_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/inproc/inproc_transport.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/client_load_reporting_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb_channel_secure.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb_client_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/load_balancer_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/lb/v1/load_balancer.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/fake/fake_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/xds/xds.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_bootstrap.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_channel_secure.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_client.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_client_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/auth/cert.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cds.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/circuit_breaker.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/filter.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/outlier_detection.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/discovery.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/eds.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/endpoint/endpoint.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/endpoint/load_report.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/service/discovery/v2/ads.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/service/load_stats/v2/lrs.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/address.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/base.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/config_source.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/grpc_service.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/health_check.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/http_uri.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/protocol.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/type/percent.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/type/range.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/pick_first/pick_first.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/round_robin/round_robin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/dns_resolver_ares.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_libuv.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_libuv.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/dns_resolver_selection.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/native/dns_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/sockaddr/sockaddr_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/xds/xds_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/census/grpc_context.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_idle/client_idle_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/max_age/max_age_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/message_size/message_size_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/client_authority_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/workarounds/workaround_cronet_compression_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/workarounds/workaround_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/plugin_registry/grpc_plugin_registry.cc
)

target_include_directories(grpc
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)
target_link_libraries(grpc
  ${_gRPC_BASELIB_LIBRARIES}
  ${_gRPC_SSL_LIBRARIES}
  ${_gRPC_ZLIB_LIBRARIES}
  ${_gRPC_CARES_LIBRARIES}
  ${_gRPC_ADDRESS_SORTING_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  gpr
)
if (_gRPC_PLATFORM_IOS OR _gRPC_PLATFORM_MAC)
  target_link_libraries(grpc "-framework CoreFoundation")
endif()

foreach(_hdr
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/byte_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/compression_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/connectivity_state.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/grpc_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/propagation_bits.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/status.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_atomic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/fork.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/log.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/port_platform.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_custom.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_generic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_security.h
  ${GRPC_SOURCE_DIR}/include/grpc/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/byte_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpc/compression.h
  ${GRPC_SOURCE_DIR}/include/grpc/fork.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_security_constants.h
  ${GRPC_SOURCE_DIR}/include/grpc/load_reporting.h
  ${GRPC_SOURCE_DIR}/include/grpc/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/slice_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/status.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/workaround_list.h
  ${GRPC_SOURCE_DIR}/include/grpc/census.h
)
  SET(_path ${_hdr})
  get_filename_component(_path ${_path} PATH)
  install(FILES ${_hdr}
    DESTINATION "${_path}"
  )
endforeach()




if (gRPC_INSTALL)
  install(TARGETS grpc EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()


add_library(grpc_cronet
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/cronet/plugin_registry/grpc_cronet_plugin_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/init.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/avl/avl.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/backoff/backoff.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_args.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_stack.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_stack_builder.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_trace.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channelz.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channelz_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/connected_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/handshaker_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/status_util.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression_args.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression_internal.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/message_compress.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression_gzip.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression_identity.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/stats.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/stats_data.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/format_request.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/httpcli.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/parser.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/buffer_list.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/call_combiner.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/cfstream_handle.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/combiner.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/error.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/error_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_epoll1_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_epollex_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_poll_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/exec_ctx.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor/mpmcqueue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor/threadpool.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/fork_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/fork_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_host_name_max.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_sysconf.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/grpc_if_nametoindex_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/grpc_if_nametoindex_unsupported.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/internal_errqueue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iocp_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_internal.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_posix_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/is_epollexclusive_available.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/load_file.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/lockfree_event.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/polling_entity.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resource_quota.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/sockaddr_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_factory_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_mutator.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_common_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_common.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_ifaddrs.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_noifaddrs.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/time_averaged_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_generic.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_heap.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_manager.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/udp_server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/unix_sockets_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/unix_sockets_posix_noop.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_eventfd.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_nospecial.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_pipe.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_reader.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_writer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/b64.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/percent_encoding.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_buffer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_intern.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_string_helpers.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/api_trace.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/byte_buffer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/byte_buffer_reader.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call_details.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call_log_batch.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_init.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_ping.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_stack_type.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/completion_queue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/completion_queue_factory.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/event_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/lame_client.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/metadata_array.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/validate_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/version.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/bdp_estimator.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/byte_stream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/connectivity_state.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/error_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/metadata_batch.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/pid_controller.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/static_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/status_conversion.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/status_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/timeout_encoding.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/transport.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/transport_op_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/uri/uri_parser.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/trace.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/cronet/client/secure/cronet_channel_create.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/cronet/transport/cronet_api_dummy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/cronet/transport/cronet_transport.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/secure/secure_channel_create.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/bin_decoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/bin_encoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/chttp2_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/chttp2_transport.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/context_list.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/flow_control.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_data.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_goaway.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_ping.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_rst_stream.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_settings.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_window_update.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_encoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_parser.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_table.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/http2_settings.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/huffsyms.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/incoming_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/parsing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/stream_lists.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/stream_map.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/varint.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/writing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/alpn/alpn.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/client/http_client_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/http_filters_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/message_compress/message_compress_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/server/http_server_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/backend_metric.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/backup_poller.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/channel_connectivity.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_channelz.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_factory.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/connector.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/global_subchannel_pool.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/health/health_check_client.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/http_connect_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/http_proxy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/local_subchannel_pool.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/parse_address.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/proxy_mapper.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/proxy_mapper_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver_result_parsing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolving_lb_policy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/retry_throttle.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/server_address.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/service_config.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/subchannel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/subchannel_pool_interface.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/deadline/deadline_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/health/v1/health.upb.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/decode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/encode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/msg.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/port.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/table.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/udpa/data/orca/v1/orca_load_report.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/gogoproto/gogo.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/validate/validate.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/api/annotations.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/api/http.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/any.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/descriptor.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/duration.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/empty.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/struct.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/timestamp.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/wrappers.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/rpc/status.upb.c
  ${GRPC_SOURCE_DIR}/src/core/lib/http/httpcli_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/context/security_context.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/alts_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/composite/composite_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/credentials_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/fake/fake_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/google_default/credentials_generic.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/google_default/google_default_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/iam/iam_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/jwt/json_token.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/jwt/jwt_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/jwt/jwt_verifier.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/local/local_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/oauth2/oauth2_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/plugin/plugin_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/ssl/ssl_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/tls/grpc_tls_credentials_options.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/tls/spiffe_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/alts/alts_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/fake/fake_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/load_system_roots_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/load_system_roots_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/local/local_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/ssl/ssl_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/ssl_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/ssl_utils_config.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/tls/spiffe_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/client_auth_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/secure_endpoint.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/security_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/server_auth_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/target_authority_table.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/tsi_error.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/util/json_util.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/init_secure.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/crypt/aes_gcm.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/crypt/gsec.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_counter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_crypter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_frame_protector.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_record_protocol_crypter_common.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_seal_privacy_integrity_crypter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_unseal_privacy_integrity_crypter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/frame_handler.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_handshaker_client.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_shared_resource.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_tsi_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_grpc_integrity_only_record_protocol.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_grpc_privacy_integrity_record_protocol.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_grpc_record_protocol_common.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_iovec_record_protocol.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_zero_copy_grpc_protector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment_no_op.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/grpc_alts_credentials_client_options.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/grpc_alts_credentials_options.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/grpc_alts_credentials_server_options.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_tsi_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/transport_security_common_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/gcp/altscontext.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/gcp/handshaker.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/gcp/transport_security_common.upb.c
  ${GRPC_SOURCE_DIR}/src/core/tsi/transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/insecure/channel_create.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/insecure/channel_create_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/authority.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/chttp2_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/fake_transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/local_transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl/session_cache/ssl_session_boringssl.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl/session_cache/ssl_session_cache.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl/session_cache/ssl_session_openssl.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl_transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/transport_security_grpc.cc
)


target_include_directories(grpc_cronet
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)
target_link_libraries(grpc_cronet
  ${_gRPC_BASELIB_LIBRARIES}
  ${_gRPC_SSL_LIBRARIES}
  ${_gRPC_ZLIB_LIBRARIES}
  ${_gRPC_CARES_LIBRARIES}
  ${_gRPC_ADDRESS_SORTING_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  gpr
)
if (_gRPC_PLATFORM_IOS OR _gRPC_PLATFORM_MAC)
  target_link_libraries(grpc_cronet "-framework CoreFoundation")
endif()

foreach(_hdr
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/byte_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/compression_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/connectivity_state.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/grpc_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/propagation_bits.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/status.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_atomic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/fork.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/log.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/port_platform.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_custom.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_generic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_cronet.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_security.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_security_constants.h
)
  SET(_path ${_hdr})
  get_filename_component(_path ${_path} PATH)
  install(FILES ${_hdr}
    DESTINATION "${_path}"
  )
endforeach()


if (gRPC_INSTALL)
  install(TARGETS grpc_cronet EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()

add_library(grpc_unsecure
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/init.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/init_unsecure.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/avl/avl.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/backoff/backoff.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_args.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_stack.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_stack_builder.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_trace.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channelz.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channelz_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/connected_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/handshaker_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/status_util.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression_args.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression_internal.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/message_compress.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression_gzip.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression_identity.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/stats.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/stats_data.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/format_request.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/httpcli.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/parser.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/buffer_list.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/call_combiner.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/cfstream_handle.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/combiner.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/error.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/error_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_epoll1_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_epollex_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_poll_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/exec_ctx.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor/mpmcqueue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor/threadpool.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/fork_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/fork_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_host_name_max.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_sysconf.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/grpc_if_nametoindex_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/grpc_if_nametoindex_unsupported.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/internal_errqueue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iocp_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_internal.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_posix_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/is_epollexclusive_available.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/load_file.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/lockfree_event.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/polling_entity.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resource_quota.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/sockaddr_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_factory_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_mutator.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_common_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_common.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_ifaddrs.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_noifaddrs.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/time_averaged_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_generic.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_heap.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_manager.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/udp_server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/unix_sockets_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/unix_sockets_posix_noop.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_eventfd.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_nospecial.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_pipe.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_reader.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_writer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/b64.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/percent_encoding.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_buffer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_intern.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_string_helpers.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/api_trace.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/byte_buffer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/byte_buffer_reader.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call_details.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call_log_batch.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_init.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_ping.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_stack_type.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/completion_queue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/completion_queue_factory.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/event_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/lame_client.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/metadata_array.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/validate_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/version.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/bdp_estimator.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/byte_stream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/connectivity_state.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/error_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/metadata_batch.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/pid_controller.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/static_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/status_conversion.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/status_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/timeout_encoding.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/transport.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/transport_op_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/uri/uri_parser.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/trace.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/insecure/server_chttp2.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/insecure/server_chttp2_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/bin_decoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/bin_encoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/chttp2_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/chttp2_transport.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/context_list.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/flow_control.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_data.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_goaway.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_ping.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_rst_stream.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_settings.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_window_update.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_encoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_parser.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_table.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/http2_settings.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/huffsyms.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/incoming_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/parsing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/stream_lists.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/stream_map.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/varint.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/writing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/alpn/alpn.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/client/http_client_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/http_filters_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/message_compress/message_compress_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/server/http_server_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/chttp2_server.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/insecure/channel_create.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/insecure/channel_create_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/authority.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/chttp2_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/backend_metric.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/backup_poller.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/channel_connectivity.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_channelz.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_factory.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/connector.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/global_subchannel_pool.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/health/health_check_client.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/http_connect_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/http_proxy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/local_subchannel_pool.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/parse_address.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/proxy_mapper.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/proxy_mapper_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver_result_parsing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolving_lb_policy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/retry_throttle.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/server_address.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/service_config.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/subchannel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/subchannel_pool_interface.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/deadline/deadline_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/health/v1/health.upb.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/decode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/encode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/msg.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/port.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/table.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/udpa/data/orca/v1/orca_load_report.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/gogoproto/gogo.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/validate/validate.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/api/annotations.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/api/http.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/any.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/descriptor.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/duration.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/empty.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/struct.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/timestamp.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/wrappers.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/rpc/status.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/inproc/inproc_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/inproc/inproc_transport.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/dns_resolver_ares.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_libuv.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_libuv.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/dns_resolver_selection.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/native/dns_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/sockaddr/sockaddr_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/fake/fake_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/xds/xds_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_bootstrap.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_client.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_client_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/auth/cert.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cds.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/circuit_breaker.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/filter.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/outlier_detection.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/discovery.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/eds.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/endpoint/endpoint.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/endpoint/load_report.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/service/discovery/v2/ads.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/service/load_stats/v2/lrs.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/address.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/base.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/config_source.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/grpc_service.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/health_check.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/http_uri.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/protocol.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/type/percent.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/type/range.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/client_load_reporting_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb_client_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/load_balancer_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/lb/v1/load_balancer.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/xds/xds.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/pick_first/pick_first.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/round_robin/round_robin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/census/grpc_context.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_idle/client_idle_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/max_age/max_age_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/message_size/message_size_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/client_authority_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/workarounds/workaround_cronet_compression_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/workarounds/workaround_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/plugin_registry/grpc_unsecure_plugin_registry.cc
)


target_include_directories(grpc_unsecure
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)
target_link_libraries(grpc_unsecure
  ${_gRPC_BASELIB_LIBRARIES}
  ${_gRPC_ZLIB_LIBRARIES}
  ${_gRPC_CARES_LIBRARIES}
  ${_gRPC_ADDRESS_SORTING_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  gpr
)
if (_gRPC_PLATFORM_IOS OR _gRPC_PLATFORM_MAC)
  target_link_libraries(grpc_unsecure "-framework CoreFoundation")
endif()

foreach(_hdr
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/byte_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/compression_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/connectivity_state.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/grpc_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/propagation_bits.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/status.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_atomic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/fork.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/log.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/port_platform.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_custom.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_generic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/byte_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpc/compression.h
  ${GRPC_SOURCE_DIR}/include/grpc/fork.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_security_constants.h
  ${GRPC_SOURCE_DIR}/include/grpc/load_reporting.h
  ${GRPC_SOURCE_DIR}/include/grpc/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/slice_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/status.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/workaround_list.h
  ${GRPC_SOURCE_DIR}/include/grpc/census.h
)
  SET(_path ${_hdr})
  get_filename_component(_path ${_path} PATH)
  install(FILES ${_hdr}
    DESTINATION "${_path}"
  )
endforeach()


if (gRPC_INSTALL)
  install(TARGETS grpc_unsecure EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()

add_library(grpc++
  ${GRPC_SOURCE_DIR}/src/cpp/client/insecure_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/secure_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/auth_property_iterator.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/secure_auth_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/secure_channel_arguments.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/secure_create_auth_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/tls_credentials_options.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/tls_credentials_options_util.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/insecure_server_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/secure_server_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/channel_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/client_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/client_interceptor.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel_internal.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel_posix.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/credentials_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/generic_stub.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/alarm.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/channel_arguments.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/channel_filter.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/completion_queue_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/core_codegen.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/resource_quota_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/rpc_method.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/validate_service_config.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/version_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/async_generic_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/channel_argument_option.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/create_default_thread_pool.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/dynamic_thread_pool.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/external_connection_acceptor_impl.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/default_health_check_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/health_check_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/health_check_service_server_builder_option.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_builder.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_posix.cc
  ${GRPC_SOURCE_DIR}/src/cpp/thread_manager/thread_manager.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/byte_buffer_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/status.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/string_ref.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/time_cc.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/health/v1/health.upb.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/decode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/encode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/msg.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/port.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/table.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/upb.c
  ${GRPC_SOURCE_DIR}/src/cpp/codegen/codegen_init.cc
)

target_include_directories(grpc++
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)
target_link_libraries(grpc++
  ${_gRPC_BASELIB_LIBRARIES}
  ${_gRPC_SSL_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc
  gpr
)

foreach(_hdr
  ${GRPC_SOURCE_DIR}/include/grpc++/alarm.h
  ${GRPC_SOURCE_DIR}/include/grpc++/channel.h
  ${GRPC_SOURCE_DIR}/include/grpc++/client_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/completion_queue.h
  ${GRPC_SOURCE_DIR}/include/grpc++/create_channel.h
  ${GRPC_SOURCE_DIR}/include/grpc++/create_channel_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc++/ext/health_check_service_server_builder_option.h
  ${GRPC_SOURCE_DIR}/include/grpc++/generic/async_generic_service.h
  ${GRPC_SOURCE_DIR}/include/grpc++/generic/generic_stub.h
  ${GRPC_SOURCE_DIR}/include/grpc++/grpc++.h
  ${GRPC_SOURCE_DIR}/include/grpc++/health_check_service_interface.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/channel_argument_option.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/client_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/core_codegen.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/grpc_library.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/method_handler_impl.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/rpc_method.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/rpc_service_method.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/serialization_traits.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/server_builder_option.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/server_builder_plugin.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/server_initializer.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/service_type.h
  ${GRPC_SOURCE_DIR}/include/grpc++/resource_quota.h
  ${GRPC_SOURCE_DIR}/include/grpc++/security/auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/security/auth_metadata_processor.h
  ${GRPC_SOURCE_DIR}/include/grpc++/security/credentials.h
  ${GRPC_SOURCE_DIR}/include/grpc++/security/server_credentials.h
  ${GRPC_SOURCE_DIR}/include/grpc++/server.h
  ${GRPC_SOURCE_DIR}/include/grpc++/server_builder.h
  ${GRPC_SOURCE_DIR}/include/grpc++/server_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/server_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/async_stream.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/async_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/channel_arguments.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/config.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/status.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/status_code_enum.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/string_ref.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/stub_options.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/sync_stream.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/time.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/alarm.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/alarm_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/channel.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/channel_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/client_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/completion_queue.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/completion_queue_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/create_channel.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/create_channel_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/create_channel_posix.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/create_channel_posix_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/ext/health_check_service_server_builder_option.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/generic/async_generic_service.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/generic/generic_stub.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/generic/generic_stub_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/grpcpp.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/health_check_service_interface.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/health_check_service_interface_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/channel_argument_option.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/client_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/core_codegen.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/grpc_library.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/method_handler_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/rpc_method.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/rpc_service_method.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/serialization_traits.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/server_builder_option.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/server_builder_option_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/server_builder_plugin.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/server_initializer.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/server_initializer_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/service_type.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/resource_quota.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/resource_quota_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/auth_metadata_processor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/auth_metadata_processor_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/credentials.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/credentials_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/server_credentials.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/server_credentials_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/tls_credentials_options.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_builder.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_builder_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_posix.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_posix_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/async_stream.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/async_stream_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/async_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/async_unary_call_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/channel_arguments.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/channel_arguments_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/client_callback.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/client_callback_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/client_interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/config.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/message_allocator.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/proto_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/proto_buffer_writer.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/server_callback.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/server_callback_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/server_interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/slice.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/status.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/status_code_enum.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/string_ref.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/stub_options.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/sync_stream.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/sync_stream_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/time.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/validate_service_config.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/alloc.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm_gcc_atomic.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm_gcc_sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/cpu.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/log.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/log_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/port_platform.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/string_util.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_custom.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_generic.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/thd_id.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/time.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_atomic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/fork.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/log.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/port_platform.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_custom.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_generic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/byte_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpc/compression.h
  ${GRPC_SOURCE_DIR}/include/grpc/fork.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_security_constants.h
  ${GRPC_SOURCE_DIR}/include/grpc/load_reporting.h
  ${GRPC_SOURCE_DIR}/include/grpc/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/slice_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/status.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/workaround_list.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/byte_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/compression_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/connectivity_state.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/grpc_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/propagation_bits.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/status.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/async_stream.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/async_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/call_hook.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/channel_interface.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/client_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/client_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/completion_queue.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/completion_queue_tag.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/config.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/core_codegen_interface.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/create_auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/grpc_library.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/metadata_map.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/method_handler_impl.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/rpc_method.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/rpc_service_method.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/security/auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/serialization_traits.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/server_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/server_interface.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/service_type.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/status.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/status_code_enum.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/string_ref.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/stub_options.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/sync_stream.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/time.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/async_generic_service.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/async_stream.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/async_stream_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/async_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/async_unary_call_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/call_hook.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/call_op_set.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/call_op_set_interface.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/callback_common.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/channel_interface.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_callback.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_callback_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_context_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/completion_queue.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/completion_queue_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/completion_queue_tag.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/config.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/core_codegen_interface.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/create_auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/delegating_channel.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/grpc_library.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/intercepted_channel.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/interceptor_common.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/message_allocator.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/metadata_map.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/method_handler.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/method_handler_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/rpc_method.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/rpc_service_method.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/security/auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/serialization_traits.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_callback.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_callback_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_context_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_interface.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/service_type.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/slice.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/status.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/status_code_enum.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/string_ref.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/stub_options.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/sync_stream.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/sync_stream_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/time.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/sync.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/proto_utils.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/proto_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/proto_buffer_writer.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/proto_utils.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/config_protobuf.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/config_protobuf.h
)
  SET(_path ${_hdr})
  get_filename_component(_path ${_path} PATH)
  install(FILES ${_hdr}
    DESTINATION "${_path}"
  )
endforeach()


if (gRPC_INSTALL)
  install(TARGETS grpc++ EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()



if (gRPC_BUILD_CODEGEN)
add_library(grpc++_error_details
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/status/status.pb.cc
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/status/status.grpc.pb.cc
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/status/status.pb.h
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/status/status.grpc.pb.h
  ${GRPC_SOURCE_DIR}/src/cpp/util/error_details.cc
)

protobuf_generate_grpc_cpp(
  src/proto/grpc/status/status.proto
)

target_include_directories(grpc++_error_details
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)
target_link_libraries(grpc++_error_details
  ${_gRPC_BASELIB_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc++
)

foreach(_hdr
  ${GRPC_SOURCE_DIR}/include/grpc++/support/error_details.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/error_details.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/error_details_impl.h
)
  SET(_path ${_hdr})
  get_filename_component(_path ${_path} PATH)
  install(FILES ${_hdr}
    DESTINATION "${_path}"
  )
endforeach()
endif (gRPC_BUILD_CODEGEN)

if (gRPC_BUILD_CODEGEN)

if (gRPC_INSTALL)
  install(TARGETS grpc++_error_details EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()

endif (gRPC_BUILD_CODEGEN)

if (gRPC_BUILD_CODEGEN)
add_library(grpc++_reflection
  ${GRPC_SOURCE_DIR}/src/cpp/ext/proto_server_reflection.cc
  ${GRPC_SOURCE_DIR}/src/cpp/ext/proto_server_reflection_plugin.cc
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/reflection/v1alpha/reflection.pb.cc
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/reflection/v1alpha/reflection.grpc.pb.cc
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/reflection/v1alpha/reflection.pb.h
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/reflection/v1alpha/reflection.grpc.pb.h
)

protobuf_generate_grpc_cpp(
  src/proto/grpc/reflection/v1alpha/reflection.proto
)

target_include_directories(grpc++_reflection
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)
target_link_libraries(grpc++_reflection
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc++
  grpc
)

foreach(_hdr
  ${GRPC_SOURCE_DIR}/include/grpc++/ext/proto_server_reflection_plugin.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/ext/proto_server_reflection_plugin.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/ext/proto_server_reflection_plugin_impl.h
)
  SET(_path ${_hdr})
  get_filename_component(_path ${_path} PATH)
  install(FILES ${_hdr}
    DESTINATION "${_path}"
  )
endforeach()
endif (gRPC_BUILD_CODEGEN)

if (gRPC_BUILD_CODEGEN)

if (gRPC_INSTALL)
  install(TARGETS grpc++_reflection EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()

endif (gRPC_BUILD_CODEGEN)

add_library(grpc++_unsecure
  ${GRPC_SOURCE_DIR}/src/cpp/client/insecure_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/insecure_create_auth_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/insecure_server_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/channel_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/client_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/client_interceptor.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel_internal.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel_posix.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/credentials_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/generic_stub.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/alarm.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/channel_arguments.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/channel_filter.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/completion_queue_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/core_codegen.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/resource_quota_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/rpc_method.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/validate_service_config.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/version_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/async_generic_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/channel_argument_option.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/create_default_thread_pool.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/dynamic_thread_pool.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/external_connection_acceptor_impl.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/default_health_check_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/health_check_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/health_check_service_server_builder_option.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_builder.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_posix.cc
  ${GRPC_SOURCE_DIR}/src/cpp/thread_manager/thread_manager.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/byte_buffer_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/status.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/string_ref.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/time_cc.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/health/v1/health.upb.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/decode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/encode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/msg.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/port.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/table.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/upb.c
  ${GRPC_SOURCE_DIR}/src/cpp/codegen/codegen_init.cc
)

target_include_directories(grpc++_unsecure
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)
target_link_libraries(grpc++_unsecure
  ${_gRPC_BASELIB_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  gpr
  grpc_unsecure
)

foreach(_hdr
  ${GRPC_SOURCE_DIR}/include/grpc++/alarm.h
  ${GRPC_SOURCE_DIR}/include/grpc++/channel.h
  ${GRPC_SOURCE_DIR}/include/grpc++/client_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/completion_queue.h
  ${GRPC_SOURCE_DIR}/include/grpc++/create_channel.h
  ${GRPC_SOURCE_DIR}/include/grpc++/create_channel_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc++/ext/health_check_service_server_builder_option.h
  ${GRPC_SOURCE_DIR}/include/grpc++/generic/async_generic_service.h
  ${GRPC_SOURCE_DIR}/include/grpc++/generic/generic_stub.h
  ${GRPC_SOURCE_DIR}/include/grpc++/grpc++.h
  ${GRPC_SOURCE_DIR}/include/grpc++/health_check_service_interface.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/channel_argument_option.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/client_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/core_codegen.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/grpc_library.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/method_handler_impl.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/rpc_method.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/rpc_service_method.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/serialization_traits.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/server_builder_option.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/server_builder_plugin.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/server_initializer.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/service_type.h
  ${GRPC_SOURCE_DIR}/include/grpc++/resource_quota.h
  ${GRPC_SOURCE_DIR}/include/grpc++/security/auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/security/auth_metadata_processor.h
  ${GRPC_SOURCE_DIR}/include/grpc++/security/credentials.h
  ${GRPC_SOURCE_DIR}/include/grpc++/security/server_credentials.h
  ${GRPC_SOURCE_DIR}/include/grpc++/server.h
  ${GRPC_SOURCE_DIR}/include/grpc++/server_builder.h
  ${GRPC_SOURCE_DIR}/include/grpc++/server_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/server_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/async_stream.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/async_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/channel_arguments.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/config.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/status.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/status_code_enum.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/string_ref.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/stub_options.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/sync_stream.h
  ${GRPC_SOURCE_DIR}/include/grpc++/support/time.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/alarm.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/alarm_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/channel.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/channel_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/client_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/completion_queue.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/completion_queue_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/create_channel.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/create_channel_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/create_channel_posix.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/create_channel_posix_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/ext/health_check_service_server_builder_option.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/generic/async_generic_service.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/generic/generic_stub.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/generic/generic_stub_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/grpcpp.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/health_check_service_interface.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/health_check_service_interface_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/channel_argument_option.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/client_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/core_codegen.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/grpc_library.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/method_handler_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/rpc_method.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/rpc_service_method.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/serialization_traits.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/server_builder_option.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/server_builder_option_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/server_builder_plugin.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/server_initializer.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/server_initializer_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/service_type.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/resource_quota.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/resource_quota_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/auth_metadata_processor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/auth_metadata_processor_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/credentials.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/credentials_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/server_credentials.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/server_credentials_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/security/tls_credentials_options.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_builder.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_builder_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_posix.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/server_posix_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/async_stream.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/async_stream_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/async_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/async_unary_call_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/channel_arguments.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/channel_arguments_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/client_callback.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/client_callback_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/client_interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/config.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/message_allocator.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/proto_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/proto_buffer_writer.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/server_callback.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/server_callback_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/server_interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/slice.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/status.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/status_code_enum.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/string_ref.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/stub_options.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/sync_stream.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/sync_stream_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/time.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/support/validate_service_config.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/alloc.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm_gcc_atomic.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm_gcc_sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/atm_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/cpu.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/log.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/log_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/port_platform.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/string_util.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_custom.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_generic.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/sync_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/thd_id.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/time.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_atomic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_gcc_sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/atm_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/fork.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/gpr_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/log.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/port_platform.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_custom.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_generic.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/sync_windows.h
  ${GRPC_SOURCE_DIR}/include/grpc/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/byte_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpc/compression.h
  ${GRPC_SOURCE_DIR}/include/grpc/fork.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_posix.h
  ${GRPC_SOURCE_DIR}/include/grpc/grpc_security_constants.h
  ${GRPC_SOURCE_DIR}/include/grpc/load_reporting.h
  ${GRPC_SOURCE_DIR}/include/grpc/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/slice_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/status.h
  ${GRPC_SOURCE_DIR}/include/grpc/support/workaround_list.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/byte_buffer_reader.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/compression_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/connectivity_state.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/grpc_types.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/propagation_bits.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc/impl/codegen/status.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/async_stream.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/async_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/call_hook.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/channel_interface.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/client_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/client_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/completion_queue.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/completion_queue_tag.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/config.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/core_codegen_interface.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/create_auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/grpc_library.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/metadata_map.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/method_handler_impl.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/rpc_method.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/rpc_service_method.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/security/auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/serialization_traits.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/server_context.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/server_interface.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/service_type.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/slice.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/status.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/status_code_enum.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/string_ref.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/stub_options.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/sync_stream.h
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/time.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/async_generic_service.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/async_stream.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/async_stream_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/async_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/async_unary_call_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/byte_buffer.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/call_hook.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/call_op_set.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/call_op_set_interface.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/callback_common.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/channel_interface.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_callback.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_callback_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_context_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/client_unary_call.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/completion_queue.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/completion_queue_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/completion_queue_tag.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/config.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/core_codegen_interface.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/create_auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/delegating_channel.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/grpc_library.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/intercepted_channel.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/interceptor_common.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/message_allocator.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/metadata_map.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/method_handler.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/method_handler_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/rpc_method.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/rpc_service_method.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/security/auth_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/serialization_traits.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_callback.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_callback_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_context.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_context_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_interceptor.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/server_interface.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/service_type.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/slice.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/status.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/status_code_enum.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/string_ref.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/stub_options.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/sync_stream.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/sync_stream_impl.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/time.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/sync.h
)
  SET(_path ${_hdr})
  get_filename_component(_path ${_path} PATH)
  install(FILES ${_hdr}
    DESTINATION "${_path}"
  )
endforeach()


if (gRPC_INSTALL)
  install(TARGETS grpc++_unsecure EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()

add_library(grpc_plugin_support
  ${GRPC_SOURCE_DIR}/src/compiler/cpp_generator.cc
)

target_include_directories(grpc_plugin_support
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)
target_link_libraries(grpc_plugin_support
  ${_gRPC_PROTOBUF_PROTOC_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
)

foreach(_hdr
  ${GRPC_SOURCE_DIR}/include/grpc++/impl/codegen/config_protobuf.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/impl/codegen/config_protobuf.h
)
  string(REPLACE "include/" "" _path ${_hdr})
  get_filename_component(_path ${_path} PATH)
  install(FILES ${_hdr}
    DESTINATION "${gRPC_INSTALL_INCLUDEDIR}/${_path}"
  )
endforeach()


if (gRPC_INSTALL)
  install(TARGETS grpc_plugin_support EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()


if (gRPC_BUILD_CODEGEN)
add_library(grpcpp_channelz
  ${GRPC_SOURCE_DIR}/src/cpp/server/channelz/channelz_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/channelz/channelz_service_plugin.cc
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/channelz/channelz.pb.cc
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/channelz/channelz.grpc.pb.cc
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/channelz/channelz.pb.h
  ${_gRPC_PROTO_GENS_DIR}/src/proto/grpc/channelz/channelz.grpc.pb.h
)

protobuf_generate_grpc_cpp(
  src/proto/grpc/channelz/channelz.proto
)

target_include_directories(grpcpp_channelz
  PUBLIC $<INSTALL_INTERFACE:${gRPC_INSTALL_INCLUDEDIR}> $<BUILD_INTERFACE:${GRPC_SOURCE_DIR}/include>
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)
target_link_libraries(grpcpp_channelz
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc++
  grpc
)

foreach(_hdr
  ${GRPC_SOURCE_DIR}/include/grpcpp/ext/channelz_service_plugin.h
  ${GRPC_SOURCE_DIR}/include/grpcpp/ext/channelz_service_plugin_impl.h
)
  SET(_path ${_hdr})
  get_filename_component(_path ${_path} PATH)
  install(FILES ${_hdr}
    DESTINATION "${_path}"
  )
endforeach()
endif (gRPC_BUILD_CODEGEN)

if (gRPC_BUILD_CODEGEN)

if (gRPC_INSTALL)
  install(TARGETS grpcpp_channelz EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()

endif (gRPC_BUILD_CODEGEN)

add_executable(check_epollexclusive
  ${GRPC_SOURCE_DIR}/test/build/check_epollexclusive.c
)


target_include_directories(check_epollexclusive
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}/include
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)

target_link_libraries(check_epollexclusive
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc
  gpr
)

add_executable(grpc_create_jwt
  ${GRPC_SOURCE_DIR}/test/core/security/create_jwt.cc
  ${GRPC_SOURCE_DIR}/test/core/util/cmdline.cc
)


target_include_directories(grpc_create_jwt
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}/include
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)

target_link_libraries(grpc_create_jwt
  ${_gRPC_SSL_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc
  gpr
)

add_executable(grpc_print_google_default_creds_token
  ${GRPC_SOURCE_DIR}/test/core/security/print_google_default_creds_token.cc
  ${GRPC_SOURCE_DIR}/test/core/util/cmdline.cc
)


target_include_directories(grpc_print_google_default_creds_token
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}/include
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)

target_link_libraries(grpc_print_google_default_creds_token
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc
  gpr
)

add_executable(grpc_verify_jwt
  ${GRPC_SOURCE_DIR}/test/core/security/verify_jwt.cc
  ${GRPC_SOURCE_DIR}/test/core/util/cmdline.cc
)


target_include_directories(grpc_verify_jwt
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}/include
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)

target_link_libraries(grpc_verify_jwt
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc
  gpr
)

add_executable(gen_hpack_tables
  ${GRPC_SOURCE_DIR}/tools/codegen/core/gen_hpack_tables.cc
)


target_include_directories(gen_hpack_tables
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}/include
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)

target_link_libraries(gen_hpack_tables
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  gpr
  grpc
)



add_executable(gen_legal_metadata_characters
  ${GRPC_SOURCE_DIR}/tools/codegen/core/gen_legal_metadata_characters.cc
)


target_include_directories(gen_legal_metadata_characters
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)

target_link_libraries(gen_legal_metadata_characters
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
)



add_executable(gen_percent_encoding_tables
  ${GRPC_SOURCE_DIR}/tools/codegen/core/gen_percent_encoding_tables.cc
)


target_include_directories(gen_percent_encoding_tables
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}/include
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)

target_link_libraries(gen_percent_encoding_tables
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
)

if (gRPC_BUILD_CODEGEN)

add_executable(grpc_cpp_plugin
  ${GRPC_SOURCE_DIR}/src/compiler/cpp_plugin.cc
)


target_include_directories(grpc_cpp_plugin
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}/include
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_BENCHMARK_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_GFLAGS_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)

target_link_libraries(grpc_cpp_plugin
  ${_gRPC_PROTOBUF_PROTOC_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc_plugin_support
)



if (gRPC_INSTALL)
  install(TARGETS grpc_cpp_plugin EXPORT gRPCTargets
    RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
    LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR}
  )
endif()

endif (gRPC_BUILD_CODEGEN)


if (gRPC_INSTALL)
  install(EXPORT gRPCTargets
    DESTINATION ${gRPC_INSTALL_CMAKEDIR}
    NAMESPACE gRPC::
  )
endif()

foreach(_config gRPCConfig gRPCConfigVersion)
  configure_file(${GRPC_SOURCE_DIR}/cmake/${_config}.cmake.in
    ${_config}.cmake @ONLY)
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${_config}.cmake
    DESTINATION ${gRPC_INSTALL_CMAKEDIR}
  )
endforeach()

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/etc/roots.pem
  DESTINATION ${gRPC_INSTALL_SHAREDIR})

# Function to generate pkg-config files.
function(generate_pkgconfig name description version requires
                            libs libs_private output_filename)
  set(PC_NAME "${name}")
  set(PC_DESCRIPTION "${description}")
  set(PC_VERSION "${version}")
  set(PC_REQUIRES "${requires}")
  set(PC_LIB "${libs}")
  set(PC_LIBS_PRIVATE "${libs_private}")
  set(output_filepath "${grpc_BINARY_DIR}/libs/opt/pkgconfig/${output_filename}")
  configure_file(
    "${GRPC_SOURCE_DIR}/cmake/pkg-config-template.pc.in"
    "${output_filepath}"
    @ONLY)
  install(FILES "${output_filepath}"
    DESTINATION "lib/pkgconfig/")
endfunction()

# gpr .pc file
generate_pkgconfig(
  "gpr"
  "gRPC platform support library"
  "${gRPC_CORE_VERSION}"
  ""
  "-lgpr"
  ""
  "gpr.pc")

# grpc .pc file
generate_pkgconfig(
  "gRPC"
  "high performance general RPC framework"
  "${gRPC_CORE_VERSION}"
  "gpr openssl"
  "-lgrpc -laddress_sorting -lcares -lz"
  ""
  "grpc.pc")

# grpc_unsecure .pc file
generate_pkgconfig(
  "gRPC unsecure"
  "high performance general RPC framework without SSL"
  "${gRPC_CORE_VERSION}"
  "gpr"
  "-lgrpc_unsecure"
  ""
  "grpc_unsecure.pc")

# grpc++ .pc file
generate_pkgconfig(
  "gRPC++"
  "C++ wrapper for gRPC"
  "${PACKAGE_VERSION}"
  "grpc"
  "-lgrpc++"
  ""
  "grpc++.pc")

# grpc++_unsecure .pc file
generate_pkgconfig(
  "gRPC++ unsecure"
  "C++ wrapper for gRPC without SSL"
  "${PACKAGE_VERSION}"
  "grpc_unsecure"
  "-lgrpc++_unsecure"
  ""
  "grpc++_unsecure.pc")
