# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.0)

# Modified version of CMakeLists.txt for ClickHouse. Doesn't link the library to libm.
# Otherwise we have extra dependency when compiling with the most fresh libc version.
# How to check:
# readelf -s ./clickhouse | grep -F 2.23

SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/zookeeper/src/c)

project(zookeeper VERSION 3.5.3)
set(email user@zookeeper.apache.org)
set(description "zookeeper C client")

# general options
if(UNIX)
  add_compile_options(-Wall -fPIC)
elseif(WIN32)
  add_compile_options(/W3)
endif()
add_definitions(-DUSE_STATIC_LIB)

# TODO: Enable /WX and /W4 on Windows. Currently there are ~1000 warnings.
# TODO: Add Solaris support.
# TODO: Add a shared library option.
# TODO: Specify symbols to export.
# TODO: Generate doxygen documentation.

# Sync API option
option(WANT_SYNCAPI "Enables Sync API support" ON)
if(WANT_SYNCAPI)
  add_definitions(-DTHREADED)
  if(WIN32)
    # Note that the generator expression ensures that `/MTd` is used when Debug
    # configurations are built.
    add_compile_options(/MT$<$<CONFIG:Debug>:d>)
  endif()
endif()

# CppUnit option
if(WIN32 OR APPLE)
  # The tests do not yet compile on Windows or macOS,
  # so we set this to off by default.
  #
  # Note that CMake does not have expressions except in conditionals,
  # so we're left with this if/else/endif pattern.
  set(DEFAULT_WANT_CPPUNIT OFF)
else()
  set(DEFAULT_WANT_CPPUNIT ON)
endif()
option(WANT_CPPUNIT "Enables CppUnit and tests" ${DEFAULT_WANT_CPPUNIT})

# The function `to_have(in out)` converts a header name like `arpa/inet.h`
# into an Autotools style preprocessor definition `HAVE_ARPA_INET_H`.
# This is then set or unset in `configure_file()` step.
#
# Note that CMake functions do not have return values; instead an "out"
# variable must be passed, and explicitly set with parent scope.
function(to_have in out)
  string(TOUPPER ${in} str)
  string(REGEX REPLACE "/|\\." "_" str ${str})
  set(${out} "HAVE_${str}" PARENT_SCOPE)
endfunction()

# include file checks
foreach(f generated/zookeeper.jute.h generated/zookeeper.jute.c)
  if(EXISTS "${LIBRARY_DIR}/${f}")
    to_have(${f} name)
    set(${name} 1)
  else()
    message(FATAL_ERROR
      "jute files are missing!\n"
      "Please run 'ant compile_jute' while in the ZooKeeper top level directory.")
  endif()
endforeach()

# header checks
include(CheckIncludeFile)
set(check_headers
  arpa/inet.h
  dlfcn.h
  fcntl.h
  inttypes.h
  memory.h
  netdb.h
  netinet/in.h
  stdint.h
  stdlib.h
  string.h
  strings.h
  sys/socket.h
  sys/stat.h
  sys/time.h
  sys/types.h
  unistd.h
  sys/utsname.h)

foreach(f ${check_headers})
  to_have(${f} name)
  check_include_file(${f} ${name})
endforeach()

# function checks
include(CheckFunctionExists)
set(check_functions
  getcwd
  gethostbyname
  gethostname
  getlogin
  getpwuid_r
  gettimeofday
  getuid
  memmove
  memset
  poll
  socket
  strchr
  strdup
  strerror
  strtol)

foreach(fn ${check_functions})
  to_have(${fn} name)
  check_function_exists(${fn} ${name})
endforeach()

# library checks
set(check_libraries rt m pthread)
foreach(lib ${check_libraries})
  to_have("lib${lib}" name)
  find_library(${name} ${lib})
endforeach()

# IPv6 check
include(CheckStructHasMember)
check_struct_has_member("struct sockaddr_in6" sin6_addr "netinet/in.h" ZOO_IPV6_ENABLED)

# configure
configure_file(${LIBRARY_DIR}/cmake_config.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/config.h)

# hashtable library
set(hashtable_sources ${LIBRARY_DIR}/src/hashtable/hashtable_itr.c ${LIBRARY_DIR}/src/hashtable/hashtable.c)
add_library(hashtable STATIC ${hashtable_sources})

# zookeeper library
set(zookeeper_sources
  ${LIBRARY_DIR}/src/zookeeper.c
  ${LIBRARY_DIR}/src/recordio.c
  ${LIBRARY_DIR}/generated/zookeeper.jute.c
  ${LIBRARY_DIR}/src/zk_log.c
  ${LIBRARY_DIR}/src/zk_hashtable.c
)
#  src/addrvec.c

if(WANT_SYNCAPI)
  list(APPEND zookeeper_sources ${LIBRARY_DIR}/src/mt_adaptor.c)
else()
  list(APPEND zookeeper_sources ${LIBRARY_DIR}/src/st_adaptor.c)
endif()

if(WIN32)
  list(APPEND zookeeper_sources ${LIBRARY_DIR}/src/winport.c)
endif()

add_library(zookeeper STATIC ${zookeeper_sources})
target_include_directories(zookeeper BEFORE PUBLIC ${LIBRARY_DIR}/include ${CMAKE_CURRENT_BINARY_DIR}/include ${LIBRARY_DIR}/generated)
target_link_libraries(zookeeper PUBLIC
  hashtable
  $<$<PLATFORM_ID:Linux>:rt> # clock_gettime
  $<$<PLATFORM_ID:Windows>:ws2_32>) # Winsock 2.0

if(WANT_SYNCAPI AND NOT WIN32)
  find_package(Threads REQUIRED)
  target_link_libraries(zookeeper PUBLIC Threads::Threads)
endif()

# cli executable
add_executable(cli ${LIBRARY_DIR}/src/cli.c)
target_link_libraries(cli zookeeper)
target_link_libraries(cli $<$<PLATFORM_ID:Linux>:m>)

# load_gen executable
if(WANT_SYNCAPI AND NOT WIN32)
  add_executable(load_gen ${LIBRARY_DIR}/src/load_gen.c)
  target_link_libraries(load_gen zookeeper)
  target_link_libraries(load_gen $<$<PLATFORM_ID:Linux>:m>)
endif()
