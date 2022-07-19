# https://github.com/apache/arrow/blob/master/cpp/cmake_modules/FindParquet.cmake

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# - Find Parquet (parquet/api/reader.h, libparquet.a, libparquet.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  PARQUET_FOUND, whether Parquet has been found
#  PARQUET_IMPORT_LIB, path to libparquet's import library (Windows only)
#  PARQUET_INCLUDE_DIR, directory containing headers
#  PARQUET_LIBS, deprecated. Use PARQUET_LIB_DIR instead
#  PARQUET_LIB_DIR, directory containing Parquet libraries
#  PARQUET_SHARED_IMP_LIB, deprecated. Use PARQUET_IMPORT_LIB instead
#  PARQUET_SHARED_LIB, path to libparquet's shared library
#  PARQUET_SO_VERSION, shared object version of found Parquet such as "100"
#  PARQUET_STATIC_LIB, path to libparquet.a

if(DEFINED PARQUET_FOUND)
  return()
endif()

set(find_package_arguments)
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_VERSION)
  list(APPEND find_package_arguments "${${CMAKE_FIND_PACKAGE_NAME}_FIND_VERSION}")
endif()
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_REQUIRED)
  list(APPEND find_package_arguments REQUIRED)
endif()
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_QUIETLY)
  list(APPEND find_package_arguments QUIET)
endif()
find_package(Arrow ${find_package_arguments})

if(NOT "$ENV{PARQUET_HOME}" STREQUAL "")
  file(TO_CMAKE_PATH "$ENV{PARQUET_HOME}" PARQUET_HOME)
endif()

if((NOT PARQUET_HOME) AND ARROW_HOME)
  set(PARQUET_HOME ${ARROW_HOME})
endif()

if(ARROW_FOUND)
  arrow_find_package(PARQUET
                     "${PARQUET_HOME}"
                     parquet
                     parquet/api/reader.h
                     Parquet
                     parquet)
  if(PARQUET_HOME)
    if(PARQUET_INCLUDE_DIR)
      file(READ "${PARQUET_INCLUDE_DIR}/parquet/parquet_version.h"
           PARQUET_VERSION_H_CONTENT)
      arrow_extract_macro_value(PARQUET_VERSION_MAJOR "PARQUET_VERSION_MAJOR"
                                "${PARQUET_VERSION_H_CONTENT}")
      arrow_extract_macro_value(PARQUET_VERSION_MINOR "PARQUET_VERSION_MINOR"
                                "${PARQUET_VERSION_H_CONTENT}")
      arrow_extract_macro_value(PARQUET_VERSION_PATCH "PARQUET_VERSION_PATCH"
                                "${PARQUET_VERSION_H_CONTENT}")
      if("${PARQUET_VERSION_MAJOR}" STREQUAL ""
         OR "${PARQUET_VERSION_MINOR}" STREQUAL ""
         OR "${PARQUET_VERSION_PATCH}" STREQUAL "")
        set(PARQUET_VERSION "0.0.0")
      else()
        set(PARQUET_VERSION
            "${PARQUET_VERSION_MAJOR}.${PARQUET_VERSION_MINOR}.${PARQUET_VERSION_PATCH}")
      endif()

      arrow_extract_macro_value(PARQUET_SO_VERSION_QUOTED "PARQUET_SO_VERSION"
                                "${PARQUET_VERSION_H_CONTENT}")
      string(REGEX
             REPLACE "^\"(.+)\"$" "\\1" PARQUET_SO_VERSION "${PARQUET_SO_VERSION_QUOTED}")
      arrow_extract_macro_value(PARQUET_FULL_SO_VERSION_QUOTED "PARQUET_FULL_SO_VERSION"
                                "${PARQUET_VERSION_H_CONTENT}")
      string(REGEX
             REPLACE "^\"(.+)\"$" "\\1" PARQUET_FULL_SO_VERSION
                     "${PARQUET_FULL_SO_VERSION_QUOTED}")
    endif()
  else()
    if(PARQUET_USE_CMAKE_PACKAGE_CONFIG)
      find_package(Parquet CONFIG)
    elseif(PARQUET_USE_PKG_CONFIG)
      pkg_get_variable(PARQUET_SO_VERSION parquet so_version)
      pkg_get_variable(PARQUET_FULL_SO_VERSION parquet full_so_version)
    endif()
  endif()
  set(PARQUET_ABI_VERSION "${PARQUET_SO_VERSION}")
endif()

mark_as_advanced(PARQUET_ABI_VERSION
                 PARQUET_IMPORT_LIB
                 PARQUET_INCLUDE_DIR
                 PARQUET_LIBS
                 PARQUET_LIB_DIR
                 PARQUET_SHARED_IMP_LIB
                 PARQUET_SHARED_LIB
                 PARQUET_SO_VERSION
                 PARQUET_STATIC_LIB
                 PARQUET_VERSION)

find_package_handle_standard_args(Parquet
                                  REQUIRED_VARS
                                  PARQUET_INCLUDE_DIR
                                  PARQUET_LIB_DIR
                                  PARQUET_SO_VERSION
                                  VERSION_VAR
                                  PARQUET_VERSION)
set(PARQUET_FOUND ${Parquet_FOUND})

if(Parquet_FOUND AND NOT Parquet_FIND_QUIETLY)
  message(STATUS "Parquet version: ${PARQUET_VERSION} (${PARQUET_FIND_APPROACH})")
  message(STATUS "Found the Parquet shared library: ${PARQUET_SHARED_LIB}")
  message(STATUS "Found the Parquet import library: ${PARQUET_IMPORT_LIB}")
  message(STATUS "Found the Parquet static library: ${PARQUET_STATIC_LIB}")
endif()
