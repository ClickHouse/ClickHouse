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

# - Find PARQUET (parquet/parquet.h, libparquet.a, libparquet.so)
# This module defines
#  PARQUET_INCLUDE_DIR, directory containing headers
#  PARQUET_LIBS, directory containing parquet libraries
#  PARQUET_STATIC_LIB, path to libparquet.a
#  PARQUET_SHARED_LIB, path to libparquet's shared library
#  PARQUET_SHARED_IMP_LIB, path to libparquet's import library (MSVC only)
#  PARQUET_FOUND, whether parquet has been found

include(FindPkgConfig)

if(NOT "$ENV{PARQUET_HOME}" STREQUAL "")
    set(PARQUET_HOME "$ENV{PARQUET_HOME}")
endif()

if (MSVC)
  SET(CMAKE_FIND_LIBRARY_SUFFIXES ".lib" ".dll")

  if (MSVC AND NOT DEFINED PARQUET_MSVC_STATIC_LIB_SUFFIX)
    set(PARQUET_MSVC_STATIC_LIB_SUFFIX "_static")
  endif()

  find_library(PARQUET_SHARED_LIBRARIES NAMES parquet
    PATHS ${PARQUET_HOME} NO_DEFAULT_PATH
    PATH_SUFFIXES "bin" )

  get_filename_component(PARQUET_SHARED_LIBS ${PARQUET_SHARED_LIBRARIES} PATH )
endif ()

if(PARQUET_HOME)
    set(PARQUET_SEARCH_HEADER_PATHS
        ${PARQUET_HOME}/include
        )
    set(PARQUET_SEARCH_LIB_PATH
        ${PARQUET_HOME}/lib
        )
    find_path(PARQUET_INCLUDE_DIR parquet/api/reader.h PATHS
        ${PARQUET_SEARCH_HEADER_PATHS}
        # make sure we don't accidentally pick up a different version
        NO_DEFAULT_PATH
        )
    find_library(PARQUET_LIBRARIES NAMES parquet
        PATHS ${PARQUET_HOME} NO_DEFAULT_PATH
        PATH_SUFFIXES "lib")
    get_filename_component(PARQUET_LIBS ${PARQUET_LIBRARIES} PATH )

    # Try to autodiscover the Parquet ABI version
    get_filename_component(PARQUET_LIB_REALPATH ${PARQUET_LIBRARIES} REALPATH)
    get_filename_component(PARQUET_EXT_REALPATH ${PARQUET_LIB_REALPATH} EXT)
    string(REGEX MATCH ".([0-9]+.[0-9]+.[0-9]+)" HAS_ABI_VERSION ${PARQUET_EXT_REALPATH})
    if (HAS_ABI_VERSION)
      if (APPLE)
        string(REGEX REPLACE ".([0-9]+.[0-9]+.[0-9]+).dylib" "\\1" PARQUET_ABI_VERSION ${PARQUET_EXT_REALPATH})
      else()
        string(REGEX REPLACE ".so.([0-9]+.[0-9]+.[0-9]+)" "\\1" PARQUET_ABI_VERSION ${PARQUET_EXT_REALPATH})
      endif()
      string(REGEX REPLACE "([0-9]+).[0-9]+.[0-9]+" "\\1" PARQUET_SO_VERSION ${PARQUET_ABI_VERSION})
    else()
      set(PARQUET_ABI_VERSION "1.0.0")
      set(PARQUET_SO_VERSION "1")
    endif()
else()
    pkg_check_modules(PARQUET parquet)
    if (PARQUET_FOUND)
        pkg_get_variable(PARQUET_ABI_VERSION parquet abi_version)
        message(STATUS "Parquet C++ ABI version: ${PARQUET_ABI_VERSION}")
        pkg_get_variable(PARQUET_SO_VERSION parquet so_version)
        message(STATUS "Parquet C++ SO version: ${PARQUET_SO_VERSION}")
        set(PARQUET_INCLUDE_DIR ${PARQUET_INCLUDE_DIRS})
        set(PARQUET_LIBS ${PARQUET_LIBRARY_DIRS})
        set(PARQUET_SEARCH_LIB_PATH ${PARQUET_LIBRARY_DIRS})
        message(STATUS "Searching for parquet libs in: ${PARQUET_SEARCH_LIB_PATH}")
        find_library(PARQUET_LIBRARIES NAMES parquet
            PATHS ${PARQUET_SEARCH_LIB_PATH} NO_DEFAULT_PATH)
    else()
        find_path(PARQUET_INCLUDE_DIR NAMES parquet/api/reader.h )
        find_library(PARQUET_LIBRARIES NAMES parquet)
        get_filename_component(PARQUET_LIBS ${PARQUET_LIBRARIES} PATH )
    endif()
endif()

if (PARQUET_INCLUDE_DIR AND PARQUET_LIBRARIES)
  set(PARQUET_FOUND TRUE)
  set(PARQUET_LIB_NAME parquet)
  if (MSVC)
    set(PARQUET_STATIC_LIB "${PARQUET_LIBS}/${PARQUET_LIB_NAME}${PARQUET_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(PARQUET_SHARED_LIB "${PARQUET_SHARED_LIBS}/${PARQUET_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(PARQUET_SHARED_IMP_LIB "${PARQUET_LIBS}/${PARQUET_LIB_NAME}.lib")
  else()
    set(PARQUET_STATIC_LIB ${PARQUET_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${PARQUET_LIB_NAME}.a)
    set(PARQUET_SHARED_LIB ${PARQUET_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${PARQUET_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()
else ()
  set(PARQUET_FOUND FALSE)
endif ()

if (PARQUET_FOUND)
  if (NOT Parquet_FIND_QUIETLY)
    message(STATUS "Found the Parquet library: ${PARQUET_LIBRARIES}")
  endif ()
else ()
  if (NOT Parquet_FIND_QUIETLY)
    if (NOT PARQUET_FOUND)
      set(PARQUET_ERR_MSG "${PARQUET_ERR_MSG} Could not find the parquet library.")
    endif()

    set(PARQUET_ERR_MSG "${PARQUET_ERR_MSG} Looked in ")
    if ( _parquet_roots )
      set(PARQUET_ERR_MSG "${PARQUET_ERR_MSG} in ${_parquet_roots}.")
    else ()
      set(PARQUET_ERR_MSG "${PARQUET_ERR_MSG} system search paths.")
    endif ()
    if (Parquet_FIND_REQUIRED)
      message(FATAL_ERROR "${PARQUET_ERR_MSG}")
    else (Parquet_FIND_REQUIRED)
      message(STATUS "${PARQUET_ERR_MSG}")
    endif (Parquet_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  PARQUET_FOUND
  PARQUET_INCLUDE_DIR
  PARQUET_LIBS
  PARQUET_LIBRARIES
  PARQUET_STATIC_LIB
  PARQUET_SHARED_LIB
)
