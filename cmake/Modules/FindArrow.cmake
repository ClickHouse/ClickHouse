# https://github.com/apache/arrow/blob/master/cpp/cmake_modules/FindArrow.cmake

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

# - Find ARROW (arrow/api.h, libarrow.a, libarrow.so)
# This module defines
#  ARROW_INCLUDE_DIR, directory containing headers
#  ARROW_LIBS, directory containing arrow libraries
#  ARROW_STATIC_LIB, path to libarrow.a
#  ARROW_SHARED_LIB, path to libarrow's shared library
#  ARROW_SHARED_IMP_LIB, path to libarrow's import library (MSVC only)
#  ARROW_FOUND, whether arrow has been found

include(FindPkgConfig)
include(GNUInstallDirs)

if ("$ENV{ARROW_HOME}" STREQUAL "")
  pkg_check_modules(ARROW arrow)
  if (ARROW_FOUND)
    pkg_get_variable(ARROW_SO_VERSION arrow so_version)
    set(ARROW_ABI_VERSION ${ARROW_SO_VERSION})
    message(STATUS "Arrow SO and ABI version: ${ARROW_SO_VERSION}")
    pkg_get_variable(ARROW_FULL_SO_VERSION arrow full_so_version)
    message(STATUS "Arrow full SO version: ${ARROW_FULL_SO_VERSION}")
    if ("${ARROW_INCLUDE_DIRS}" STREQUAL "")
      set(ARROW_INCLUDE_DIRS "/usr/${CMAKE_INSTALL_INCLUDEDIR}")
    endif()
    if ("${ARROW_LIBRARY_DIRS}" STREQUAL "")
      set(ARROW_LIBRARY_DIRS "/usr/${CMAKE_INSTALL_LIBDIR}")
      if (EXISTS "/etc/debian_version" AND CMAKE_LIBRARY_ARCHITECTURE)
        set(ARROW_LIBRARY_DIRS
          "${ARROW_LIBRARY_DIRS}/${CMAKE_LIBRARY_ARCHITECTURE}")
      endif()
    endif()
    set(ARROW_INCLUDE_DIR ${ARROW_INCLUDE_DIRS})
    set(ARROW_LIBS ${ARROW_LIBRARY_DIRS})
    set(ARROW_SEARCH_LIB_PATH ${ARROW_LIBRARY_DIRS})
  endif()
else()
  set(ARROW_HOME "$ENV{ARROW_HOME}")

  set(ARROW_SEARCH_HEADER_PATHS
    ${ARROW_HOME}/include
    )

  set(ARROW_SEARCH_LIB_PATH
    ${ARROW_HOME}/lib
    )

  find_path(ARROW_INCLUDE_DIR arrow/array.h PATHS
    ${ARROW_SEARCH_HEADER_PATHS}
    # make sure we don't accidentally pick up a different version
    NO_DEFAULT_PATH
    )
endif()

find_library(ARROW_LIB_PATH NAMES arrow
  PATHS
  ${ARROW_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)
get_filename_component(ARROW_LIBS ${ARROW_LIB_PATH} DIRECTORY)

find_library(ARROW_PYTHON_LIB_PATH NAMES arrow_python
  PATHS
  ${ARROW_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)
get_filename_component(ARROW_PYTHON_LIBS ${ARROW_PYTHON_LIB_PATH} DIRECTORY)

if (MSVC)
  SET(CMAKE_FIND_LIBRARY_SUFFIXES ".lib" ".dll")

  if (MSVC AND NOT DEFINED ARROW_MSVC_STATIC_LIB_SUFFIX)
    set(ARROW_MSVC_STATIC_LIB_SUFFIX "_static")
  endif()

  find_library(ARROW_SHARED_LIBRARIES NAMES arrow
    PATHS ${ARROW_HOME} NO_DEFAULT_PATH
    PATH_SUFFIXES "bin" )

  find_library(ARROW_PYTHON_SHARED_LIBRARIES NAMES arrow_python
    PATHS ${ARROW_HOME} NO_DEFAULT_PATH
    PATH_SUFFIXES "bin" )
  get_filename_component(ARROW_SHARED_LIBS ${ARROW_SHARED_LIBRARIES} PATH )
  get_filename_component(ARROW_PYTHON_SHARED_LIBS ${ARROW_PYTHON_SHARED_LIBRARIES} PATH )
endif ()

if (ARROW_INCLUDE_DIR AND ARROW_LIBS)
  set(ARROW_FOUND TRUE)
  set(ARROW_LIB_NAME arrow)
  set(ARROW_PYTHON_LIB_NAME arrow_python)
  if (MSVC)
    set(ARROW_STATIC_LIB ${ARROW_LIBS}/${ARROW_LIB_NAME}${ARROW_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
    set(ARROW_PYTHON_STATIC_LIB ${ARROW_PYTHON_LIBS}/${ARROW_PYTHON_LIB_NAME}${ARROW_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
    set(ARROW_SHARED_LIB ${ARROW_SHARED_LIBS}/${ARROW_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_PYTHON_SHARED_LIB ${ARROW_PYTHON_SHARED_LIBS}/${ARROW_PYTHON_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_SHARED_IMP_LIB ${ARROW_LIBS}/${ARROW_LIB_NAME}.lib)
    set(ARROW_PYTHON_SHARED_IMP_LIB ${ARROW_PYTHON_LIBS}/${ARROW_PYTHON_LIB_NAME}.lib)
  else()
    set(ARROW_STATIC_LIB ${ARROW_LIBS}/lib${ARROW_LIB_NAME}.a)
    set(ARROW_PYTHON_STATIC_LIB ${ARROW_LIBS}/lib${ARROW_PYTHON_LIB_NAME}.a)

    set(ARROW_SHARED_LIB ${ARROW_LIBS}/lib${ARROW_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_PYTHON_SHARED_LIB ${ARROW_LIBS}/lib${ARROW_PYTHON_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()
endif()

if (ARROW_FOUND)
  if (NOT Arrow_FIND_QUIETLY)
    message(STATUS "Found the Arrow core library: ${ARROW_LIB_PATH}")
    message(STATUS "Found the Arrow Python library: ${ARROW_PYTHON_LIB_PATH}")
  endif ()
else ()
  if (NOT Arrow_FIND_QUIETLY)
    set(ARROW_ERR_MSG "Could not find the Arrow library. Looked for headers")
    set(ARROW_ERR_MSG "${ARROW_ERR_MSG} in ${ARROW_SEARCH_HEADER_PATHS}, and for libs")
    set(ARROW_ERR_MSG "${ARROW_ERR_MSG} in ${ARROW_SEARCH_LIB_PATH}")
    if (Arrow_FIND_REQUIRED)
      message(FATAL_ERROR "${ARROW_ERR_MSG}")
    else (Arrow_FIND_REQUIRED)
      message(STATUS "${ARROW_ERR_MSG}")
    endif (Arrow_FIND_REQUIRED)
  endif ()
  set(ARROW_FOUND FALSE)
endif ()

if (MSVC)
  mark_as_advanced(
    ARROW_INCLUDE_DIR
    ARROW_STATIC_LIB
    ARROW_SHARED_LIB
    ARROW_SHARED_IMP_LIB
    ARROW_PYTHON_STATIC_LIB
    ARROW_PYTHON_SHARED_LIB
    ARROW_PYTHON_SHARED_IMP_LIB
  )
else()
  mark_as_advanced(
    ARROW_INCLUDE_DIR
    ARROW_STATIC_LIB
    ARROW_SHARED_LIB
    ARROW_PYTHON_STATIC_LIB
    ARROW_PYTHON_SHARED_LIB
  )
endif()
