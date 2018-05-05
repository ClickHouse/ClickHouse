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
#  ARROW_STATIC_LIB, path to libarrow.a
#  ARROW_SHARED_LIB, path to libarrow's shared library
#  ARROW_FOUND, whether arrow has been found

if (DEFINED ENV{ARROW_HOME})
  set(ARROW_HOME "$ENV{ARROW_HOME}")
endif()

if ("${ARROW_HOME}" STREQUAL "")
  # PARQUET-955. If the user has set $ARROW_HOME in the environment, we respect
  # this, otherwise try to locate the pkgconfig in the system environment
  pkg_check_modules(ARROW arrow)
  if (ARROW_FOUND)
    # We found the pkgconfig
    set(ARROW_INCLUDE_DIR ${ARROW_INCLUDE_DIRS})

    if (COMMAND pkg_get_variable)
      pkg_get_variable(ARROW_ABI_VERSION arrow abi_version)
    else()
      set(ARROW_ABI_VERSION "")
    endif()
    if (ARROW_ABI_VERSION STREQUAL "")
      set(ARROW_SHARED_LIB_SUFFIX "")
    else()
      set(ARROW_SHARED_LIB_SUFFIX ".${ARROW_ABI_VERSION}")
    endif()

    set(ARROW_LIB_NAME ${CMAKE_SHARED_LIBRARY_PREFIX}arrow)

    if (APPLE)
      set(ARROW_SHARED_LIB ${ARROW_LIBDIR}/${ARROW_LIB_NAME}${ARROW_SHARED_LIB_SUFFIX}${CMAKE_SHARED_LIBRARY_SUFFIX})
    else()
      set(ARROW_SHARED_LIB ${ARROW_LIBDIR}/${ARROW_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}${ARROW_SHARED_LIB_SUFFIX})
    endif()
    set(ARROW_STATIC_LIB ${ARROW_LIBDIR}/${ARROW_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX})
  endif()
else()
  set(ARROW_HOME "${ARROW_HOME}")

  if (MSVC AND NOT ARROW_MSVC_STATIC_LIB_SUFFIX)
    set(ARROW_MSVC_STATIC_LIB_SUFFIX _static)
  endif()

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

  find_library(ARROW_LIB_PATH NAMES arrow arrow${ARROW_MSVC_STATIC_LIB_SUFFIX}
    PATHS
    ${ARROW_SEARCH_LIB_PATH}
    NO_DEFAULT_PATH)

  if (ARROW_INCLUDE_DIR AND (PARQUET_MINIMAL_DEPENDENCY OR ARROW_LIB_PATH))
    set(ARROW_FOUND TRUE)
    set(ARROW_HEADER_NAME arrow/api.h)
    set(ARROW_HEADER ${ARROW_INCLUDE_DIR}/${ARROW_HEADER_NAME})
    set(ARROW_LIB_NAME arrow)

    get_filename_component(ARROW_LIBS ${ARROW_LIB_PATH} DIRECTORY)
    set(ARROW_STATIC_LIB ${ARROW_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${ARROW_LIB_NAME}${ARROW_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
    set(ARROW_SHARED_LIB ${ARROW_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_SHARED_IMPLIB ${ARROW_LIBS}/${ARROW_LIB_NAME}.lib)
  endif ()
endif()

if (ARROW_FOUND)
  if (NOT Arrow_FIND_QUIETLY)
    message(STATUS "Arrow include path: ${ARROW_INCLUDE_DIR}")
    if (PARQUET_MINIMAL_DEPENDENCY)
      message(STATUS "Found the Arrow header: ${ARROW_HEADER}")
    else ()
      message(STATUS "Found the Arrow library: ${ARROW_LIB_PATH}")
    endif ()
  endif ()
else()
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
endif()

mark_as_advanced(
  ARROW_FOUND
  ARROW_INCLUDE_DIR
  ARROW_STATIC_LIB
  ARROW_SHARED_LIB
)
