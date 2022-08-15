# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

#.rst:
# FindICU
# -------
#
# Find the International Components for Unicode (ICU) libraries and
# programs.
#
# This module supports multiple components.
# Components can include any of: ``data``, ``i18n``, ``io``, ``le``,
# ``lx``, ``test``, ``tu`` and ``uc``.
#
# Note that on Windows ``data`` is named ``dt`` and ``i18n`` is named
# ``in``; any of the names may be used, and the appropriate
# platform-specific library name will be automatically selected.
#
# This module reports information about the ICU installation in
# several variables.  General variables::
#
#   ICU_VERSION - ICU release version
#   ICU_FOUND - true if the main programs and libraries were found
#   ICU_LIBRARIES - component libraries to be linked
#   ICU_INCLUDE_DIRS - the directories containing the ICU headers
#
# Imported targets::
#
#   ICU::<C>
#
# Where ``<C>`` is the name of an ICU component, for example
# ``ICU::i18n``.
#
# ICU programs are reported in::
#
#   ICU_GENCNVAL_EXECUTABLE - path to gencnval executable
#   ICU_ICUINFO_EXECUTABLE - path to icuinfo executable
#   ICU_GENBRK_EXECUTABLE - path to genbrk executable
#   ICU_ICU-CONFIG_EXECUTABLE - path to icu-config executable
#   ICU_GENRB_EXECUTABLE - path to genrb executable
#   ICU_GENDICT_EXECUTABLE - path to gendict executable
#   ICU_DERB_EXECUTABLE - path to derb executable
#   ICU_PKGDATA_EXECUTABLE - path to pkgdata executable
#   ICU_UCONV_EXECUTABLE - path to uconv executable
#   ICU_GENCFU_EXECUTABLE - path to gencfu executable
#   ICU_MAKECONV_EXECUTABLE - path to makeconv executable
#   ICU_GENNORM2_EXECUTABLE - path to gennorm2 executable
#   ICU_GENCCODE_EXECUTABLE - path to genccode executable
#   ICU_GENSPREP_EXECUTABLE - path to gensprep executable
#   ICU_ICUPKG_EXECUTABLE - path to icupkg executable
#   ICU_GENCMN_EXECUTABLE - path to gencmn executable
#
# ICU component libraries are reported in::
#
#   ICU_<C>_FOUND - ON if component was found
#   ICU_<C>_LIBRARIES - libraries for component
#
# ICU datafiles are reported in::
#
#   ICU_MAKEFILE_INC - Makefile.inc
#   ICU_PKGDATA_INC - pkgdata.inc
#
# Note that ``<C>`` is the uppercased name of the component.
#
# This module reads hints about search results from::
#
#   ICU_ROOT - the root of the ICU installation
#
# The environment variable ``ICU_ROOT`` may also be used; the
# ICU_ROOT variable takes precedence.
#
# The following cache variables may also be set::
#
#   ICU_<P>_EXECUTABLE - the path to executable <P>
#   ICU_INCLUDE_DIR - the directory containing the ICU headers
#   ICU_<C>_LIBRARY - the library for component <C>
#
# .. note::
#
#   In most cases none of the above variables will require setting,
#   unless multiple ICU versions are available and a specific version
#   is required.
#
# Other variables one may set to control this module are::
#
#   ICU_DEBUG - Set to ON to enable debug output from FindICU.

# Written by Roger Leigh <rleigh@codelibre.net>

set(icu_programs
  gencnval
  icuinfo
  genbrk
  icu-config
  genrb
  gendict
  derb
  pkgdata
  uconv
  gencfu
  makeconv
  gennorm2
  genccode
  gensprep
  icupkg
  gencmn)

set(icu_data
  Makefile.inc
  pkgdata.inc)

# The ICU checks are contained in a function due to the large number
# of temporary variables needed.
function(_ICU_FIND)
  # Set up search paths, taking compiler into account.  Search ICU_ROOT,
  # with ICU_ROOT in the environment as a fallback if unset.
  if(ICU_ROOT)
    list(APPEND icu_roots "${ICU_ROOT}")
  else()
    if(NOT "$ENV{ICU_ROOT}" STREQUAL "")
      file(TO_CMAKE_PATH "$ENV{ICU_ROOT}" NATIVE_PATH)
      list(APPEND icu_roots "${NATIVE_PATH}")
      set(ICU_ROOT "${NATIVE_PATH}"
          CACHE PATH "Location of the ICU installation" FORCE)
    endif()
  endif()

  # Find include directory
  list(APPEND icu_include_suffixes "include")
  find_path(ICU_INCLUDE_DIR
            NAMES "unicode/utypes.h"
            HINTS ${icu_roots}
            PATH_SUFFIXES ${icu_include_suffixes}
            DOC "ICU include directory")
  set(ICU_INCLUDE_DIR "${ICU_INCLUDE_DIR}" PARENT_SCOPE)

  # Get version
  if(ICU_INCLUDE_DIR AND EXISTS "${ICU_INCLUDE_DIR}/unicode/uvernum.h")
    file(STRINGS "${ICU_INCLUDE_DIR}/unicode/uvernum.h" icu_header_str
      REGEX "^#define[\t ]+U_ICU_VERSION[\t ]+\".*\".*")

    string(REGEX REPLACE "^#define[\t ]+U_ICU_VERSION[\t ]+\"([^ \\n]*)\".*"
      "\\1" icu_version_string "${icu_header_str}")
    set(ICU_VERSION "${icu_version_string}")
    set(ICU_VERSION "${icu_version_string}" PARENT_SCOPE)
    unset(icu_header_str)
    unset(icu_version_string)
  endif()

  if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    # 64-bit binary directory
    set(_bin64 "bin64")
    # 64-bit library directory
    set(_lib64 "lib64")
  endif()


  # Find all ICU programs
  list(APPEND icu_binary_suffixes "${_bin64}" "bin")
  foreach(program ${icu_programs})
    string(TOUPPER "${program}" program_upcase)
    set(cache_var "ICU_${program_upcase}_EXECUTABLE")
    set(program_var "ICU_${program_upcase}_EXECUTABLE")
    find_program("${cache_var}" "${program}"
      HINTS ${icu_roots}
      PATH_SUFFIXES ${icu_binary_suffixes}
      DOC "ICU ${program} executable")
    mark_as_advanced(cache_var)
    set("${program_var}" "${${cache_var}}" PARENT_SCOPE)
  endforeach()

  # Find all ICU libraries
  list(APPEND icu_library_suffixes "${_lib64}" "lib")
  set(ICU_REQUIRED_LIBS_FOUND ON)
  foreach(component ${ICU_FIND_COMPONENTS})
    string(TOUPPER "${component}" component_upcase)
    set(component_cache "ICU_${component_upcase}_LIBRARY")
    set(component_cache_release "${component_cache}_RELEASE")
    set(component_cache_debug "${component_cache}_DEBUG")
    set(component_found "${component_upcase}_FOUND")
    set(component_libnames "icu${component}")
    set(component_debug_libnames "icu${component}d")

    # Special case deliberate library naming mismatches between Unix
    # and Windows builds
    unset(component_libnames)
    unset(component_debug_libnames)
    list(APPEND component_libnames "icu${component}")
    list(APPEND component_debug_libnames "icu${component}d")
    if(component STREQUAL "data")
      list(APPEND component_libnames "icudt")
      # Note there is no debug variant at present
      list(APPEND component_debug_libnames "icudtd")
    endif()
    if(component STREQUAL "dt")
      list(APPEND component_libnames "icudata")
      # Note there is no debug variant at present
      list(APPEND component_debug_libnames "icudatad")
    endif()
    if(component STREQUAL "i18n")
      list(APPEND component_libnames "icuin")
      list(APPEND component_debug_libnames "icuind")
    endif()
    if(component STREQUAL "in")
      list(APPEND component_libnames "icui18n")
      list(APPEND component_debug_libnames "icui18nd")
    endif()

    find_library("${component_cache_release}" ${component_libnames}
      HINTS ${icu_roots}
      PATH_SUFFIXES ${icu_library_suffixes}
      DOC "ICU ${component} library (release)")
    find_library("${component_cache_debug}" ${component_debug_libnames}
      HINTS ${icu_roots}
      PATH_SUFFIXES ${icu_library_suffixes}
      DOC "ICU ${component} library (debug)")
    include(SelectLibraryConfigurations)
    select_library_configurations(ICU_${component_upcase})
    mark_as_advanced("${component_cache_release}" "${component_cache_debug}")
    if(${component_cache})
      set("${component_found}" ON)
      list(APPEND ICU_LIBRARY "${${component_cache}}")
    endif()
    mark_as_advanced("${component_found}")
    set("${component_cache}" "${${component_cache}}" PARENT_SCOPE)
    set("${component_found}" "${${component_found}}" PARENT_SCOPE)
    if(${component_found})
      if (ICU_FIND_REQUIRED_${component})
        list(APPEND ICU_LIBS_FOUND "${component} (required)")
      else()
        list(APPEND ICU_LIBS_FOUND "${component} (optional)")
      endif()
    else()
      if (ICU_FIND_REQUIRED_${component})
        set(ICU_REQUIRED_LIBS_FOUND OFF)
        list(APPEND ICU_LIBS_NOTFOUND "${component} (required)")
      else()
        list(APPEND ICU_LIBS_NOTFOUND "${component} (optional)")
      endif()
    endif()
  endforeach()
  set(_ICU_REQUIRED_LIBS_FOUND "${ICU_REQUIRED_LIBS_FOUND}" PARENT_SCOPE)
  set(ICU_LIBRARY "${ICU_LIBRARY}" PARENT_SCOPE)

  # Find all ICU data files
  if(CMAKE_LIBRARY_ARCHITECTURE)
    list(APPEND icu_data_suffixes
      "${_lib64}/${CMAKE_LIBRARY_ARCHITECTURE}/icu/${ICU_VERSION}"
      "lib/${CMAKE_LIBRARY_ARCHITECTURE}/icu/${ICU_VERSION}"
      "${_lib64}/${CMAKE_LIBRARY_ARCHITECTURE}/icu"
      "lib/${CMAKE_LIBRARY_ARCHITECTURE}/icu")
  endif()
  list(APPEND icu_data_suffixes
    "${_lib64}/icu/${ICU_VERSION}"
    "lib/icu/${ICU_VERSION}"
    "${_lib64}/icu"
    "lib/icu")
  foreach(data ${icu_data})
    string(TOUPPER "${data}" data_upcase)
    string(REPLACE "." "_" data_upcase "${data_upcase}")
    set(cache_var "ICU_${data_upcase}")
    set(data_var "ICU_${data_upcase}")
    find_file("${cache_var}" "${data}"
      HINTS ${icu_roots}
      PATH_SUFFIXES ${icu_data_suffixes}
      DOC "ICU ${data} data file")
    mark_as_advanced(cache_var)
    set("${data_var}" "${${cache_var}}" PARENT_SCOPE)
  endforeach()

  if(NOT ICU_FIND_QUIETLY)
    if(ICU_LIBS_FOUND)
      message(STATUS "Found the following ICU libraries:")
      foreach(found ${ICU_LIBS_FOUND})
        message(STATUS "  ${found}")
      endforeach()
    endif()
    if(ICU_LIBS_NOTFOUND)
      message(STATUS "The following ICU libraries were not found:")
      foreach(notfound ${ICU_LIBS_NOTFOUND})
        message(STATUS "  ${notfound}")
      endforeach()
    endif()
  endif()

  if(ICU_DEBUG)
    message(STATUS "--------FindICU.cmake search debug--------")
    message(STATUS "ICU binary path search order: ${icu_roots}")
    message(STATUS "ICU include path search order: ${icu_roots}")
    message(STATUS "ICU library path search order: ${icu_roots}")
    message(STATUS "----------------")
  endif()
endfunction()

_ICU_FIND()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(ICU
                                  FOUND_VAR ICU_FOUND
                                  REQUIRED_VARS ICU_INCLUDE_DIR
                                                ICU_LIBRARY
                                                _ICU_REQUIRED_LIBS_FOUND
                                  VERSION_VAR ICU_VERSION
                                  FAIL_MESSAGE "Failed to find all ICU components")

unset(_ICU_REQUIRED_LIBS_FOUND)

if(ICU_FOUND)
  set(ICU_INCLUDE_DIRS "${ICU_INCLUDE_DIR}")
  set(ICU_LIBRARIES "${ICU_LIBRARY}")
  foreach(_ICU_component ${ICU_FIND_COMPONENTS})
    string(TOUPPER "${_ICU_component}" _ICU_component_upcase)
    set(_ICU_component_cache "ICU_${_ICU_component_upcase}_LIBRARY")
    set(_ICU_component_cache_release "ICU_${_ICU_component_upcase}_LIBRARY_RELEASE")
    set(_ICU_component_cache_debug "ICU_${_ICU_component_upcase}_LIBRARY_DEBUG")
    set(_ICU_component_lib "ICU_${_ICU_component_upcase}_LIBRARIES")
    set(_ICU_component_found "${_ICU_component_upcase}_FOUND")
    set(_ICU_imported_target "ICU::${_ICU_component}")
    if(${_ICU_component_found})
      set("${_ICU_component_lib}" "${${_ICU_component_cache}}")
      if(NOT TARGET ${_ICU_imported_target})
        add_library(${_ICU_imported_target} UNKNOWN IMPORTED)
        if(ICU_INCLUDE_DIR)
          set_target_properties(${_ICU_imported_target} PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${ICU_INCLUDE_DIR}")
        endif()
        if(EXISTS "${${_ICU_component_cache}}")
          set_target_properties(${_ICU_imported_target} PROPERTIES
            IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
            IMPORTED_LOCATION "${${_ICU_component_cache}}")
        endif()
        if(EXISTS "${${_ICU_component_cache_release}}")
          set_property(TARGET ${_ICU_imported_target} APPEND PROPERTY
            IMPORTED_CONFIGURATIONS RELEASE)
          set_target_properties(${_ICU_imported_target} PROPERTIES
            IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
            IMPORTED_LOCATION_RELEASE "${${_ICU_component_cache_release}}")
        endif()
        if(EXISTS "${${_ICU_component_cache_debug}}")
          set_property(TARGET ${_ICU_imported_target} APPEND PROPERTY
            IMPORTED_CONFIGURATIONS DEBUG)
          set_target_properties(${_ICU_imported_target} PROPERTIES
            IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "CXX"
            IMPORTED_LOCATION_DEBUG "${${_ICU_component_cache_debug}}")
        endif()
      endif()
    endif()
    unset(_ICU_component_upcase)
    unset(_ICU_component_cache)
    unset(_ICU_component_lib)
    unset(_ICU_component_found)
    unset(_ICU_imported_target)
  endforeach()
endif()

if(ICU_DEBUG)
  message(STATUS "--------FindICU.cmake results debug--------")
  message(STATUS "ICU found: ${ICU_FOUND}")
  message(STATUS "ICU_VERSION number: ${ICU_VERSION}")
  message(STATUS "ICU_ROOT directory: ${ICU_ROOT}")
  message(STATUS "ICU_INCLUDE_DIR directory: ${ICU_INCLUDE_DIR}")
  message(STATUS "ICU_LIBRARIES: ${ICU_LIBRARIES}")

  foreach(program IN LISTS icu_programs)
    string(TOUPPER "${program}" program_upcase)
    set(program_lib "ICU_${program_upcase}_EXECUTABLE")
    message(STATUS "${program} program: ${${program_lib}}")
    unset(program_upcase)
    unset(program_lib)
  endforeach()

  foreach(data IN LISTS icu_data)
    string(TOUPPER "${data}" data_upcase)
    string(REPLACE "." "_" data_upcase "${data_upcase}")
    set(data_lib "ICU_${data_upcase}")
    message(STATUS "${data} data: ${${data_lib}}")
    unset(data_upcase)
    unset(data_lib)
  endforeach()

  foreach(component IN LISTS ICU_FIND_COMPONENTS)
    string(TOUPPER "${component}" component_upcase)
    set(component_lib "ICU_${component_upcase}_LIBRARIES")
    set(component_found "${component_upcase}_FOUND")
    message(STATUS "${component} library found: ${${component_found}}")
    message(STATUS "${component} library: ${${component_lib}}")
    unset(component_upcase)
    unset(component_lib)
    unset(component_found)
  endforeach()
  message(STATUS "----------------")
endif()

unset(icu_programs)
