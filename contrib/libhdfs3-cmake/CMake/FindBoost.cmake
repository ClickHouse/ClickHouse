# - Find Boost include dirs and libraries
# Use this module by invoking find_package with the form:
#  find_package(Boost
#    [version] [EXACT]      # Minimum or EXACT version e.g. 1.36.0
#    [REQUIRED]             # Fail with error if Boost is not found
#    [COMPONENTS <libs>...] # Boost libraries by their canonical name
#    )                      # e.g. "date_time" for "libboost_date_time"
# This module finds headers and requested component libraries OR a CMake
# package configuration file provided by a "Boost CMake" build.  For the
# latter case skip to the "Boost CMake" section below.  For the former
# case results are reported in variables:
#  Boost_FOUND            - True if headers and requested libraries were found
#  Boost_INCLUDE_DIRS     - Boost include directories
#  Boost_LIBRARY_DIRS     - Link directories for Boost libraries
#  Boost_LIBRARIES        - Boost component libraries to be linked
#  Boost_<C>_FOUND        - True if component <C> was found (<C> is upper-case)
#  Boost_<C>_LIBRARY      - Libraries to link for component <C> (may include
#                           target_link_libraries debug/optimized keywords)
#  Boost_VERSION          - BOOST_VERSION value from boost/version.hpp
#  Boost_LIB_VERSION      - Version string appended to library filenames
#  Boost_MAJOR_VERSION    - Boost major version number (X in X.y.z)
#  Boost_MINOR_VERSION    - Boost minor version number (Y in x.Y.z)
#  Boost_SUBMINOR_VERSION - Boost subminor version number (Z in x.y.Z)
#  Boost_LIB_DIAGNOSTIC_DEFINITIONS (Windows)
#                         - Pass to add_definitions() to have diagnostic
#                           information about Boost's automatic linking
#                           displayed during compilation
#
# This module reads hints about search locations from variables:
#  BOOST_ROOT             - Preferred installation prefix
#   (or BOOSTROOT)
#  BOOST_INCLUDEDIR       - Preferred include directory e.g. <prefix>/include
#  BOOST_LIBRARYDIR       - Preferred library directory e.g. <prefix>/lib
#  Boost_NO_SYSTEM_PATHS  - Set to ON to disable searching in locations not
#                           specified by these hint variables. Default is OFF.
#  Boost_ADDITIONAL_VERSIONS
#                         - List of Boost versions not known to this module
#                           (Boost install locations may contain the version)
# and saves search results persistently in CMake cache entries:
#  Boost_INCLUDE_DIR         - Directory containing Boost headers
#  Boost_LIBRARY_DIR         - Directory containing Boost libraries
#  Boost_<C>_LIBRARY_DEBUG   - Component <C> library debug variant
#  Boost_<C>_LIBRARY_RELEASE - Component <C> library release variant
# Users may set these hints or results as cache entries.  Projects should
# not read these entries directly but instead use the above result variables.
# Note that some hint names start in upper-case "BOOST".  One may specify
# these as environment variables if they are not specified as CMake variables
# or cache entries.
#
# This module first searches for the Boost header files using the above hint
# variables (excluding BOOST_LIBRARYDIR) and saves the result in
# Boost_INCLUDE_DIR.  Then it searches for requested component libraries using
# the above hints (excluding BOOST_INCLUDEDIR and Boost_ADDITIONAL_VERSIONS),
# "lib" directories near Boost_INCLUDE_DIR, and the library name configuration
# settings below.  It saves the library directory in Boost_LIBRARY_DIR and
# individual library locations in Boost_<C>_LIBRARY_DEBUG and
# Boost_<C>_LIBRARY_RELEASE.  When one changes settings used by previous
# searches in the same build tree (excluding environment variables) this
# module discards previous search results affected by the changes and searches
# again.
#
# Boost libraries come in many variants encoded in their file name.  Users or
# projects may tell this module which variant to find by setting variables:
#  Boost_USE_MULTITHREADED  - Set to OFF to use the non-multithreaded
#                             libraries ('mt' tag).  Default is ON.
#  Boost_USE_STATIC_LIBS    - Set to ON to force the use of the static
#                             libraries.  Default is OFF.
#  Boost_USE_STATIC_RUNTIME - Set to ON or OFF to specify whether to use
#                             libraries linked statically to the C++ runtime
#                             ('s' tag).  Default is platform dependent.
#  Boost_USE_DEBUG_PYTHON   - Set to ON to use libraries compiled with a
#                             debug Python build ('y' tag). Default is OFF.
#  Boost_USE_STLPORT        - Set to ON to use libraries compiled with
#                             STLPort ('p' tag).  Default is OFF.
#  Boost_USE_STLPORT_DEPRECATED_NATIVE_IOSTREAMS
#                           - Set to ON to use libraries compiled with
#                             STLPort deprecated "native iostreams"
#                             ('n' tag).  Default is OFF.
#  Boost_COMPILER           - Set to the compiler-specific library suffix
#                             (e.g. "-gcc43").  Default is auto-computed
#                             for the C++ compiler in use.
#  Boost_THREADAPI          - Suffix for "thread" component library name,
#                             such as "pthread" or "win32".  Names with
#                             and without this suffix will both be tried.
# Other variables one may set to control this module are:
#  Boost_DEBUG              - Set to ON to enable debug output from FindBoost.
#                             Please enable this before filing any bug report.
#  Boost_DETAILED_FAILURE_MSG
#                           - Set to ON to add detailed information to the
#                             failure message even when the REQUIRED option
#                             is not given to the find_package call.
#  Boost_REALPATH           - Set to ON to resolve symlinks for discovered
#                             libraries to assist with packaging.  For example,
#                             the "system" component library may be resolved to
#                             "/usr/lib/libboost_system.so.1.42.0" instead of
#                             "/usr/lib/libboost_system.so".  This does not
#                             affect linking and should not be enabled unless
#                             the user needs this information.
# On Visual Studio and Borland compilers Boost headers request automatic
# linking to corresponding libraries.  This requires matching libraries to be
# linked explicitly or available in the link library search path.  In this
# case setting Boost_USE_STATIC_LIBS to OFF may not achieve dynamic linking.
# Boost automatic linking typically requests static libraries with a few
# exceptions (such as Boost.Python).  Use
#  add_definitions(${Boost_LIB_DIAGNOSTIC_DEFINITIONS})
# to ask Boost to report information about automatic linking requests.
#
# Example to find Boost headers only:
#  find_package(Boost 1.36.0)
#  if(Boost_FOUND)
#    include_directories(${Boost_INCLUDE_DIRS})
#    add_executable(foo foo.cc)
#  endif()
# Example to find Boost headers and some libraries:
#  set(Boost_USE_STATIC_LIBS        ON)
#  set(Boost_USE_MULTITHREADED      ON)
#  set(Boost_USE_STATIC_RUNTIME    OFF)
#  find_package(Boost 1.36.0 COMPONENTS date_time filesystem system ...)
#  if(Boost_FOUND)
#    include_directories(${Boost_INCLUDE_DIRS})
#    add_executable(foo foo.cc)
#    target_link_libraries(foo ${Boost_LIBRARIES})
#  endif()
#
# Boost CMake ----------------------------------------------------------
#
# If Boost was built using the boost-cmake project it provides a package
# configuration file for use with find_package's Config mode.  This module
# looks for the package configuration file called BoostConfig.cmake or
# boost-config.cmake and stores the result in cache entry "Boost_DIR".  If
# found, the package configuration file is loaded and this module returns with
# no further action.  See documentation of the Boost CMake package
# configuration for details on what it provides.
#
# Set Boost_NO_BOOST_CMAKE to ON to disable the search for boost-cmake.

#=============================================================================
# Copyright 2006-2012 Kitware, Inc.
# Copyright 2006-2008 Andreas Schneider <mail@cynapses.org>
# Copyright 2007      Wengo
# Copyright 2007      Mike Jackson
# Copyright 2008      Andreas Pakulat <apaku@gmx.de>
# Copyright 2008-2012 Philip Lowman <philip@yhbt.com>
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file Copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the License for more information.
#=============================================================================
# (To distribute this file outside of CMake, substitute the full
#  License text for the above reference.)


#-------------------------------------------------------------------------------
# Before we go searching, check whether boost-cmake is available, unless the
# user specifically asked NOT to search for boost-cmake.
#
# If Boost_DIR is set, this behaves as any find_package call would. If not,
# it looks at BOOST_ROOT and BOOSTROOT to find Boost.
#
if (NOT Boost_NO_BOOST_CMAKE)
  # If Boost_DIR is not set, look for BOOSTROOT and BOOST_ROOT as alternatives,
  # since these are more conventional for Boost.
  if ("$ENV{Boost_DIR}" STREQUAL "")
    if (NOT "$ENV{BOOST_ROOT}" STREQUAL "")
      set(ENV{Boost_DIR} $ENV{BOOST_ROOT})
    elseif (NOT "$ENV{BOOSTROOT}" STREQUAL "")
      set(ENV{Boost_DIR} $ENV{BOOSTROOT})
    endif()
  endif()

  # Do the same find_package call but look specifically for the CMake version.
  # Note that args are passed in the Boost_FIND_xxxxx variables, so there is no
  # need to delegate them to this find_package call.
  find_package(Boost QUIET NO_MODULE)
  mark_as_advanced(Boost_DIR)

  # If we found boost-cmake, then we're done.  Print out what we found.
  # Otherwise let the rest of the module try to find it.
  if (Boost_FOUND)
    message("Boost ${Boost_FIND_VERSION} found.")
    if (Boost_FIND_COMPONENTS)
      message("Found Boost components:")
      message("   ${Boost_FIND_COMPONENTS}")
    endif()
    return()
  endif()
endif()


#-------------------------------------------------------------------------------
#  FindBoost functions & macros
#

############################################
#
# Check the existence of the libraries.
#
############################################
# This macro was taken directly from the FindQt4.cmake file that is included
# with the CMake distribution. This is NOT my work. All work was done by the
# original authors of the FindQt4.cmake file. Only minor modifications were
# made to remove references to Qt and make this file more generally applicable
# And ELSE/ENDIF pairs were removed for readability.
#########################################################################

macro(_Boost_ADJUST_LIB_VARS basename)
  if(Boost_INCLUDE_DIR )
    if(Boost_${basename}_LIBRARY_DEBUG AND Boost_${basename}_LIBRARY_RELEASE)
      # if the generator supports configuration types then set
      # optimized and debug libraries, or if the CMAKE_BUILD_TYPE has a value
      if(CMAKE_CONFIGURATION_TYPES OR CMAKE_BUILD_TYPE)
        set(Boost_${basename}_LIBRARY optimized ${Boost_${basename}_LIBRARY_RELEASE} debug ${Boost_${basename}_LIBRARY_DEBUG})
      else()
        # if there are no configuration types and CMAKE_BUILD_TYPE has no value
        # then just use the release libraries
        set(Boost_${basename}_LIBRARY ${Boost_${basename}_LIBRARY_RELEASE} )
      endif()
      # FIXME: This probably should be set for both cases
      set(Boost_${basename}_LIBRARIES optimized ${Boost_${basename}_LIBRARY_RELEASE} debug ${Boost_${basename}_LIBRARY_DEBUG})
    endif()

    # if only the release version was found, set the debug variable also to the release version
    if(Boost_${basename}_LIBRARY_RELEASE AND NOT Boost_${basename}_LIBRARY_DEBUG)
      set(Boost_${basename}_LIBRARY_DEBUG ${Boost_${basename}_LIBRARY_RELEASE})
      set(Boost_${basename}_LIBRARY       ${Boost_${basename}_LIBRARY_RELEASE})
      set(Boost_${basename}_LIBRARIES     ${Boost_${basename}_LIBRARY_RELEASE})
    endif()

    # if only the debug version was found, set the release variable also to the debug version
    if(Boost_${basename}_LIBRARY_DEBUG AND NOT Boost_${basename}_LIBRARY_RELEASE)
      set(Boost_${basename}_LIBRARY_RELEASE ${Boost_${basename}_LIBRARY_DEBUG})
      set(Boost_${basename}_LIBRARY         ${Boost_${basename}_LIBRARY_DEBUG})
      set(Boost_${basename}_LIBRARIES       ${Boost_${basename}_LIBRARY_DEBUG})
    endif()

    # If the debug & release library ends up being the same, omit the keywords
    if(${Boost_${basename}_LIBRARY_RELEASE} STREQUAL ${Boost_${basename}_LIBRARY_DEBUG})
      set(Boost_${basename}_LIBRARY   ${Boost_${basename}_LIBRARY_RELEASE} )
      set(Boost_${basename}_LIBRARIES ${Boost_${basename}_LIBRARY_RELEASE} )
    endif()

    if(Boost_${basename}_LIBRARY)
      set(Boost_${basename}_FOUND ON)
    endif()

  endif()
  # Make variables changeable to the advanced user
  mark_as_advanced(
      Boost_${basename}_LIBRARY_RELEASE
      Boost_${basename}_LIBRARY_DEBUG
  )
endmacro()

macro(_Boost_CHANGE_DETECT changed_var)
  set(${changed_var} 0)
  foreach(v ${ARGN})
    if(DEFINED _Boost_COMPONENTS_SEARCHED)
      if(${v})
        if(_${v}_LAST)
          string(COMPARE NOTEQUAL "${${v}}" "${_${v}_LAST}" _${v}_CHANGED)
        else()
          set(_${v}_CHANGED 1)
        endif()
      elseif(_${v}_LAST)
        set(_${v}_CHANGED 1)
      endif()
      if(_${v}_CHANGED)
        set(${changed_var} 1)
      endif()
    else()
      set(_${v}_CHANGED 0)
    endif()
  endforeach()
endmacro()

macro(_Boost_FIND_LIBRARY var)
  find_library(${var} ${ARGN})

  # If we found the first library save Boost_LIBRARY_DIR.
  if(${var} AND NOT Boost_LIBRARY_DIR)
    get_filename_component(_dir "${${var}}" PATH)
    set(Boost_LIBRARY_DIR "${_dir}" CACHE PATH "Boost library directory" FORCE)
  endif()

  # If Boost_LIBRARY_DIR is known then search only there.
  if(Boost_LIBRARY_DIR)
    set(_boost_LIBRARY_SEARCH_DIRS ${Boost_LIBRARY_DIR} NO_DEFAULT_PATH)
  endif()
endmacro()

#-------------------------------------------------------------------------------

#
# Runs compiler with "-dumpversion" and parses major/minor
# version with a regex.
#
function(_Boost_COMPILER_DUMPVERSION _OUTPUT_VERSION)

  exec_program(${CMAKE_CXX_COMPILER}
    ARGS ${CMAKE_CXX_COMPILER_ARG1} -dumpversion
    OUTPUT_VARIABLE _boost_COMPILER_VERSION
  )
  string(REGEX REPLACE "([0-9])\\.([0-9])(\\.[0-9])?" "\\1\\2"
    _boost_COMPILER_VERSION ${_boost_COMPILER_VERSION})

  set(${_OUTPUT_VERSION} ${_boost_COMPILER_VERSION} PARENT_SCOPE)
endfunction()

#
# Take a list of libraries with "thread" in it
# and prepend duplicates with "thread_${Boost_THREADAPI}"
# at the front of the list
#
function(_Boost_PREPEND_LIST_WITH_THREADAPI _output)
  set(_orig_libnames ${ARGN})
  string(REPLACE "thread" "thread_${Boost_THREADAPI}" _threadapi_libnames "${_orig_libnames}")
  set(${_output} ${_threadapi_libnames} ${_orig_libnames} PARENT_SCOPE)
endfunction()

#
# If a library is found, replace its cache entry with its REALPATH
#
function(_Boost_SWAP_WITH_REALPATH _library _docstring)
  if(${_library})
    get_filename_component(_boost_filepathreal ${${_library}} REALPATH)
    unset(${_library} CACHE)
    set(${_library} ${_boost_filepathreal} CACHE FILEPATH "${_docstring}")
  endif()
endfunction()

function(_Boost_CHECK_SPELLING _var)
  if(${_var})
    string(TOUPPER ${_var} _var_UC)
    message(FATAL_ERROR "ERROR: ${_var} is not the correct spelling.  The proper spelling is ${_var_UC}.")
  endif()
endfunction()

# Guesses Boost's compiler prefix used in built library names
# Returns the guess by setting the variable pointed to by _ret
function(_Boost_GUESS_COMPILER_PREFIX _ret)
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Intel"
      OR "${CMAKE_CXX_COMPILER}" MATCHES "icl"
      OR "${CMAKE_CXX_COMPILER}" MATCHES "icpc")
    if(WIN32)
      set (_boost_COMPILER "-iw")
    else()
      set (_boost_COMPILER "-il")
    endif()
  elseif (MSVC12)
    set(_boost_COMPILER "-vc120")
  elseif (MSVC11)
    set(_boost_COMPILER "-vc110")
  elseif (MSVC10)
    set(_boost_COMPILER "-vc100")
  elseif (MSVC90)
    set(_boost_COMPILER "-vc90")
  elseif (MSVC80)
    set(_boost_COMPILER "-vc80")
  elseif (MSVC71)
    set(_boost_COMPILER "-vc71")
  elseif (MSVC70) # Good luck!
    set(_boost_COMPILER "-vc7") # yes, this is correct
  elseif (MSVC60) # Good luck!
    set(_boost_COMPILER "-vc6") # yes, this is correct
  elseif (BORLAND)
    set(_boost_COMPILER "-bcb")
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "SunPro")
    set(_boost_COMPILER "-sw")
  elseif (MINGW)
    if(${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION} VERSION_LESS 1.34)
        set(_boost_COMPILER "-mgw") # no GCC version encoding prior to 1.34
    else()
      _Boost_COMPILER_DUMPVERSION(_boost_COMPILER_VERSION)
      set(_boost_COMPILER "-mgw${_boost_COMPILER_VERSION}")
    endif()
  elseif (UNIX)
    if (CMAKE_COMPILER_IS_GNUCXX)
      if(${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION} VERSION_LESS 1.34)
        set(_boost_COMPILER "-gcc") # no GCC version encoding prior to 1.34
      else()
        _Boost_COMPILER_DUMPVERSION(_boost_COMPILER_VERSION)
        # Determine which version of GCC we have.
        if(APPLE)
          if(Boost_MINOR_VERSION)
            if(${Boost_MINOR_VERSION} GREATER 35)
              # In Boost 1.36.0 and newer, the mangled compiler name used
              # on Mac OS X/Darwin is "xgcc".
              set(_boost_COMPILER "-xgcc${_boost_COMPILER_VERSION}")
            else()
              # In Boost <= 1.35.0, there is no mangled compiler name for
              # the Mac OS X/Darwin version of GCC.
              set(_boost_COMPILER "")
            endif()
          else()
            # We don't know the Boost version, so assume it's
            # pre-1.36.0.
            set(_boost_COMPILER "")
          endif()
        else()
          set(_boost_COMPILER "-gcc${_boost_COMPILER_VERSION}")
        endif()
      endif()
    endif ()
  else()
    # TODO at least Boost_DEBUG here?
    set(_boost_COMPILER "")
  endif()
  set(${_ret} ${_boost_COMPILER} PARENT_SCOPE)
endfunction()

#
# End functions/macros
#
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# main.
#-------------------------------------------------------------------------------

if(NOT DEFINED Boost_USE_MULTITHREADED)
    set(Boost_USE_MULTITHREADED TRUE)
endif()

# Check the version of Boost against the requested version.
if(Boost_FIND_VERSION AND NOT Boost_FIND_VERSION_MINOR)
  message(SEND_ERROR "When requesting a specific version of Boost, you must provide at least the major and minor version numbers, e.g., 1.34")
endif()

if(Boost_FIND_VERSION_EXACT)
  # The version may appear in a directory with or without the patch
  # level, even when the patch level is non-zero.
  set(_boost_TEST_VERSIONS
    "${Boost_FIND_VERSION_MAJOR}.${Boost_FIND_VERSION_MINOR}.${Boost_FIND_VERSION_PATCH}"
    "${Boost_FIND_VERSION_MAJOR}.${Boost_FIND_VERSION_MINOR}")
else()
  # The user has not requested an exact version.  Among known
  # versions, find those that are acceptable to the user request.
  set(_Boost_KNOWN_VERSIONS ${Boost_ADDITIONAL_VERSIONS}
    "1.56.0" "1.56" "1.55.0" "1.55" "1.54.0" "1.54"
    "1.53.0" "1.53" "1.52.0" "1.52" "1.51.0" "1.51"
    "1.50.0" "1.50" "1.49.0" "1.49" "1.48.0" "1.48" "1.47.0" "1.47" "1.46.1"
    "1.46.0" "1.46" "1.45.0" "1.45" "1.44.0" "1.44" "1.43.0" "1.43" "1.42.0" "1.42"
    "1.41.0" "1.41" "1.40.0" "1.40" "1.39.0" "1.39" "1.38.0" "1.38" "1.37.0" "1.37"
    "1.36.1" "1.36.0" "1.36" "1.35.1" "1.35.0" "1.35" "1.34.1" "1.34.0"
    "1.34" "1.33.1" "1.33.0" "1.33")
  set(_boost_TEST_VERSIONS)
  if(Boost_FIND_VERSION)
    set(_Boost_FIND_VERSION_SHORT "${Boost_FIND_VERSION_MAJOR}.${Boost_FIND_VERSION_MINOR}")
    # Select acceptable versions.
    foreach(version ${_Boost_KNOWN_VERSIONS})
      if(NOT "${version}" VERSION_LESS "${Boost_FIND_VERSION}")
        # This version is high enough.
        list(APPEND _boost_TEST_VERSIONS "${version}")
      elseif("${version}.99" VERSION_EQUAL "${_Boost_FIND_VERSION_SHORT}.99")
        # This version is a short-form for the requested version with
        # the patch level dropped.
        list(APPEND _boost_TEST_VERSIONS "${version}")
      endif()
    endforeach()
  else()
    # Any version is acceptable.
    set(_boost_TEST_VERSIONS "${_Boost_KNOWN_VERSIONS}")
  endif()
endif()

# The reason that we failed to find Boost. This will be set to a
# user-friendly message when we fail to find some necessary piece of
# Boost.
set(Boost_ERROR_REASON)

if(Boost_DEBUG)
  # Output some of their choices
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "_boost_TEST_VERSIONS = ${_boost_TEST_VERSIONS}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Boost_USE_MULTITHREADED = ${Boost_USE_MULTITHREADED}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Boost_USE_STATIC_LIBS = ${Boost_USE_STATIC_LIBS}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Boost_USE_STATIC_RUNTIME = ${Boost_USE_STATIC_RUNTIME}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Boost_ADDITIONAL_VERSIONS = ${Boost_ADDITIONAL_VERSIONS}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Boost_NO_SYSTEM_PATHS = ${Boost_NO_SYSTEM_PATHS}")
endif()

if(WIN32)
  # In windows, automatic linking is performed, so you do not have
  # to specify the libraries.  If you are linking to a dynamic
  # runtime, then you can choose to link to either a static or a
  # dynamic Boost library, the default is to do a static link.  You
  # can alter this for a specific library "whatever" by defining
  # BOOST_WHATEVER_DYN_LINK to force Boost library "whatever" to be
  # linked dynamically.  Alternatively you can force all Boost
  # libraries to dynamic link by defining BOOST_ALL_DYN_LINK.

  # This feature can be disabled for Boost library "whatever" by
  # defining BOOST_WHATEVER_NO_LIB, or for all of Boost by defining
  # BOOST_ALL_NO_LIB.

  # If you want to observe which libraries are being linked against
  # then defining BOOST_LIB_DIAGNOSTIC will cause the auto-linking
  # code to emit a #pragma message each time a library is selected
  # for linking.
  set(Boost_LIB_DIAGNOSTIC_DEFINITIONS "-DBOOST_LIB_DIAGNOSTIC")
endif()

_Boost_CHECK_SPELLING(Boost_ROOT)
_Boost_CHECK_SPELLING(Boost_LIBRARYDIR)
_Boost_CHECK_SPELLING(Boost_INCLUDEDIR)

# Collect environment variable inputs as hints.  Do not consider changes.
foreach(v BOOSTROOT BOOST_ROOT BOOST_INCLUDEDIR BOOST_LIBRARYDIR)
  set(_env $ENV{${v}})
  if(_env)
    file(TO_CMAKE_PATH "${_env}" _ENV_${v})
  else()
    set(_ENV_${v} "")
  endif()
endforeach()
if(NOT _ENV_BOOST_ROOT AND _ENV_BOOSTROOT)
  set(_ENV_BOOST_ROOT "${_ENV_BOOSTROOT}")
endif()

# Collect inputs and cached results.  Detect changes since the last run.
if(NOT BOOST_ROOT AND BOOSTROOT)
  set(BOOST_ROOT "${BOOSTROOT}")
endif()
set(_Boost_VARS_DIR
  BOOST_ROOT
  Boost_NO_SYSTEM_PATHS
  )

if(Boost_DEBUG)
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "Declared as CMake or Environmental Variables:")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "  BOOST_ROOT = ${BOOST_ROOT}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "  BOOST_INCLUDEDIR = ${BOOST_INCLUDEDIR}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "  BOOST_LIBRARYDIR = ${BOOST_LIBRARYDIR}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                 "_boost_TEST_VERSIONS = ${_boost_TEST_VERSIONS}")
endif()

# ------------------------------------------------------------------------
#  Search for Boost include DIR
# ------------------------------------------------------------------------

set(_Boost_VARS_INC BOOST_INCLUDEDIR Boost_INCLUDE_DIR Boost_ADDITIONAL_VERSIONS)
_Boost_CHANGE_DETECT(_Boost_CHANGE_INCDIR ${_Boost_VARS_DIR} ${_Boost_VARS_INC})
# Clear Boost_INCLUDE_DIR if it did not change but other input affecting the
# location did.  We will find a new one based on the new inputs.
if(_Boost_CHANGE_INCDIR AND NOT _Boost_INCLUDE_DIR_CHANGED)
  unset(Boost_INCLUDE_DIR CACHE)
endif()

if(NOT Boost_INCLUDE_DIR)
  set(_boost_INCLUDE_SEARCH_DIRS "")
  if(BOOST_INCLUDEDIR)
    list(APPEND _boost_INCLUDE_SEARCH_DIRS ${BOOST_INCLUDEDIR})
  elseif(_ENV_BOOST_INCLUDEDIR)
    list(APPEND _boost_INCLUDE_SEARCH_DIRS ${_ENV_BOOST_INCLUDEDIR})
  endif()

  if( BOOST_ROOT )
    list(APPEND _boost_INCLUDE_SEARCH_DIRS ${BOOST_ROOT}/include ${BOOST_ROOT})
  elseif( _ENV_BOOST_ROOT )
    list(APPEND _boost_INCLUDE_SEARCH_DIRS ${_ENV_BOOST_ROOT}/include ${_ENV_BOOST_ROOT})
  endif()

  if( Boost_NO_SYSTEM_PATHS)
    list(APPEND _boost_INCLUDE_SEARCH_DIRS NO_CMAKE_SYSTEM_PATH)
  else()
    list(APPEND _boost_INCLUDE_SEARCH_DIRS PATHS
      C:/boost/include
      C:/boost
      /sw/local/include
      )
  endif()

  # Try to find Boost by stepping backwards through the Boost versions
  # we know about.
  # Build a list of path suffixes for each version.
  set(_boost_PATH_SUFFIXES)
  foreach(_boost_VER ${_boost_TEST_VERSIONS})
    # Add in a path suffix, based on the required version, ideally
    # we could read this from version.hpp, but for that to work we'd
    # need to know the include dir already
    set(_boost_BOOSTIFIED_VERSION)

    # Transform 1.35 => 1_35 and 1.36.0 => 1_36_0
    if(_boost_VER MATCHES "[0-9]+\\.[0-9]+\\.[0-9]+")
        string(REGEX REPLACE "([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\1_\\2_\\3"
          _boost_BOOSTIFIED_VERSION ${_boost_VER})
    elseif(_boost_VER MATCHES "[0-9]+\\.[0-9]+")
        string(REGEX REPLACE "([0-9]+)\\.([0-9]+)" "\\1_\\2"
          _boost_BOOSTIFIED_VERSION ${_boost_VER})
    endif()

    list(APPEND _boost_PATH_SUFFIXES
      "boost-${_boost_BOOSTIFIED_VERSION}"
      "boost_${_boost_BOOSTIFIED_VERSION}"
      "boost/boost-${_boost_BOOSTIFIED_VERSION}"
      "boost/boost_${_boost_BOOSTIFIED_VERSION}"
      )

  endforeach()

  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "Include debugging info:")
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "  _boost_INCLUDE_SEARCH_DIRS = ${_boost_INCLUDE_SEARCH_DIRS}")
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "  _boost_PATH_SUFFIXES = ${_boost_PATH_SUFFIXES}")
  endif()

  # Look for a standard boost header file.
  find_path(Boost_INCLUDE_DIR
    NAMES         boost/config.hpp
    HINTS         ${_boost_INCLUDE_SEARCH_DIRS}
    PATH_SUFFIXES ${_boost_PATH_SUFFIXES}
    )
endif()

# ------------------------------------------------------------------------
#  Extract version information from version.hpp
# ------------------------------------------------------------------------

# Set Boost_FOUND based only on header location and version.
# It will be updated below for component libraries.
if(Boost_INCLUDE_DIR)
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "location of version.hpp: ${Boost_INCLUDE_DIR}/boost/version.hpp")
  endif()

  # Extract Boost_VERSION and Boost_LIB_VERSION from version.hpp
  set(Boost_VERSION 0)
  set(Boost_LIB_VERSION "")
  file(STRINGS "${Boost_INCLUDE_DIR}/boost/version.hpp" _boost_VERSION_HPP_CONTENTS REGEX "#define BOOST_(LIB_)?VERSION ")
  set(_Boost_VERSION_REGEX "([0-9]+)")
  set(_Boost_LIB_VERSION_REGEX "\"([0-9_]+)\"")
  foreach(v VERSION LIB_VERSION)
    if("${_boost_VERSION_HPP_CONTENTS}" MATCHES ".*#define BOOST_${v} ${_Boost_${v}_REGEX}.*")
      set(Boost_${v} "${CMAKE_MATCH_1}")
    endif()
  endforeach()
  unset(_boost_VERSION_HPP_CONTENTS)

  math(EXPR Boost_MAJOR_VERSION "${Boost_VERSION} / 100000")
  math(EXPR Boost_MINOR_VERSION "${Boost_VERSION} / 100 % 1000")
  math(EXPR Boost_SUBMINOR_VERSION "${Boost_VERSION} % 100")

  set(Boost_ERROR_REASON
    "${Boost_ERROR_REASON}Boost version: ${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}\nBoost include path: ${Boost_INCLUDE_DIR}")
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "version.hpp reveals boost "
                   "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
  endif()

  if(Boost_FIND_VERSION)
    # Set Boost_FOUND based on requested version.
    set(_Boost_VERSION "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
    if("${_Boost_VERSION}" VERSION_LESS "${Boost_FIND_VERSION}")
      set(Boost_FOUND 0)
      set(_Boost_VERSION_AGE "old")
    elseif(Boost_FIND_VERSION_EXACT AND
        NOT "${_Boost_VERSION}" VERSION_EQUAL "${Boost_FIND_VERSION}")
      set(Boost_FOUND 0)
      set(_Boost_VERSION_AGE "new")
    else()
      set(Boost_FOUND 1)
    endif()
    if(NOT Boost_FOUND)
      # State that we found a version of Boost that is too new or too old.
      set(Boost_ERROR_REASON
        "${Boost_ERROR_REASON}\nDetected version of Boost is too ${_Boost_VERSION_AGE}. Requested version was ${Boost_FIND_VERSION_MAJOR}.${Boost_FIND_VERSION_MINOR}")
      if (Boost_FIND_VERSION_PATCH)
        set(Boost_ERROR_REASON
          "${Boost_ERROR_REASON}.${Boost_FIND_VERSION_PATCH}")
      endif ()
      if (NOT Boost_FIND_VERSION_EXACT)
        set(Boost_ERROR_REASON "${Boost_ERROR_REASON} (or newer)")
      endif ()
      set(Boost_ERROR_REASON "${Boost_ERROR_REASON}.")
    endif ()
  else()
    # Caller will accept any Boost version.
    set(Boost_FOUND 1)
  endif()
else()
  set(Boost_FOUND 0)
  set(Boost_ERROR_REASON
    "${Boost_ERROR_REASON}Unable to find the Boost header files. Please set BOOST_ROOT to the root directory containing Boost or BOOST_INCLUDEDIR to the directory containing Boost's headers.")
endif()

# ------------------------------------------------------------------------
#  Suffix initialization and compiler suffix detection.
# ------------------------------------------------------------------------

set(_Boost_VARS_NAME
  Boost_COMPILER
  Boost_THREADAPI
  Boost_USE_DEBUG_PYTHON
  Boost_USE_MULTITHREADED
  Boost_USE_STATIC_LIBS
  Boost_USE_STATIC_RUNTIME
  Boost_USE_STLPORT
  Boost_USE_STLPORT_DEPRECATED_NATIVE_IOSTREAMS
  )
_Boost_CHANGE_DETECT(_Boost_CHANGE_LIBNAME ${_Boost_VARS_NAME})

# Setting some more suffixes for the library
set(Boost_LIB_PREFIX "")
if ( WIN32 AND Boost_USE_STATIC_LIBS AND NOT CYGWIN)
  set(Boost_LIB_PREFIX "lib")
endif()

if (Boost_COMPILER)
  set(_boost_COMPILER ${Boost_COMPILER})
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "using user-specified Boost_COMPILER = ${_boost_COMPILER}")
  endif()
else()
  # Attempt to guess the compiler suffix
  # NOTE: this is not perfect yet, if you experience any issues
  # please report them and use the Boost_COMPILER variable
  # to work around the problems.
  _Boost_GUESS_COMPILER_PREFIX(_boost_COMPILER)
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
      "guessed _boost_COMPILER = ${_boost_COMPILER}")
  endif()
endif()

set (_boost_MULTITHREADED "-mt")
if( NOT Boost_USE_MULTITHREADED )
  set (_boost_MULTITHREADED "")
endif()
if(Boost_DEBUG)
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
    "_boost_MULTITHREADED = ${_boost_MULTITHREADED}")
endif()

#======================
# Systematically build up the Boost ABI tag
# http://boost.org/doc/libs/1_41_0/more/getting_started/windows.html#library-naming
set( _boost_RELEASE_ABI_TAG "-")
set( _boost_DEBUG_ABI_TAG   "-")
# Key       Use this library when:
#  s        linking statically to the C++ standard library and
#           compiler runtime support libraries.
if(Boost_USE_STATIC_RUNTIME)
  set( _boost_RELEASE_ABI_TAG "${_boost_RELEASE_ABI_TAG}s")
  set( _boost_DEBUG_ABI_TAG   "${_boost_DEBUG_ABI_TAG}s")
endif()
#  g        using debug versions of the standard and runtime
#           support libraries
if(WIN32)
  if(MSVC OR "${CMAKE_CXX_COMPILER}" MATCHES "icl"
          OR "${CMAKE_CXX_COMPILER}" MATCHES "icpc")
    set(_boost_DEBUG_ABI_TAG "${_boost_DEBUG_ABI_TAG}g")
  endif()
endif()
#  y        using special debug build of python
if(Boost_USE_DEBUG_PYTHON)
  set(_boost_DEBUG_ABI_TAG "${_boost_DEBUG_ABI_TAG}y")
endif()
#  d        using a debug version of your code
set(_boost_DEBUG_ABI_TAG "${_boost_DEBUG_ABI_TAG}d")
#  p        using the STLport standard library rather than the
#           default one supplied with your compiler
if(Boost_USE_STLPORT)
  set( _boost_RELEASE_ABI_TAG "${_boost_RELEASE_ABI_TAG}p")
  set( _boost_DEBUG_ABI_TAG   "${_boost_DEBUG_ABI_TAG}p")
endif()
#  n        using the STLport deprecated "native iostreams" feature
if(Boost_USE_STLPORT_DEPRECATED_NATIVE_IOSTREAMS)
  set( _boost_RELEASE_ABI_TAG "${_boost_RELEASE_ABI_TAG}n")
  set( _boost_DEBUG_ABI_TAG   "${_boost_DEBUG_ABI_TAG}n")
endif()

if(Boost_DEBUG)
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
    "_boost_RELEASE_ABI_TAG = ${_boost_RELEASE_ABI_TAG}")
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
    "_boost_DEBUG_ABI_TAG = ${_boost_DEBUG_ABI_TAG}")
endif()

# ------------------------------------------------------------------------
#  Begin finding boost libraries
# ------------------------------------------------------------------------
set(_Boost_VARS_LIB BOOST_LIBRARYDIR Boost_LIBRARY_DIR)
_Boost_CHANGE_DETECT(_Boost_CHANGE_LIBDIR ${_Boost_VARS_DIR} ${_Boost_VARS_LIB} Boost_INCLUDE_DIR)
# Clear Boost_LIBRARY_DIR if it did not change but other input affecting the
# location did.  We will find a new one based on the new inputs.
if(_Boost_CHANGE_LIBDIR AND NOT _Boost_LIBRARY_DIR_CHANGED)
  unset(Boost_LIBRARY_DIR CACHE)
endif()

if(Boost_LIBRARY_DIR)
  set(_boost_LIBRARY_SEARCH_DIRS ${Boost_LIBRARY_DIR} NO_DEFAULT_PATH)
else()
  set(_boost_LIBRARY_SEARCH_DIRS "")
  if(BOOST_LIBRARYDIR)
    list(APPEND _boost_LIBRARY_SEARCH_DIRS ${BOOST_LIBRARYDIR})
  elseif(_ENV_BOOST_LIBRARYDIR)
    list(APPEND _boost_LIBRARY_SEARCH_DIRS ${_ENV_BOOST_LIBRARYDIR})
  endif()

  if(BOOST_ROOT)
    list(APPEND _boost_LIBRARY_SEARCH_DIRS ${BOOST_ROOT}/lib ${BOOST_ROOT}/stage/lib)
  elseif(_ENV_BOOST_ROOT)
    list(APPEND _boost_LIBRARY_SEARCH_DIRS ${_ENV_BOOST_ROOT}/lib ${_ENV_BOOST_ROOT}/stage/lib)
  endif()

  list(APPEND _boost_LIBRARY_SEARCH_DIRS
    ${Boost_INCLUDE_DIR}/lib
    ${Boost_INCLUDE_DIR}/../lib
    ${Boost_INCLUDE_DIR}/stage/lib
    )
  if( Boost_NO_SYSTEM_PATHS )
    list(APPEND _boost_LIBRARY_SEARCH_DIRS NO_CMAKE_SYSTEM_PATH)
  else()
    list(APPEND _boost_LIBRARY_SEARCH_DIRS PATHS
      C:/boost/lib
      C:/boost
      /sw/local/lib
      )
  endif()
endif()

if(Boost_DEBUG)
  message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
    "_boost_LIBRARY_SEARCH_DIRS = ${_boost_LIBRARY_SEARCH_DIRS}")
endif()

# Support preference of static libs by adjusting CMAKE_FIND_LIBRARY_SUFFIXES
if( Boost_USE_STATIC_LIBS )
  set( _boost_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
  if(WIN32)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
  else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a )
  endif()
endif()

# We want to use the tag inline below without risking double dashes
if(_boost_RELEASE_ABI_TAG)
  if(${_boost_RELEASE_ABI_TAG} STREQUAL "-")
    set(_boost_RELEASE_ABI_TAG "")
  endif()
endif()
if(_boost_DEBUG_ABI_TAG)
  if(${_boost_DEBUG_ABI_TAG} STREQUAL "-")
    set(_boost_DEBUG_ABI_TAG "")
  endif()
endif()

# The previous behavior of FindBoost when Boost_USE_STATIC_LIBS was enabled
# on WIN32 was to:
#  1. Search for static libs compiled against a SHARED C++ standard runtime library (use if found)
#  2. Search for static libs compiled against a STATIC C++ standard runtime library (use if found)
# We maintain this behavior since changing it could break people's builds.
# To disable the ambiguous behavior, the user need only
# set Boost_USE_STATIC_RUNTIME either ON or OFF.
set(_boost_STATIC_RUNTIME_WORKAROUND false)
if(WIN32 AND Boost_USE_STATIC_LIBS)
  if(NOT DEFINED Boost_USE_STATIC_RUNTIME)
    set(_boost_STATIC_RUNTIME_WORKAROUND true)
  endif()
endif()

# On versions < 1.35, remove the System library from the considered list
# since it wasn't added until 1.35.
if(Boost_VERSION AND Boost_FIND_COMPONENTS)
   if(Boost_VERSION LESS 103500)
     list(REMOVE_ITEM Boost_FIND_COMPONENTS system)
   endif()
endif()

# If the user changed any of our control inputs flush previous results.
if(_Boost_CHANGE_LIBDIR OR _Boost_CHANGE_LIBNAME)
  foreach(COMPONENT ${_Boost_COMPONENTS_SEARCHED})
    string(TOUPPER ${COMPONENT} UPPERCOMPONENT)
    foreach(c DEBUG RELEASE)
      set(_var Boost_${UPPERCOMPONENT}_LIBRARY_${c})
      unset(${_var} CACHE)
      set(${_var} "${_var}-NOTFOUND")
    endforeach()
  endforeach()
  set(_Boost_COMPONENTS_SEARCHED "")
endif()

foreach(COMPONENT ${Boost_FIND_COMPONENTS})
  string(TOUPPER ${COMPONENT} UPPERCOMPONENT)

  set( _boost_docstring_release "Boost ${COMPONENT} library (release)")
  set( _boost_docstring_debug   "Boost ${COMPONENT} library (debug)")

  #
  # Find RELEASE libraries
  #
  set(_boost_RELEASE_NAMES
    ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_COMPILER}${_boost_MULTITHREADED}${_boost_RELEASE_ABI_TAG}-${Boost_LIB_VERSION}
    ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_COMPILER}${_boost_MULTITHREADED}${_boost_RELEASE_ABI_TAG}
    ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_MULTITHREADED}${_boost_RELEASE_ABI_TAG}-${Boost_LIB_VERSION}
    ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_MULTITHREADED}${_boost_RELEASE_ABI_TAG}
    ${Boost_LIB_PREFIX}boost_${COMPONENT} )
  if(_boost_STATIC_RUNTIME_WORKAROUND)
    set(_boost_RELEASE_STATIC_ABI_TAG "-s${_boost_RELEASE_ABI_TAG}")
    list(APPEND _boost_RELEASE_NAMES
      ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_COMPILER}${_boost_MULTITHREADED}${_boost_RELEASE_STATIC_ABI_TAG}-${Boost_LIB_VERSION}
      ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_COMPILER}${_boost_MULTITHREADED}${_boost_RELEASE_STATIC_ABI_TAG}
      ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_MULTITHREADED}${_boost_RELEASE_STATIC_ABI_TAG}-${Boost_LIB_VERSION}
      ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_MULTITHREADED}${_boost_RELEASE_STATIC_ABI_TAG} )
  endif()
  if(Boost_THREADAPI AND ${COMPONENT} STREQUAL "thread")
     _Boost_PREPEND_LIST_WITH_THREADAPI(_boost_RELEASE_NAMES ${_boost_RELEASE_NAMES})
  endif()
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "Searching for ${UPPERCOMPONENT}_LIBRARY_RELEASE: ${_boost_RELEASE_NAMES}")
  endif()

  # Avoid passing backslashes to _Boost_FIND_LIBRARY due to macro re-parsing.
  string(REPLACE "\\" "/" _boost_LIBRARY_SEARCH_DIRS_tmp "${_boost_LIBRARY_SEARCH_DIRS}")

  _Boost_FIND_LIBRARY(Boost_${UPPERCOMPONENT}_LIBRARY_RELEASE
    NAMES ${_boost_RELEASE_NAMES}
    HINTS ${_boost_LIBRARY_SEARCH_DIRS_tmp}
    NAMES_PER_DIR
    DOC "${_boost_docstring_release}"
    )

  #
  # Find DEBUG libraries
  #
  set(_boost_DEBUG_NAMES
    ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_COMPILER}${_boost_MULTITHREADED}${_boost_DEBUG_ABI_TAG}-${Boost_LIB_VERSION}
    ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_COMPILER}${_boost_MULTITHREADED}${_boost_DEBUG_ABI_TAG}
    ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_MULTITHREADED}${_boost_DEBUG_ABI_TAG}-${Boost_LIB_VERSION}
    ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_MULTITHREADED}${_boost_DEBUG_ABI_TAG}
    ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_MULTITHREADED}
    ${Boost_LIB_PREFIX}boost_${COMPONENT} )
  if(_boost_STATIC_RUNTIME_WORKAROUND)
    set(_boost_DEBUG_STATIC_ABI_TAG "-s${_boost_DEBUG_ABI_TAG}")
    list(APPEND _boost_DEBUG_NAMES
      ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_COMPILER}${_boost_MULTITHREADED}${_boost_DEBUG_STATIC_ABI_TAG}-${Boost_LIB_VERSION}
      ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_COMPILER}${_boost_MULTITHREADED}${_boost_DEBUG_STATIC_ABI_TAG}
      ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_MULTITHREADED}${_boost_DEBUG_STATIC_ABI_TAG}-${Boost_LIB_VERSION}
      ${Boost_LIB_PREFIX}boost_${COMPONENT}${_boost_MULTITHREADED}${_boost_DEBUG_STATIC_ABI_TAG} )
  endif()
  if(Boost_THREADAPI AND ${COMPONENT} STREQUAL "thread")
     _Boost_PREPEND_LIST_WITH_THREADAPI(_boost_DEBUG_NAMES ${_boost_DEBUG_NAMES})
  endif()
  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] "
                   "Searching for ${UPPERCOMPONENT}_LIBRARY_DEBUG: ${_boost_DEBUG_NAMES}")
  endif()

  # Avoid passing backslashes to _Boost_FIND_LIBRARY due to macro re-parsing.
  string(REPLACE "\\" "/" _boost_LIBRARY_SEARCH_DIRS_tmp "${_boost_LIBRARY_SEARCH_DIRS}")

  _Boost_FIND_LIBRARY(Boost_${UPPERCOMPONENT}_LIBRARY_DEBUG
    NAMES ${_boost_DEBUG_NAMES}
    HINTS ${_boost_LIBRARY_SEARCH_DIRS_tmp}
    NAMES_PER_DIR
    DOC "${_boost_docstring_debug}"
    )

  if(Boost_REALPATH)
    _Boost_SWAP_WITH_REALPATH(Boost_${UPPERCOMPONENT}_LIBRARY_RELEASE "${_boost_docstring_release}")
    _Boost_SWAP_WITH_REALPATH(Boost_${UPPERCOMPONENT}_LIBRARY_DEBUG   "${_boost_docstring_debug}"  )
  endif()

  _Boost_ADJUST_LIB_VARS(${UPPERCOMPONENT})

endforeach()

# Restore the original find library ordering
if( Boost_USE_STATIC_LIBS )
  set(CMAKE_FIND_LIBRARY_SUFFIXES ${_boost_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
endif()

# ------------------------------------------------------------------------
#  End finding boost libraries
# ------------------------------------------------------------------------

set(Boost_INCLUDE_DIRS ${Boost_INCLUDE_DIR})
set(Boost_LIBRARY_DIRS ${Boost_LIBRARY_DIR})

# The above setting of Boost_FOUND was based only on the header files.
# Update it for the requested component libraries.
if(Boost_FOUND)
  # The headers were found.  Check for requested component libs.
  set(_boost_CHECKED_COMPONENT FALSE)
  set(_Boost_MISSING_COMPONENTS "")
  foreach(COMPONENT ${Boost_FIND_COMPONENTS})
    string(TOUPPER ${COMPONENT} COMPONENT)
    set(_boost_CHECKED_COMPONENT TRUE)
    if(NOT Boost_${COMPONENT}_FOUND)
      string(TOLOWER ${COMPONENT} COMPONENT)
      list(APPEND _Boost_MISSING_COMPONENTS ${COMPONENT})
    endif()
  endforeach()

  if(Boost_DEBUG)
    message(STATUS "[ ${CMAKE_CURRENT_LIST_FILE}:${CMAKE_CURRENT_LIST_LINE} ] Boost_FOUND = ${Boost_FOUND}")
  endif()

  if (_Boost_MISSING_COMPONENTS)
    set(Boost_FOUND 0)
    # We were unable to find some libraries, so generate a sensible
    # error message that lists the libraries we were unable to find.
    set(Boost_ERROR_REASON
      "${Boost_ERROR_REASON}\nCould not find the following")
    if(Boost_USE_STATIC_LIBS)
      set(Boost_ERROR_REASON "${Boost_ERROR_REASON} static")
    endif()
    set(Boost_ERROR_REASON
      "${Boost_ERROR_REASON} Boost libraries:\n")
    foreach(COMPONENT ${_Boost_MISSING_COMPONENTS})
      set(Boost_ERROR_REASON
        "${Boost_ERROR_REASON}        boost_${COMPONENT}\n")
    endforeach()

    list(LENGTH Boost_FIND_COMPONENTS Boost_NUM_COMPONENTS_WANTED)
    list(LENGTH _Boost_MISSING_COMPONENTS Boost_NUM_MISSING_COMPONENTS)
    if (${Boost_NUM_COMPONENTS_WANTED} EQUAL ${Boost_NUM_MISSING_COMPONENTS})
      set(Boost_ERROR_REASON
        "${Boost_ERROR_REASON}No Boost libraries were found. You may need to set BOOST_LIBRARYDIR to the directory containing Boost libraries or BOOST_ROOT to the location of Boost.")
    else ()
      set(Boost_ERROR_REASON
        "${Boost_ERROR_REASON}Some (but not all) of the required Boost libraries were found. You may need to install these additional Boost libraries. Alternatively, set BOOST_LIBRARYDIR to the directory containing Boost libraries or BOOST_ROOT to the location of Boost.")
    endif ()
  endif ()

  if( NOT Boost_LIBRARY_DIRS AND NOT _boost_CHECKED_COMPONENT )
    # Compatibility Code for backwards compatibility with CMake
    # 2.4's FindBoost module.

    # Look for the boost library path.
    # Note that the user may not have installed any libraries
    # so it is quite possible the Boost_LIBRARY_DIRS may not exist.
    set(_boost_LIB_DIR ${Boost_INCLUDE_DIR})

    if("${_boost_LIB_DIR}" MATCHES "boost-[0-9]+")
      get_filename_component(_boost_LIB_DIR ${_boost_LIB_DIR} PATH)
    endif()

    if("${_boost_LIB_DIR}" MATCHES "/include$")
      # Strip off the trailing "/include" in the path.
      get_filename_component(_boost_LIB_DIR ${_boost_LIB_DIR} PATH)
    endif()

    if(EXISTS "${_boost_LIB_DIR}/lib")
      set(_boost_LIB_DIR ${_boost_LIB_DIR}/lib)
    else()
      if(EXISTS "${_boost_LIB_DIR}/stage/lib")
        set(_boost_LIB_DIR ${_boost_LIB_DIR}/stage/lib)
      else()
        set(_boost_LIB_DIR "")
      endif()
    endif()

    if(_boost_LIB_DIR AND EXISTS "${_boost_LIB_DIR}")
      set(Boost_LIBRARY_DIRS ${_boost_LIB_DIR})
    endif()

  endif()
else()
  # Boost headers were not found so no components were found.
  foreach(COMPONENT ${Boost_FIND_COMPONENTS})
    string(TOUPPER ${COMPONENT} UPPERCOMPONENT)
    set(Boost_${UPPERCOMPONENT}_FOUND 0)
  endforeach()
endif()

# ------------------------------------------------------------------------
#  Notification to end user about what was found
# ------------------------------------------------------------------------

set(Boost_LIBRARIES "")
if(Boost_FOUND)
  if(NOT Boost_FIND_QUIETLY)
    message(STATUS "Boost version: ${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
    if(Boost_FIND_COMPONENTS)
      message(STATUS "Found the following Boost libraries:")
    endif()
  endif()
  foreach( COMPONENT  ${Boost_FIND_COMPONENTS} )
    string( TOUPPER ${COMPONENT} UPPERCOMPONENT )
    if( Boost_${UPPERCOMPONENT}_FOUND )
      if(NOT Boost_FIND_QUIETLY)
        message (STATUS "  ${COMPONENT}")
      endif()
      list(APPEND Boost_LIBRARIES ${Boost_${UPPERCOMPONENT}_LIBRARY})
    endif()
  endforeach()
else()
  if(Boost_FIND_REQUIRED)
    message(SEND_ERROR "Unable to find the requested Boost libraries.\n${Boost_ERROR_REASON}")
  else()
    if(NOT Boost_FIND_QUIETLY)
      # we opt not to automatically output Boost_ERROR_REASON here as
      # it could be quite lengthy and somewhat imposing in its requests
      # Since Boost is not always a required dependency we'll leave this
      # up to the end-user.
      if(Boost_DEBUG OR Boost_DETAILED_FAILURE_MSG)
        message(STATUS "Could NOT find Boost\n${Boost_ERROR_REASON}")
      else()
        message(STATUS "Could NOT find Boost")
      endif()
    endif()
  endif()
endif()

# Configure display of cache entries in GUI.
foreach(v BOOSTROOT BOOST_ROOT ${_Boost_VARS_INC} ${_Boost_VARS_LIB})
  get_property(_type CACHE ${v} PROPERTY TYPE)
  if(_type)
    set_property(CACHE ${v} PROPERTY ADVANCED 1)
    if("x${_type}" STREQUAL "xUNINITIALIZED")
      if("x${v}" STREQUAL "xBoost_ADDITIONAL_VERSIONS")
        set_property(CACHE ${v} PROPERTY TYPE STRING)
      else()
        set_property(CACHE ${v} PROPERTY TYPE PATH)
      endif()
    endif()
  endif()
endforeach()

# Record last used values of input variables so we can
# detect on the next run if the user changed them.
foreach(v
    ${_Boost_VARS_INC} ${_Boost_VARS_LIB}
    ${_Boost_VARS_DIR} ${_Boost_VARS_NAME}
    )
  if(DEFINED ${v})
    set(_${v}_LAST "${${v}}" CACHE INTERNAL "Last used ${v} value.")
  else()
    unset(_${v}_LAST CACHE)
  endif()
endforeach()

# Maintain a persistent list of components requested anywhere since
# the last flush.
set(_Boost_COMPONENTS_SEARCHED "${_Boost_COMPONENTS_SEARCHED}")
list(APPEND _Boost_COMPONENTS_SEARCHED ${Boost_FIND_COMPONENTS})
list(REMOVE_DUPLICATES _Boost_COMPONENTS_SEARCHED)
list(SORT _Boost_COMPONENTS_SEARCHED)
set(_Boost_COMPONENTS_SEARCHED "${_Boost_COMPONENTS_SEARCHED}"
  CACHE INTERNAL "Components requested for this build tree.")
