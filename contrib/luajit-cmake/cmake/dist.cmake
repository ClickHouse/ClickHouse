# LuaDist CMake utility library.
# Provides sane project defaults and macros common to LuaDist CMake builds.
# 
# Copyright (C) 2007-2012 LuaDist.
# by David Manura, Peter Drahoš
# Redistribution and use of this file is allowed according to the terms of the MIT license.
# For details see the COPYRIGHT file distributed with LuaDist.
# Please note that the package source code is licensed under its own license.

## Extract information from dist.info
message("HERE" .)
message(" HERE 2 ${LUAJIT_DIR}")
if ( NOT EXISTS ${LUAJIT_DIR}/dist.info )
  message ( FATAL_ERROR
    "Missing dist.info file (${LUAJIT_DIR}/dist.info)." )
endif ()
file ( READ ${LUAJIT_DIR}/dist.info DIST_INFO )
if ( "${DIST_INFO}" STREQUAL "" )
  message ( FATAL_ERROR "Failed to load dist.info." )
endif ()
# Reads field `name` from dist.info string `DIST_INFO` into variable `var`.
macro ( _parse_dist_field name var )
  string ( REGEX REPLACE ".*${name}[ \t]?=[ \t]?[\"']([^\"']+)[\"'].*" "\\1"
           ${var} "${DIST_INFO}" )
  if ( ${var} STREQUAL DIST_INFO )
    message ( FATAL_ERROR "Failed to extract \"${var}\" from dist.info" )
  endif ()
endmacro ()
#
_parse_dist_field ( name DIST_NAME )
_parse_dist_field ( version DIST_VERSION )
_parse_dist_field ( license DIST_LICENSE )
_parse_dist_field ( author DIST_AUTHOR )
_parse_dist_field ( maintainer DIST_MAINTAINER )
_parse_dist_field ( url DIST_URL )
_parse_dist_field ( desc DIST_DESC )
message ( "DIST_NAME: ${DIST_NAME}")
message ( "DIST_VERSION: ${DIST_VERSION}")
message ( "DIST_LICENSE: ${DIST_LICENSE}")
message ( "DIST_AUTHOR: ${DIST_AUTHOR}")
message ( "DIST_MAINTAINER: ${DIST_MAINTAINER}")
message ( "DIST_URL: ${DIST_URL}")
message ( "DIST_DESC: ${DIST_DESC}")
string ( REGEX REPLACE ".*depends[ \t]?=[ \t]?[\"']([^\"']+)[\"'].*" "\\1"
         DIST_DEPENDS ${DIST_INFO} )
if ( DIST_DEPENDS STREQUAL DIST_INFO )
  set ( DIST_DEPENDS "" )
endif ()
message ( "DIST_DEPENDS: ${DIST_DEPENDS}")
## 2DO: Parse DIST_DEPENDS and try to install Dependencies with automatically using externalproject_add


## INSTALL DEFAULTS (Relative to CMAKE_INSTALL_PREFIX)
# Primary paths
set ( INSTALL_BIN bin CACHE PATH "Where to install binaries to." )
set ( INSTALL_LIB lib CACHE PATH "Where to install libraries to." )
set ( INSTALL_INC include CACHE PATH "Where to install headers to." )
set ( INSTALL_ETC etc CACHE PATH "Where to store configuration files" )
set ( INSTALL_SHARE share CACHE PATH "Directory for shared data." )

# Secondary paths
option ( INSTALL_VERSION
      "Install runtime libraries and executables with version information." OFF)
set ( INSTALL_DATA ${INSTALL_SHARE}/${DIST_NAME} CACHE PATH
      "Directory the package can store documentation, tests or other data in.")  
set ( INSTALL_DOC  ${INSTALL_DATA}/doc CACHE PATH
      "Recommended directory to install documentation into.")
set ( INSTALL_EXAMPLE ${INSTALL_DATA}/example CACHE PATH
      "Recommended directory to install examples into.")
set ( INSTALL_TEST ${INSTALL_DATA}/test CACHE PATH
      "Recommended directory to install tests into.")
set ( INSTALL_FOO  ${INSTALL_DATA}/etc CACHE PATH
      "Where to install additional files")

# Tweaks and other defaults
# Setting CMAKE to use loose block and search for find modules in source directory
set ( CMAKE_ALLOW_LOOSE_LOOP_CONSTRUCTS true )
set ( CMAKE_MODULE_PATH "${LUAJIT_DIR}/cmake" ${CMAKE_MODULE_PATH} )
option ( BUILD_SHARED_LIBS "Build shared libraries" ON )

# In MSVC, prevent warnings that can occur when using standard libraries.
if ( MSVC )
  add_definitions ( -D_CRT_SECURE_NO_WARNINGS )
endif ()

# RPath and relative linking
option ( USE_RPATH "Use relative linking." ON)
if ( USE_RPATH )
  string ( REGEX REPLACE "[^!/]+" "../../aerospike-cmake" UP_DIR ${INSTALL_BIN} )
  set ( CMAKE_SKIP_BUILD_RPATH FALSE CACHE STRING "" FORCE )
  set ( CMAKE_BUILD_WITH_INSTALL_RPATH FALSE CACHE STRING "" FORCE )
  set ( CMAKE_INSTALL_RPATH $ORIGIN/${UP_DIR}/${INSTALL_LIB}
        CACHE STRING "" FORCE )
  set ( CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE CACHE STRING "" FORCE )
  set ( CMAKE_INSTALL_NAME_DIR @executable_path/${UP_DIR}/${INSTALL_LIB}
        CACHE STRING "" FORCE )
endif ()

## MACROS
# Parser macro
macro ( parse_arguments prefix arg_names option_names)
  set ( DEFAULT_ARGS )
  foreach ( arg_name ${arg_names} )
    set ( ${prefix}_${arg_name} )
  endforeach ()
  foreach ( option ${option_names} )
    set ( ${prefix}_${option} FALSE )
  endforeach ()

  set ( current_arg_name DEFAULT_ARGS )
  set ( current_arg_list )
  foreach ( arg ${ARGN} )            
    set ( larg_names ${arg_names} )    
    list ( FIND larg_names "${arg}" is_arg_name )                   
    if ( is_arg_name GREATER -1 )
      set ( ${prefix}_${current_arg_name} ${current_arg_list} )
      set ( current_arg_name ${arg} )
      set ( current_arg_list )
    else ()
      set ( loption_names ${option_names} )    
      list ( FIND loption_names "${arg}" is_option )            
      if ( is_option GREATER -1 )
        set ( ${prefix}_${arg} TRUE )
      else ()
        set ( current_arg_list ${current_arg_list} ${arg} )
      endif ()
    endif ()
  endforeach ()
  set ( ${prefix}_${current_arg_name} ${current_arg_list} )
endmacro ()


# install_executable ( executable_targets )
# Installs any executables generated using "add_executable".
# USE: install_executable ( lua )
# NOTE: subdirectories are NOT supported
set ( CPACK_COMPONENT_RUNTIME_DISPLAY_NAME "${DIST_NAME} Runtime" )
set ( CPACK_COMPONENT_RUNTIME_DESCRIPTION
      "Executables and runtime libraries. Installed into ${INSTALL_BIN}." )
macro ( install_executable )
  foreach ( _file ${ARGN} )
    if ( INSTALL_VERSION )
      set_target_properties ( ${_file} PROPERTIES VERSION ${DIST_VERSION}
                              SOVERSION ${DIST_VERSION} )
    endif ()
    install ( TARGETS ${_file} RUNTIME DESTINATION ${INSTALL_BIN}
              COMPONENT Runtime )
  endforeach()
endmacro ()

# install_library ( library_targets )
# Installs any libraries generated using "add_library" into apropriate places.
# USE: install_library ( libexpat )
# NOTE: subdirectories are NOT supported
set ( CPACK_COMPONENT_LIBRARY_DISPLAY_NAME "${DIST_NAME} Development Libraries" )
set ( CPACK_COMPONENT_LIBRARY_DESCRIPTION
  "Static and import libraries needed for development. Installed into ${INSTALL_LIB} or ${INSTALL_BIN}." )
macro ( install_library )
  foreach ( _file ${ARGN} )
    if ( INSTALL_VERSION )
      set_target_properties ( ${_file} PROPERTIES VERSION ${DIST_VERSION}
                              SOVERSION ${DIST_VERSION} )
    endif ()
    install ( TARGETS ${_file}
              RUNTIME DESTINATION ${INSTALL_BIN} COMPONENT Runtime
              LIBRARY DESTINATION ${INSTALL_LIB} COMPONENT Runtime 
              ARCHIVE DESTINATION ${INSTALL_LIB} COMPONENT Library )
  endforeach()
endmacro ()

# helper function for various install_* functions, for PATTERN/REGEX args.
macro ( _complete_install_args )
  if ( NOT("${_ARG_PATTERN}" STREQUAL "") )
    set ( _ARG_PATTERN PATTERN ${_ARG_PATTERN} )
  endif ()
  if ( NOT("${_ARG_REGEX}" STREQUAL "") )
    set ( _ARG_REGEX REGEX ${_ARG_REGEX} )
  endif ()
endmacro ()

# install_header ( files/directories [INTO destination] )
# Install a directories or files into header destination.
# USE: install_header ( lua.h luaconf.h ) or install_header ( GL )
# USE: install_header ( mylib.h INTO mylib )
# For directories, supports optional PATTERN/REGEX arguments like install().
set ( CPACK_COMPONENT_HEADER_DISPLAY_NAME "${DIST_NAME} Development Headers" )
set ( CPACK_COMPONENT_HEADER_DESCRIPTION
      "Headers needed for development. Installed into ${INSTALL_INC}." )
macro ( install_header )
  parse_arguments ( _ARG "INTO;PATTERN;REGEX" "" ${ARGN} )
  _complete_install_args()
  foreach ( _file ${_ARG_DEFAULT_ARGS} )
    if ( IS_DIRECTORY "${LUAJIT_DIR}/${_file}" )
      install ( DIRECTORY ${_file} DESTINATION ${INSTALL_INC}/${_ARG_INTO}
                COMPONENT Header ${_ARG_PATTERN} ${_ARG_REGEX} )
    else ()
      install ( FILES ${_file} DESTINATION ${INSTALL_INC}/${_ARG_INTO}
                COMPONENT Header )
    endif ()
  endforeach()
endmacro ()

# install_data ( files/directories [INTO destination] )
# This installs additional data files or directories.
# USE: install_data ( extra data.dat )
# USE: install_data ( image1.png image2.png INTO images )
# For directories, supports optional PATTERN/REGEX arguments like install().
set ( CPACK_COMPONENT_DATA_DISPLAY_NAME "${DIST_NAME} Data" )
set ( CPACK_COMPONENT_DATA_DESCRIPTION
      "Application data. Installed into ${INSTALL_DATA}." )
macro ( install_data )
  parse_arguments ( _ARG "INTO;PATTERN;REGEX" "" ${ARGN} )
  _complete_install_args()
  foreach ( _file ${_ARG_DEFAULT_ARGS} )
    if ( IS_DIRECTORY "${LUAJIT_DIR}/${_file}" )
      install ( DIRECTORY ${_file}
                DESTINATION ${INSTALL_DATA}/${_ARG_INTO}
                COMPONENT Data ${_ARG_PATTERN} ${_ARG_REGEX} )
    else ()
      install ( FILES ${_file} DESTINATION ${INSTALL_DATA}/${_ARG_INTO}
                COMPONENT Data )
    endif ()
  endforeach()
endmacro ()

# INSTALL_DOC ( files/directories [INTO destination] )
# This installs documentation content
# USE: install_doc ( doc/ doc.pdf )
# USE: install_doc ( index.html INTO html )
# For directories, supports optional PATTERN/REGEX arguments like install().
set ( CPACK_COMPONENT_DOCUMENTATION_DISPLAY_NAME "${DIST_NAME} Documentation" )
set ( CPACK_COMPONENT_DOCUMENTATION_DESCRIPTION
      "Application documentation. Installed into ${INSTALL_DOC}." )
macro ( install_doc )
  parse_arguments ( _ARG "INTO;PATTERN;REGEX" "" ${ARGN} )
  _complete_install_args()
  foreach ( _file ${_ARG_DEFAULT_ARGS} )
    if ( IS_DIRECTORY "${LUAJIT_DIR}/${_file}" )
      install ( DIRECTORY ${_file} DESTINATION ${INSTALL_DOC}/${_ARG_INTO}
                COMPONENT Documentation ${_ARG_PATTERN} ${_ARG_REGEX} )
    else ()
      install ( FILES ${_file} DESTINATION ${INSTALL_DOC}/${_ARG_INTO}
                COMPONENT Documentation )
    endif ()
  endforeach()
endmacro ()

# install_example ( files/directories [INTO destination]  )
# This installs additional examples
# USE: install_example ( examples/ exampleA )
# USE: install_example ( super_example super_data INTO super)
# For directories, supports optional PATTERN/REGEX argument like install().
set ( CPACK_COMPONENT_EXAMPLE_DISPLAY_NAME "${DIST_NAME} Examples" )
set ( CPACK_COMPONENT_EXAMPLE_DESCRIPTION
    "Examples and their associated data. Installed into ${INSTALL_EXAMPLE}." )
macro ( install_example )
  parse_arguments ( _ARG "INTO;PATTERN;REGEX" "" ${ARGN} )
  _complete_install_args()
  foreach ( _file ${_ARG_DEFAULT_ARGS} )
    if ( IS_DIRECTORY "${LUAJIT_DIR}/${_file}" )
      install ( DIRECTORY ${_file} DESTINATION ${INSTALL_EXAMPLE}/${_ARG_INTO}
                COMPONENT Example ${_ARG_PATTERN} ${_ARG_REGEX} )
    else ()
      install ( FILES ${_file} DESTINATION ${INSTALL_EXAMPLE}/${_ARG_INTO}
                COMPONENT Example )
    endif ()
  endforeach()
endmacro ()

# install_test ( files/directories [INTO destination] )
# This installs tests and test files, DOES NOT EXECUTE TESTS
# USE: install_test ( my_test data.sql )
# USE: install_test ( feature_x_test INTO x )
# For directories, supports optional PATTERN/REGEX argument like install().
set ( CPACK_COMPONENT_TEST_DISPLAY_NAME "${DIST_NAME} Tests" )
set ( CPACK_COMPONENT_TEST_DESCRIPTION
      "Tests and associated data. Installed into ${INSTALL_TEST}." )
macro ( install_test )
  parse_arguments ( _ARG "INTO;PATTERN;REGEX" "" ${ARGN} )
  _complete_install_args()
  foreach ( _file ${_ARG_DEFAULT_ARGS} )
    if ( IS_DIRECTORY "${LUAJIT_DIR}/${_file}" )
      install ( DIRECTORY ${_file} DESTINATION ${INSTALL_TEST}/${_ARG_INTO}
                COMPONENT Test ${_ARG_PATTERN} ${_ARG_REGEX} )
    else ()
      install ( FILES ${_file} DESTINATION ${INSTALL_TEST}/${_ARG_INTO}
                COMPONENT Test )
    endif ()
  endforeach()
endmacro ()

# install_foo ( files/directories [INTO destination] )
# This installs optional or otherwise unneeded content
# USE: install_foo ( etc/ example.doc )
# USE: install_foo ( icon.png logo.png INTO icons)
# For directories, supports optional PATTERN/REGEX argument like install().
set ( CPACK_COMPONENT_OTHER_DISPLAY_NAME "${DIST_NAME} Unspecified Content" )
set ( CPACK_COMPONENT_OTHER_DESCRIPTION
      "Other unspecified content. Installed into ${INSTALL_FOO}." )
macro ( install_foo )
  parse_arguments ( _ARG "INTO;PATTERN;REGEX" "" ${ARGN} )
  _complete_install_args()
  foreach ( _file ${_ARG_DEFAULT_ARGS} )
    if ( IS_DIRECTORY "${LUAJIT_DIR}/${_file}" )
      install ( DIRECTORY ${_file} DESTINATION ${INSTALL_FOO}/${_ARG_INTO}
                COMPONENT Other ${_ARG_PATTERN} ${_ARG_REGEX} )
    else ()
      install ( FILES ${_file} DESTINATION ${INSTALL_FOO}/${_ARG_INTO}
                COMPONENT Other )
    endif ()
  endforeach()
endmacro ()

## CTest defaults

## CPack defaults
set ( CPACK_GENERATOR "ZIP" )
set ( CPACK_STRIP_FILES TRUE )
set ( CPACK_PACKAGE_NAME "${DIST_NAME}" )
set ( CPACK_PACKAGE_VERSION "${DIST_VERSION}")
set ( CPACK_PACKAGE_VENDOR "LuaDist" )
set ( CPACK_COMPONENTS_ALL Runtime Library Header Data Documentation Example Other )
include ( CPack )
