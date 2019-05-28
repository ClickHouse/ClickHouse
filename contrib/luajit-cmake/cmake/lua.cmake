# LuaDist CMake utility library for Lua.
# 
# Copyright (C) 2007-2012 LuaDist.
# by David Manura, Peter Drahos
# Redistribution and use of this file is allowed according to the terms of the MIT license.
# For details see the COPYRIGHT file distributed with LuaDist.
# Please note that the package source code is licensed under its own license.

set ( INSTALL_LMOD ${INSTALL_LIB}/lua
      CACHE PATH "Directory to install Lua modules." )
set ( INSTALL_CMOD ${INSTALL_LIB}/lua
      CACHE PATH "Directory to install Lua binary modules." )

option ( SKIP_LUA_WRAPPER
         "Do not build and install Lua executable wrappers." OFF)

# List of (Lua module name, file path) pairs.
# Used internally by add_lua_test.  Built by add_lua_module.
set ( _lua_modules )

# utility function: appends path `path` to path `basepath`, properly
# handling cases when `path` may be relative or absolute.
macro ( _append_path basepath path result )
  if ( IS_ABSOLUTE "${path}" )
    set ( ${result} "${path}" )
  else ()
    set ( ${result} "${basepath}/${path}" )
  endif ()
endmacro ()

# install_lua_executable ( target source )
# Automatically generate a binary if srlua package is available
# The application or its source will be placed into /bin 
# If the application source did have .lua suffix then it will be removed
# USE: lua_executable ( sputnik src/sputnik.lua )
macro ( install_lua_executable _name _source )
  # Find srlua and glue
  find_program( SRLUA_EXECUTABLE NAMES srlua )
  find_program( GLUE_EXECUTABLE NAMES glue )
  # Executable output
  set ( _exe ${CMAKE_CURRENT_BINARY_DIR}/${_name}${CMAKE_EXECUTABLE_SUFFIX} )
  if ( NOT SKIP_LUA_WRAPPER AND SRLUA_EXECUTABLE AND GLUE_EXECUTABLE )
    # Generate binary gluing the lua code to srlua, this is a robuust approach for most systems
    # Recommended, expecially for Windows systems
    add_custom_command(
      OUTPUT ${_exe}
      COMMAND ${GLUE_EXECUTABLE} 
      ARGS ${SRLUA_EXECUTABLE} ${_source} ${_exe}
      DEPENDS ${_source}
      WORKING_DIRECTORY .
      VERBATIM
    )
    # Make sure we have a target associated with the binary
    add_custom_target(${_name} ALL
        DEPENDS ${_exe}
    )
    # Install with run permissions
    install ( PROGRAMS ${_exe} DESTINATION ${INSTALL_BIN} COMPONENT Runtime)
	  # Also install the source as optional component
	  install ( PROGRAMS ${_source} DESTINATION ${INSTALL_FOO} 
            RENAME ${_name}
			      COMPONENT Other 
    )
  else()
    # Install into bin as is, we assume the executable uses UNIX shebang
    # NOTE: This approach is unsuitable for non UNIX systems
    install ( PROGRAMS ${_source} DESTINATION ${INSTALL_BIN}
            RENAME ${_name}
            COMPONENT Runtime
    )
  endif()
endmacro ()

macro ( _lua_module_helper is_install _name ) 
  parse_arguments ( _MODULE "LINK;ALL_IN_ONE" "" ${ARGN} )
  # _target is CMake-compatible target name for module (e.g. socket_core).
  # _module is relative path of target (e.g. socket/core),
  #   without extension (e.g. .lua/.so/.dll).
  # _MODULE_SRC is list of module source files (e.g. .lua and .c files).
  # _MODULE_NAMES is list of module names (e.g. socket.core).
  if ( _MODULE_ALL_IN_ONE )
    string ( REGEX REPLACE "\\..*" "" _target "${_name}" )
    string ( REGEX REPLACE "\\..*" "" _module "${_name}" )
    set ( _target "${_target}_all_in_one")
    set ( _MODULE_SRC ${_MODULE_ALL_IN_ONE} )
    set ( _MODULE_NAMES ${_name} ${_MODULE_DEFAULT_ARGS} )
  else ()
    string ( REPLACE "." "_" _target "${_name}" )
    string ( REPLACE "." "." _module "${_name}" )
    set ( _MODULE_SRC ${_MODULE_DEFAULT_ARGS} )
    set ( _MODULE_NAMES ${_name} )
  endif ()
  if ( NOT _MODULE_SRC )
    message ( FATAL_ERROR "no module sources specified" )
  endif ()
  list ( GET _MODULE_SRC 0 _first_source )
  
  get_filename_component ( _ext ${_first_source} EXT )
  if ( _ext STREQUAL ".lua" )  # Lua source module
    list ( LENGTH _MODULE_SRC _len )
    if ( _len GREATER 1 )
      message ( FATAL_ERROR "more than one source file specified" )
    endif ()
  
    set ( _module "${_module}.lua" )

    get_filename_component ( _module_dir ${_module} PATH )
    get_filename_component ( _module_filename ${_module} NAME )
    _append_path ( "." "${_first_source}" _module_path )
    list ( APPEND _lua_modules "${_name}" "${_module_path}" )

    if ( ${is_install} )
      install ( FILES ${_first_source} DESTINATION ${INSTALL_LMOD}/${_module_dir}
                RENAME ${_module_filename} 
                COMPONENT Runtime
      )
    endif ()
  else ()  # Lua C binary module
    enable_language ( C )
    find_package ( Lua REQUIRED )
    include_directories ( ${LUA_INCLUDE_DIR} )

    set ( _module "${_module}${CMAKE_SHARED_MODULE_SUFFIX}" )

    get_filename_component ( _module_dir ${_module} PATH )
    get_filename_component ( _module_filenamebase ${_module} NAME_WE )
    foreach ( _thisname ${_MODULE_NAMES} )
      list ( APPEND _lua_modules "${_thisname}"
             "${CMAKE_CURRENT_BINARY_DIR}/\${CMAKE_CFG_INTDIR}/${_module}" )
    endforeach ()
   
    add_library( ${_target} MODULE ${_MODULE_SRC})
    target_link_libraries ( ${_target} ${LUA_LIBRARY} ${_MODULE_LINK} )
    set_target_properties ( ${_target} PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                "${_module_dir}" PREFIX "" OUTPUT_NAME "${_module_filenamebase}" )
    if ( ${is_install} )
      install ( TARGETS ${_target} DESTINATION ${INSTALL_CMOD}/${_module_dir} COMPONENT Runtime)
    endif ()
  endif ()
endmacro ()

# add_lua_module
# Builds a Lua source module into a destination locatable by Lua
# require syntax.
# Binary modules are also supported where this function takes sources and
# libraries to compile separated by LINK keyword.
# USE: add_lua_module ( socket.http src/http.lua )
# USE2: add_lua_module ( mime.core src/mime.c )
# USE3: add_lua_module ( socket.core ${SRC_SOCKET} LINK ${LIB_SOCKET} )
# USE4: add_lua_module ( ssl.context ssl.core ALL_IN_ONE src/context.c src/ssl.c )
#   This form builds an "all-in-one" module (e.g. ssl.so or ssl.dll containing
#   both modules ssl.context and ssl.core).  The CMake target name will be
#   ssl_all_in_one.
# Also sets variable _module_path (relative path where module typically
# would be installed).
macro ( add_lua_module )
  _lua_module_helper ( 0 ${ARGN} )
endmacro ()


# install_lua_module
# This is the same as `add_lua_module` but also installs the module.
# USE: install_lua_module ( socket.http src/http.lua )
# USE2: install_lua_module ( mime.core src/mime.c )
# USE3: install_lua_module ( socket.core ${SRC_SOCKET} LINK ${LIB_SOCKET} )
macro ( install_lua_module )
  _lua_module_helper ( 1 ${ARGN} )
endmacro ()

# Builds string representing Lua table mapping Lua modules names to file
# paths.  Used internally.
macro ( _make_module_table _outvar )
  set ( ${_outvar} )
  list ( LENGTH _lua_modules _n )
  if ( ${_n} GREATER 0 ) # avoids cmake complaint
  foreach ( _i RANGE 1 ${_n} 2 )
    list ( GET _lua_modules ${_i} _path )
    math ( EXPR _ii ${_i}-1 )
    list ( GET _lua_modules ${_ii} _name )
    set ( ${_outvar} "${_table}  ['${_name}'] = '${_path}'\;\n")
  endforeach ()
  endif ()
  set ( ${_outvar}
"local modules = {
${_table}}" )
endmacro ()

# add_lua_test ( _testfile [ WORKING_DIRECTORY _working_dir ] )
# Runs Lua script `_testfile` under CTest tester.
# Optional named argument `WORKING_DIRECTORY` is current working directory to
# run test under (defaults to ${CMAKE_CURRENT_BINARY_DIR}).
# Both paths, if relative, are relative to ${CMAKE_CURRENT_SOURCE_DIR}.
# Any modules previously defined with install_lua_module are automatically
# preloaded (via package.preload) prior to running the test script.
# Under LuaDist, set test=true in config.lua to enable testing.
# USE: add_lua_test ( test/test1.lua [args...] [WORKING_DIRECTORY dir])
macro ( add_lua_test _testfile )
  if ( NOT SKIP_TESTING )
    parse_arguments ( _ARG "WORKING_DIRECTORY" "" ${ARGN} )
    include ( CTest )
    find_program ( LUA NAMES lua lua.bat )
    get_filename_component ( TESTFILEABS ${_testfile} ABSOLUTE )
    get_filename_component ( TESTFILENAME ${_testfile} NAME )
    get_filename_component ( TESTFILEBASE ${_testfile} NAME_WE )

    # Write wrapper script.
    # Note: One simple way to allow the script to find modules is
    # to just put them in package.preload.
    set ( TESTWRAPPER ${CMAKE_CURRENT_BINARY_DIR}/${TESTFILENAME} )
    _make_module_table ( _table )
    set ( TESTWRAPPERSOURCE
"local CMAKE_CFG_INTDIR = ... or '.'
${_table}
local function preload_modules(modules)
  for name, path in pairs(modules) do
    if path:match'%.lua' then
      package.preload[name] = assert(loadfile(path))
    else
      local name = name:gsub('.*%-', '') -- remove any hyphen prefix
      local symbol = 'luaopen_' .. name:gsub('%.', '_')
          --improve: generalize to support all-in-one loader?
      local path = path:gsub('%$%{CMAKE_CFG_INTDIR%}', CMAKE_CFG_INTDIR)
      package.preload[name] = assert(package.loadlib(path, symbol))
    end
  end
end
preload_modules(modules)
arg[0] = '${TESTFILEABS}'
table.remove(arg, 1)
return assert(loadfile '${TESTFILEABS}')(unpack(arg))
"    )
    if ( _ARG_WORKING_DIRECTORY )
      get_filename_component (
         TESTCURRENTDIRABS ${_ARG_WORKING_DIRECTORY} ABSOLUTE )
      # note: CMake 2.6 (unlike 2.8) lacks WORKING_DIRECTORY parameter.
      set ( _pre ${CMAKE_COMMAND} -E chdir "${TESTCURRENTDIRABS}" )
    endif ()
    file ( WRITE ${TESTWRAPPER} ${TESTWRAPPERSOURCE})
    add_test ( NAME ${TESTFILEBASE} COMMAND ${_pre} ${LUA}
               ${TESTWRAPPER} "${CMAKE_CFG_INTDIR}"
               ${_ARG_DEFAULT_ARGS} )
  endif ()
  # see also http://gdcm.svn.sourceforge.net/viewvc/gdcm/Sandbox/CMakeModules/UsePythonTest.cmake
  # Note: ${CMAKE_CFG_INTDIR} is a command-line argument to allow proper
  # expansion by the native build tool.
endmacro ()


# Converts Lua source file `_source` to binary string embedded in C source
# file `_target`.  Optionally compiles Lua source to byte code (not available
# under LuaJIT2, which doesn't have a bytecode loader).  Additionally, Lua
# versions of bin2c [1] and luac [2] may be passed respectively as additional
# arguments.
#
# [1] http://lua-users.org/wiki/BinToCee
# [2] http://lua-users.org/wiki/LuaCompilerInLua
function ( add_lua_bin2c _target _source )
  find_program ( LUA NAMES lua lua.bat )
  execute_process ( COMMAND ${LUA} -e "string.dump(function()end)"
                    RESULT_VARIABLE _LUA_DUMP_RESULT ERROR_QUIET )
  if ( NOT ${_LUA_DUMP_RESULT} )
    SET ( HAVE_LUA_DUMP true )
  endif ()
  message ( "-- string.dump=${HAVE_LUA_DUMP}" )

  if ( ARGV2 )
    get_filename_component ( BIN2C ${ARGV2} ABSOLUTE )
    set ( BIN2C ${LUA} ${BIN2C} )
  else ()
    find_program ( BIN2C NAMES bin2c bin2c.bat )
  endif ()
  if ( HAVE_LUA_DUMP )
    if ( ARGV3 )
      get_filename_component ( LUAC ${ARGV3} ABSOLUTE )
      set ( LUAC ${LUA} ${LUAC} )
    else ()
      find_program ( LUAC NAMES luac luac.bat )
    endif ()
  endif ( HAVE_LUA_DUMP )
  message ( "-- bin2c=${BIN2C}" )
  message ( "-- luac=${LUAC}" )

  get_filename_component ( SOURCEABS ${_source} ABSOLUTE )
  if ( HAVE_LUA_DUMP )
    get_filename_component ( SOURCEBASE ${_source} NAME_WE )
    add_custom_command (
      OUTPUT  ${_target} DEPENDS ${_source}
      COMMAND ${LUAC} -o ${CMAKE_CURRENT_BINARY_DIR}/${SOURCEBASE}.lo
              ${SOURCEABS}
      COMMAND ${BIN2C} ${CMAKE_CURRENT_BINARY_DIR}/${SOURCEBASE}.lo
              ">${_target}" )
  else ()
    add_custom_command (
      OUTPUT  ${_target} DEPENDS ${SOURCEABS}
      COMMAND ${BIN2C} ${_source} ">${_target}" )
  endif ()
endfunction()
