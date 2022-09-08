if (NOT LIBSSH_GLOBAL_BIND_CONFIG)
  set(GLOBAL_BIND_CONFIG "/etc/ssh/libssh_server_config")
else()
  set(GLOBAL_BIND_CONFIG LIBSSH_GLOBAL_BIND_CONFIG)
endif()

if (NOT LIBSSH_GLOBAL_CLIENT_CONFIG)
  set(GLOBAL_CLIENT_CONFIG "/etc/ssh/ssh_config")
else()
  set(GLOBAL_CLIENT_CONFIG LIBSSH_GLOBAL_CLIENT_CONFIG)
endif ()

set(LIB_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libssh")
set(LIB_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/libssh")
# Specify search path for CMake modules to be loaded by include() 
# and find_package()
list(APPEND CMAKE_MODULE_PATH "${LIB_SOURCE_DIR}/cmake/Modules")

include(DefineCMakeDefaults)
include(DefineCompilerFlags)

project(libssh VERSION 0.9.6 LANGUAGES C)

# global needed variable
set(APPLICATION_NAME ${PROJECT_NAME})

# SOVERSION scheme: CURRENT.AGE.REVISION
#   If there was an incompatible interface change:
#     Increment CURRENT. Set AGE and REVISION to 0
#   If there was a compatible interface change:
#     Increment AGE. Set REVISION to 0
#   If the source code was changed, but there were no interface changes:
#     Increment REVISION.
set(LIBRARY_VERSION "4.8.7")
set(LIBRARY_SOVERSION "4")

# where to look first for cmake modules, before ${CMAKE_ROOT}/Modules/ is checked

# add definitions

include(DefinePlatformDefaults)
include("${LIB_SOURCE_DIR}/CompilerChecks.cmake")

# Copy library files to a lib sub-directory
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${LIB_BINARY_DIR}/lib")

set(CMAKE_THREAD_PREFER_PTHREADS ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)

set(WITH_ZLIB OFF)
set(WITH_SYMBOL_VERSIONING OFF)

# config.h checks
include(ConfigureChecks.cmake)
configure_file(${LIB_SOURCE_DIR}/config.h.cmake ${LIB_BINARY_DIR}/config.h)
configure_file(${LIB_SOURCE_DIR}/include/libssh/libssh_version.h.cmake
               ${LIB_BINARY_DIR}/include/libssh/libssh_version.h
               @ONLY)

include(IncludeSources.cmake)

message(STATUS "********************************************")
message(STATUS "********** ${PROJECT_NAME} build options : **********")

message(STATUS "Global client config: ${GLOBAL_CLIENT_CONFIG}")
if (WITH_SERVER)
message(STATUS "Global bind config: ${GLOBAL_BIND_CONFIG}")
endif()
message(STATUS "********************************************")

