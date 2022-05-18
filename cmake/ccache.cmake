# Setup integration with ccache to speed up builds, see https://ccache.dev/

if (CMAKE_CXX_COMPILER_LAUNCHER MATCHES "ccache" OR CMAKE_C_COMPILER_LAUNCHER MATCHES "ccache")
    # custom compiler launcher already defined, most likely because cmake was invoked with like "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache" or
    # via environment variable --> respect setting and trust that the launcher was specified correctly
    message(STATUS "Using custom C compiler launcher: ${CMAKE_C_COMPILER_LAUNCHER}")
    message(STATUS "Using custom C++ compiler launcher: ${CMAKE_CXX_COMPILER_LAUNCHER}")
    return()
endif()

option(ENABLE_CCACHE "Speedup re-compilations using ccache (external tool)" ON)

if (NOT ENABLE_CCACHE)
    message(STATUS "Using ccache: no (disabled via configuration)")
    return()
endif()

find_program (CCACHE_EXECUTABLE ccache)

if (NOT CCACHE_EXECUTABLE)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Using ccache: no (Could not find find ccache. To significantly reduce compile times for the 2nd, 3rd, etc. build, it is highly recommended to install ccache. To suppress this message, run cmake with -DENABLE_CCACHE=0)")
    return()
endif()

execute_process(COMMAND ${CCACHE_EXECUTABLE} "-V" OUTPUT_VARIABLE CCACHE_VERSION)
string(REGEX REPLACE "ccache version ([0-9\\.]+).*" "\\1" CCACHE_VERSION ${CCACHE_VERSION})

set (CCACHE_MINIMUM_VERSION 3.3)

if (CCACHE_VERSION VERSION_LESS_EQUAL ${CCACHE_MINIMUM_VERSION})
    message(${RECONFIGURE_MESSAGE_LEVEL} "Using ccache: no (found ${CCACHE_EXECUTABLE} (version ${CCACHE_VERSION}), the minimum required version is ${CCACHE_MINIMUM_VERSION}")
    return()
endif()

message(STATUS "Using ccache: ${CCACHE_EXECUTABLE} (version ${CCACHE_VERSION})")
set(LAUNCHER ${CCACHE_EXECUTABLE})

# debian (debhelpers) set SOURCE_DATE_EPOCH environment variable, that is
# filled from the debian/changelog or current time.
#
# - 4.0+ ccache always includes this environment variable into the hash
#   of the manifest, which do not allow to use previous cache,
# - 4.2+ ccache ignores SOURCE_DATE_EPOCH for every file w/o __DATE__/__TIME__
#
# Exclude SOURCE_DATE_EPOCH env for ccache versions between [4.0, 4.2).
if (CCACHE_VERSION VERSION_GREATER_EQUAL "4.0" AND CCACHE_VERSION VERSION_LESS "4.2")
    message(STATUS "Ignore SOURCE_DATE_EPOCH for ccache")
    set(LAUNCHER env -u SOURCE_DATE_EPOCH ${CCACHE_EXECUTABLE})
endif()

set (CMAKE_CXX_COMPILER_LAUNCHER ${LAUNCHER} ${CMAKE_CXX_COMPILER_LAUNCHER})
set (CMAKE_C_COMPILER_LAUNCHER ${LAUNCHER} ${CMAKE_C_COMPILER_LAUNCHER})
