# Setup integration with ccache to speed up builds, see https://ccache.dev/

include(cmake/utils.cmake)

set(CHCACHE_EXECUTABLE_PATH "" CACHE STRING "Path to chcache executable to use. If the compiler cache is set to use chcache, chcache will be used from here instead of building it.")

# Defensive programming: early return to avoid configuring any cache after we've set dummy launchers.
# If something includes this file by mistake after the first setup, it'd override the dummy launchers.
if(USING_DUMMY_LAUNCHERS)
    message(STATUS "Skipping cache integration a second time because dummy launchers are in use")
    return()
endif()

# Matches both ccache and sccache
if (CMAKE_CXX_COMPILER_LAUNCHER MATCHES "ccache" OR CMAKE_C_COMPILER_LAUNCHER MATCHES "ccache")
    # custom compiler launcher already defined, most likely because cmake was invoked with like "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache" or
    # via environment variable --> respect setting and trust that the launcher was specified correctly
    message(STATUS "Using custom C compiler launcher: ${CMAKE_C_COMPILER_LAUNCHER}")
    message(STATUS "Using custom C++ compiler launcher: ${CMAKE_CXX_COMPILER_LAUNCHER}")
    return()
endif()

set(COMPILER_CACHE "auto" CACHE STRING "Speedup re-compilations using the caching tools; valid options are 'auto' (sccache, then ccache), 'ccache', 'sccache', 'chcache', or 'disabled'")

if(COMPILER_CACHE STREQUAL "auto")
    find_program (CCACHE_EXECUTABLE NAMES sccache ccache)
elseif(COMPILER_CACHE STREQUAL "ccache")
    find_program (CCACHE_EXECUTABLE ccache)
elseif(COMPILER_CACHE STREQUAL "sccache")
    find_program (CCACHE_EXECUTABLE sccache)
elseif(COMPILER_CACHE STREQUAL "chcache")
    if(CHCACHE_EXECUTABLE_PATH STREQUAL "")
        message(STATUS "Using self-built chcache")
        set(CCACHE_EXECUTABLE ${CMAKE_CURRENT_BINARY_DIR}/rust/chcache/chcache)
    else()
        message(STATUS "Using already built chcache from ${CHCACHE_EXECUTABLE_PATH}")
        set(CCACHE_EXECUTABLE ${CHCACHE_EXECUTABLE_PATH})
    endif()
elseif(COMPILER_CACHE STREQUAL "disabled")
    message(STATUS "Using *ccache: no (disabled via configuration)")
    return()
else()
    message(${RECONFIGURE_MESSAGE_LEVEL} "The COMPILER_CACHE must be one of (auto|sccache|ccache|chcache|disabled), value: '${COMPILER_CACHE}'")
endif()


if (NOT CCACHE_EXECUTABLE)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Using *ccache: no (Could not find find ccache or sccache. To significantly reduce compile times for the 2nd, 3rd, etc. build, it is highly recommended to install one of them. To suppress this message, run cmake with -DCOMPILER_CACHE=disabled)")
    return()
endif()

if (CCACHE_EXECUTABLE MATCHES "/ccache$")
    execute_process(COMMAND ${CCACHE_EXECUTABLE} "-V" OUTPUT_VARIABLE CCACHE_VERSION)
    string(REGEX REPLACE "ccache version ([0-9\\.]+).*" "\\1" CCACHE_VERSION ${CCACHE_VERSION})

    set (CCACHE_MINIMUM_VERSION 3.3)

    if (CCACHE_VERSION VERSION_LESS_EQUAL ${CCACHE_MINIMUM_VERSION})
        message(${RECONFIGURE_MESSAGE_LEVEL} "Using ccache: no (found ${CCACHE_EXECUTABLE} (version ${CCACHE_VERSION}), the minimum required version is ${CCACHE_MINIMUM_VERSION}")
        return()
    endif()

    message(STATUS "Using ccache: ${CCACHE_EXECUTABLE} (version ${CCACHE_VERSION})")
    set(LAUNCHER ${CCACHE_EXECUTABLE})

    # Work around a well-intended but unfortunate behavior of ccache 4.0 & 4.1 with
    # environment variable SOURCE_DATE_EPOCH. This variable provides an alternative
    # to source-code embedded timestamps (__DATE__/__TIME__) and therefore helps with
    # reproducible builds (*). SOURCE_DATE_EPOCH is set automatically by the
    # distribution, e.g. Debian. Ccache 4.0 & 4.1 incorporate SOURCE_DATE_EPOCH into
    # the hash calculation regardless they contain timestamps or not. This invalidates
    # the cache whenever SOURCE_DATE_EPOCH changes. As a fix, ignore SOURCE_DATE_EPOCH.
    #
    # (*) https://reproducible-builds.org/specs/source-date-epoch/
    if (CCACHE_VERSION VERSION_GREATER_EQUAL "4.0" AND CCACHE_VERSION VERSION_LESS "4.2")
        message(STATUS "Ignore SOURCE_DATE_EPOCH for ccache 4.0 / 4.1")
        set(LAUNCHER env -u SOURCE_DATE_EPOCH ${CCACHE_EXECUTABLE})
    endif()
elseif(CCACHE_EXECUTABLE MATCHES "/sccache$")
    message(STATUS "Using sccache: ${CCACHE_EXECUTABLE}")
    set(LAUNCHER ${CCACHE_EXECUTABLE})
elseif(CCACHE_EXECUTABLE MATCHES "/chcache$")
    message(STATUS "Using chcache: ${CCACHE_EXECUTABLE}")
    set(LAUNCHER ${CCACHE_EXECUTABLE})
endif()

set (CMAKE_CXX_COMPILER_LAUNCHER ${LAUNCHER} ${CMAKE_CXX_COMPILER_LAUNCHER})
set (CMAKE_C_COMPILER_LAUNCHER ${LAUNCHER} ${CMAKE_C_COMPILER_LAUNCHER})
