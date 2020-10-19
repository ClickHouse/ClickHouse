if (CMAKE_CXX_COMPILER_LAUNCHER MATCHES "ccache" OR CMAKE_CXX_COMPILER_LAUNCHER MATCHES "ccache")
    set(COMPILER_MATCHES_CCACHE 1)
else()
    set(COMPILER_MATCHES_CCACHE 0)
endif()

if ((ENABLE_CCACHE OR NOT DEFINED ENABLE_CCACHE) AND NOT COMPILER_MATCHES_CCACHE)
    find_program (CCACHE_FOUND ccache)
    if (CCACHE_FOUND)
        set(ENABLE_CCACHE_BY_DEFAULT 1)
    else()
        set(ENABLE_CCACHE_BY_DEFAULT 0)
    endif()
endif()

if (NOT CCACHE_FOUND AND NOT DEFINED ENABLE_CCACHE AND NOT COMPILER_MATCHES_CCACHE)
    message(WARNING "CCache is not found. We recommend setting it up if you build ClickHouse from source often. "
            "Setting it up will significantly reduce compilation time for 2nd and consequent builds")
endif()

# https://ccache.dev/
option(ENABLE_CCACHE "Speedup re-compilations using ccache (external tool)" ${ENABLE_CCACHE_BY_DEFAULT})

if (NOT ENABLE_CCACHE)
    return()
endif()

if (CCACHE_FOUND AND NOT COMPILER_MATCHES_CCACHE)
   execute_process(COMMAND ${CCACHE_FOUND} "-V" OUTPUT_VARIABLE CCACHE_VERSION)
   string(REGEX REPLACE "ccache version ([0-9\\.]+).*" "\\1" CCACHE_VERSION ${CCACHE_VERSION})

   if (CCACHE_VERSION VERSION_GREATER "3.2.0" OR NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
      message(STATUS "Using ${CCACHE_FOUND} ${CCACHE_VERSION}")
      set_property (GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_FOUND})
      set_property (GLOBAL PROPERTY RULE_LAUNCH_LINK ${CCACHE_FOUND})
   else ()
      message(${RECONFIGURE_MESSAGE_LEVEL} "Not using ${CCACHE_FOUND} ${CCACHE_VERSION} bug: https://bugzilla.samba.org/show_bug.cgi?id=8118")
   endif ()
elseif (NOT CCACHE_FOUND AND NOT COMPILER_MATCHES_CCACHE)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use ccache")
endif ()
