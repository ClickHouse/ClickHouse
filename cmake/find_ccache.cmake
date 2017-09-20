
find_program (CCACHE_FOUND ccache)
if (CCACHE_FOUND AND NOT CMAKE_CXX_COMPILER_LAUNCHER MATCHES "ccache" AND NOT CMAKE_CXX_COMPILER MATCHES "ccache")
    execute_process(COMMAND ${CCACHE_FOUND} "-V" OUTPUT_VARIABLE CCACHE_VERSION)
    string(REGEX REPLACE "ccache version ([0-9\\.]+).*" "\\1" CCACHE_VERSION ${CCACHE_VERSION} )

    if (CCACHE_VERSION VERSION_GREATER "3.2.0" OR NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
       #message(STATUS "Using ${CCACHE_FOUND} ${CCACHE_VERSION}")
       set_property (GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_FOUND})
       set_property (GLOBAL PROPERTY RULE_LAUNCH_LINK ${CCACHE_FOUND})
    else ()
       message(STATUS "Not using ${CCACHE_FOUND} ${CCACHE_VERSION} bug: https://bugzilla.samba.org/show_bug.cgi?id=8118")
    endif ()
endif ()
