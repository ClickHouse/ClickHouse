option (ENABLE_CAPNP "Enable Cap'n Proto" ON)

if (ENABLE_CAPNP)

    # cmake 3.5.1 bug:
    # capnproto uses this cmake feature:
    # target_compile_features(kj PUBLIC cxx_constexpr)
    # old cmake adds -std=gnu++11 to end of all compile commands (even if -std=gnu++17 already present in compile string)
    # cmake 3.9.1 (ubuntu artful) have no this bug (c++17 support added to cmake 3.8.2)
    if (CMAKE_VERSION VERSION_LESS "3.8.0")
       set (USE_INTERNAL_CAPNP_LIBRARY_DEFAULT 0)
       set (MISSING_INTERNAL_CAPNP_LIBRARY 1)
    else ()
       set (USE_INTERNAL_CAPNP_LIBRARY_DEFAULT ${NOT_UNBUNDLED})
    endif ()

    option (USE_INTERNAL_CAPNP_LIBRARY "Set to FALSE to use system capnproto library instead of bundled" ${USE_INTERNAL_CAPNP_LIBRARY_DEFAULT})

    if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/capnproto/c++/CMakeLists.txt")
       if (USE_INTERNAL_CAPNP_LIBRARY)
           message (WARNING "submodule contrib/capnproto is missing. to fix try run: \n git submodule update --init --recursive")
       endif ()
       set (USE_INTERNAL_CAPNP_LIBRARY 0)
       set (MISSING_INTERNAL_CAPNP_LIBRARY 1)
    endif ()

    if (NOT USE_INTERNAL_CAPNP_LIBRARY)
        set (CAPNP_PATHS "/usr/local/opt/capnp/lib")
        set (CAPNP_INCLUDE_PATHS "/usr/local/opt/capnp/include")
        find_library (CAPNP capnp PATHS ${CAPNP_PATHS})
        find_library (CAPNPC capnpc PATHS ${CAPNP_PATHS})
        find_library (KJ kj PATHS ${CAPNP_PATHS})
        set (CAPNP_LIBRARY ${CAPNP} ${CAPNPC} ${KJ})
        find_path (CAPNP_INCLUDE_DIR NAMES capnp/schema-parser.h PATHS ${CAPNP_INCLUDE_PATHS})
    endif ()

    if (CAPNP_INCLUDE_DIR AND CAPNP_LIBRARY)
        set(USE_CAPNP 1)
    elseif (NOT MISSING_INTERNAL_CAPNP_LIBRARY)
        set (USE_INTERNAL_CAPNP_LIBRARY 1)
        set (CAPNP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/capnproto/c++/src")
        set (CAPNP_LIBRARY capnpc)
        set (USE_CAPNP 1)
    endif ()

endif ()

if (USE_CAPNP)
    message (STATUS "Using capnp=${USE_CAPNP}: ${CAPNP_INCLUDE_DIR} : ${CAPNP_LIBRARY}")
else ()
    message (STATUS "Build without capnp (support for Cap'n Proto format will be disabled)")
endif ()
