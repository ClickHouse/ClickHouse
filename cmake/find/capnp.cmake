option (ENABLE_CAPNP "Enable Cap'n Proto" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CAPNP)
    if (USE_INTERNAL_CAPNP_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal capnproto library with ENABLE_CAPNP=OFF")
    endif()
    return()
endif()

option (USE_INTERNAL_CAPNP_LIBRARY "Set to FALSE to use system capnproto library instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/capnproto/CMakeLists.txt")
    if(USE_INTERNAL_CAPNP_LIBRARY)
        message(WARNING "submodule contrib/capnproto is missing. to fix try run: \n git submodule update --init")
        message(${RECONFIGURE_MESSAGE_LEVEL} "cannot find internal capnproto")
        set(USE_INTERNAL_CAPNP_LIBRARY 0)
    endif()
    set(MISSING_INTERNAL_CAPNP_LIBRARY 1)
endif()

# FIXME: refactor to use `add_library(… IMPORTED)` if possible.
if (NOT USE_INTERNAL_CAPNP_LIBRARY)
    find_library (KJ kj)
    find_library (CAPNP capnp)
    find_library (CAPNPC capnpc)

    if(KJ AND CAPNP AND CAPNPC)
        set (CAPNP_LIBRARIES ${CAPNPC} ${CAPNP} ${KJ})
    else()
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system capnproto")
    endif()
endif()

if (CAPNP_LIBRARIES)
    set (USE_CAPNP 1)
elseif(NOT MISSING_INTERNAL_CAPNP_LIBRARY)
    set (CAPNP_LIBRARIES capnpc)
    set (USE_CAPNP 1)
    set (USE_INTERNAL_CAPNP_LIBRARY 1)
endif ()

message (STATUS "Using capnp=${USE_CAPNP}: ${CAPNP_LIBRARIES}")
