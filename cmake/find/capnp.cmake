option (ENABLE_CAPNP "Enable Cap'n Proto" ${ENABLE_LIBRARIES})

if (ENABLE_CAPNP)

option (USE_INTERNAL_CAPNP_LIBRARY "Set to FALSE to use system capnproto library instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/capnproto/CMakeLists.txt")
    if(USE_INTERNAL_CAPNP_LIBRARY)
        message(WARNING "submodule contrib/capnproto is missing. to fix try run: \n git submodule update --init --recursive")
    endif()
    set(MISSING_INTERNAL_CAPNP_LIBRARY 1)
    set(USE_INTERNAL_CAPNP_LIBRARY 0)
endif()

# FIXME: refactor to use `add_library(… IMPORTED)` if possible.
if (NOT USE_INTERNAL_CAPNP_LIBRARY)
    find_library (KJ kj)
    find_library (CAPNP capnp)
    find_library (CAPNPC capnpc)

    set (CAPNP_LIBRARIES ${CAPNPC} ${CAPNP} ${KJ})
elseif(NOT MISSING_INTERNAL_CAPNP_LIBRARY)
    add_subdirectory(contrib/capnproto-cmake)

    set (CAPNP_LIBRARIES capnpc)
endif ()

if (CAPNP_LIBRARIES)
    set (USE_CAPNP 1)
endif ()

endif ()

message (STATUS "Using capnp=${USE_CAPNP}: ${CAPNP_LIBRARIES}")
