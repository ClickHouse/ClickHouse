option (USE_CAPNP "Enable Cap'n Proto" ON)

if (USE_CAPNP)
    option (USE_INTERNAL_CAPNP_LIBRARY "Set to FALSE to use system capnproto library instead of bundled" ${NOT_UNBUNDLED})

    # FIXME: refactor to use `add_library(… IMPORTED)` if possible.
    if (NOT USE_INTERNAL_CAPNP_LIBRARY)
        find_library (KJ kj)
        find_library (CAPNP capnp)
        find_library (CAPNPC capnpc)

        set (CAPNP_LIBRARIES ${CAPNPC} ${CAPNP} ${KJ})
    else ()
        add_subdirectory(contrib/capnproto-cmake)

        set (CAPNP_LIBRARIES capnpc)
    endif ()

    message (STATUS "Using capnp: ${CAPNP_LIBRARIES}")
endif ()
