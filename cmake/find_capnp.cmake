option (ENABLE_CAPNP "Enable Cap'n Proto" ON)

if (ENABLE_CAPNP)

option (USE_INTERNAL_CAPNP_LIBRARY "Set to FALSE to use system capnproto library instead of bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_CAPNP_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/capnproto/c++/CMakeLists.txt")
   message (WARNING "submodule contrib/capnproto is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_CAPNP_LIBRARY 0)
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
    elseif (NOT MISSING_INTERNAL_RDKAFKA_LIBRARY)
        set (USE_INTERNAL_CAPNP_LIBRARY 1)
        set (CAPNP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/capnproto/c++/src")
        set (CAPNP_LIBRARY capnpc)
	    set(USE_CAPNP 1)
	endif ()

endif ()

if (USE_CAPNP)
    message (STATUS "Using capnp=${USE_CAPNP}: ${CAPNP_INCLUDE_DIR} : ${CAPNP_LIBRARY}")
else ()
    message (STATUS "Build without capnp (support for Cap'n Proto format will be disabled)")
endif ()
