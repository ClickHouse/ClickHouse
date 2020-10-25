option (ENABLE_MSGPACK "Enable msgpack library" ${ENABLE_LIBRARIES})

if (ENABLE_MSGPACK)

option (USE_INTERNAL_MSGPACK_LIBRARY "Set to FALSE to use system msgpack library instead of bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_MSGPACK_LIBRARY)
    if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/msgpack-c/include/msgpack.hpp")
        message(WARNING "Submodule contrib/msgpack-c is missing. To fix try run: \n git submodule update --init --recursive")
        set(USE_INTERNAL_MSGPACK_LIBRARY     0)
        set(MISSING_INTERNAL_MSGPACK_LIBRARY 1)
    endif()
endif()

if (USE_INTERNAL_MSGPACK_LIBRARY)
    set(MSGPACK_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/msgpack-c/include)
else()
    find_path(MSGPACK_INCLUDE_DIR NAMES msgpack.hpp PATHS ${MSGPACK_INCLUDE_PATHS})
endif()

if (MSGPACK_INCLUDE_DIR)
    set(USE_MSGPACK 1)
endif()

endif()

message(STATUS "Using msgpack=${USE_MSGPACK}: ${MSGPACK_INCLUDE_DIR}")
