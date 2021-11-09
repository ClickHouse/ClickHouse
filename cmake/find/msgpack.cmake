option (ENABLE_MSGPACK "Enable msgpack library" ${ENABLE_LIBRARIES})

if(NOT ENABLE_MSGPACK)
    if(USE_INTERNAL_MSGPACK_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use internal msgpack with ENABLE_MSGPACK=OFF")
    endif()
    return()
endif()

option (USE_INTERNAL_MSGPACK_LIBRARY "Set to FALSE to use system msgpack library instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/msgpack-c/include/msgpack.hpp")
    if(USE_INTERNAL_MSGPACK_LIBRARY)
        message(WARNING "Submodule contrib/msgpack-c is missing. To fix try run: \n git submodule update --init")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use internal msgpack")
        set(USE_INTERNAL_MSGPACK_LIBRARY 0)
    endif()
    set(MISSING_INTERNAL_MSGPACK_LIBRARY 1)
endif()

if(NOT USE_INTERNAL_MSGPACK_LIBRARY)
    find_path(MSGPACK_INCLUDE_DIR NAMES msgpack.hpp PATHS ${MSGPACK_INCLUDE_PATHS})
    if(NOT MSGPACK_INCLUDE_DIR)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system msgpack")
    endif()
endif()

if(NOT MSGPACK_INCLUDE_DIR AND NOT MISSING_INTERNAL_MSGPACK_LIBRARY)
    set(MSGPACK_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/msgpack-c/include")
    set(USE_INTERNAL_MSGPACK_LIBRARY 1)
endif()

if (MSGPACK_INCLUDE_DIR)
    set(USE_MSGPACK 1)
endif()

message(STATUS "Using msgpack=${USE_MSGPACK}: ${MSGPACK_INCLUDE_DIR}")
