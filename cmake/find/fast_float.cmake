option (USE_FAST_FLOAT "Use fast_float" ${ENABLE_LIBRARIES})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/fast_float/include/fast_float/fast_float.h")
    message (WARNING "submodule contrib/fast_float is missing. to fix try run: \n git submodule update --init --recursive")
    if (USE_FAST_FLOAT)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal fast_float library")
    endif()
    return()
endif ()

if (USE_FAST_FLOAT)
    set(FAST_FLOAT_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/fast_float/include/")
endif ()

message(STATUS "Using fast_float=${USE_FAST_FLOAT}")
