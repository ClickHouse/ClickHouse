if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/fast_float/include/fast_float/fast_float.h")
    message (FATAL_ERROR "submodule contrib/fast_float is missing. to fix try run: \n git submodule update --init --recursive")
endif ()

set(FAST_FLOAT_LIBRARY fast_float)
set(FAST_FLOAT_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/fast_float/include/")
