option (ENABLE_STREAMVBYTE "Enable StreamVBte" ${ENABLE_LIBRARIES})

if (NOT ENABLE_STREAMVBYTE)
    message(STATUS "Not using streamvbyte")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/streamvbyte")

set (SRCS
    "${LIBRARY_DIR}/src/streamvbyte_0124_decode.c"
    "${LIBRARY_DIR}/src/streamvbyte_0124_encode.c"
    "${LIBRARY_DIR}/src/streamvbyte_arm_decode.c"
    "${LIBRARY_DIR}/src/streamvbyte_arm_encode.c"
    "${LIBRARY_DIR}/src/streamvbyte_decode.c"
    "${LIBRARY_DIR}/src/streamvbyte_encode.c"
    "${LIBRARY_DIR}/src/streamvbyte_x64_decode.c"
    "${LIBRARY_DIR}/src/streamvbyte_x64_encode.c"
    "${LIBRARY_DIR}/src/streamvbyte_zigzag.c"
    "${LIBRARY_DIR}/src/streamvbytedelta_decode.c"
    "${LIBRARY_DIR}/src/streamvbytedelta_encode.c"
    "${LIBRARY_DIR}/src/streamvbytedelta_x64_decode.c"
    "${LIBRARY_DIR}/src/streamvbytedelta_x64_encode.c"
    "${LIBRARY_DIR}/src/streamvbytedelta.c"
)

add_library (_streamvbyte ${SRCS})

target_include_directories (_streamvbyte SYSTEM PUBLIC
    "${LIBRARY_DIR}/include"
    "${LIBRARY_DIR}/src"
)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options (_streamvbyte PRIVATE -g0)
endif()

target_compile_options(_streamvbyte PRIVATE
        -Wno-unused-parameter
        -Wno-sign-compare
        -Wno-missing-field-initializers
        -Wno-unused-function
        -Wno-unused-variable
        -Wno-implicit-fallthrough
        -Wno-implicit-function-declaration
)

add_library (ch_contrib::streamvbyte ALIAS _streamvbyte)
