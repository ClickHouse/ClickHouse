set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/fsst")
set (SRCS
    "${LIBRARY_DIR}/libfsst.cpp"
    "${LIBRARY_DIR}/fsst_avx512.cpp"
    "${LIBRARY_DIR}/fsst_avx512_unroll1.inc"
    "${LIBRARY_DIR}/fsst_avx512_unroll2.inc"
    "${LIBRARY_DIR}/fsst_avx512_unroll3.inc"
    "${LIBRARY_DIR}/fsst_avx512_unroll4.inc"
)

add_library (fsst ${SRCS})

if (SANITIZE STREQUAL "undefined")
    target_compile_options (fsst PRIVATE -fno-sanitize=undefined)
endif ()
target_include_directories(fsst PUBLIC "${LIBRARY_DIR}/lib")
