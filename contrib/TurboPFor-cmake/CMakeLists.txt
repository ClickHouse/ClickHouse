set(TURBOPFOR_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../TurboPFor-Integer-Compression)

add_library(_turbopfor
    ${TURBOPFOR_SOURCE_DIR}/lib/v8.c
    ${TURBOPFOR_SOURCE_DIR}/lib/vint.c
    ${TURBOPFOR_SOURCE_DIR}/lib/trlec.c
    ${TURBOPFOR_SOURCE_DIR}/lib/trled.c
    ${TURBOPFOR_SOURCE_DIR}/lib/vsimple.c
    ${TURBOPFOR_SOURCE_DIR}/lib/bitutil.c
    ${TURBOPFOR_SOURCE_DIR}/lib/bitpack.c
    ${TURBOPFOR_SOURCE_DIR}/lib/bitunpack.c
    ${TURBOPFOR_SOURCE_DIR}/lib/vp4c.c
    ${TURBOPFOR_SOURCE_DIR}/lib/vp4d.c
    ${TURBOPFOR_SOURCE_DIR}/lib/transpose.c
)

target_compile_options(_turbopfor PRIVATE "-w" "-fstrict-aliasing" "-falign-loops" "-fomit-frame-pointer")

# Disable MSAN for TurboPFor to avoid false positives from performance optimizations
# TurboPFor intentionally reads uninitialized memory in tight loops for performance,
# but guarantees correctness by only using valid portions of the data in final output.
# See: MSAN_TurboPFor_Analysis.md and MSAN_TurboPFor_Decode_Analysis.md for details.
if (SANITIZE STREQUAL "memory")
    target_compile_options(_turbopfor PRIVATE "-fno-sanitize=memory")
endif()

target_compile_definitions(_turbopfor PRIVATE SSE2_ON)
target_include_directories(_turbopfor SYSTEM PUBLIC ${TURBOPFOR_SOURCE_DIR}/lib/include_)
add_library(ch_contrib::turbopfor ALIAS _turbopfor)
