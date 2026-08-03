set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/TurboPFor-CPP")

# abpfor — integer bit-packing codecs. Sources are architecture-independent;
# SIMD paths are selected at compile time via predefined macros (__AVX2__ etc.),
# so we only need to supply the right -march flags per file.
set(SRCS
    "${LIBRARY_DIR}/src/api_b128.cpp"
    "${LIBRARY_DIR}/src/api_b256.cpp"
    "${LIBRARY_DIR}/src/pack_dispatch.cpp"
    "${LIBRARY_DIR}/src/pack_dispatch_64.cpp"
    "${LIBRARY_DIR}/src/pack_dispatch_delta32.cpp"
    "${LIBRARY_DIR}/src/pack_dispatch_delta64.cpp"
)

add_library(_turbopfor_cpp ${SRCS})

# shortcut: these TUs generate large dispatch tables whose DWARF alone exceeds
# the 50MB check-large-objects limit (text is only ~2MB). Line tables keep
# stack traces usable while staying under the limit.
target_compile_options(_turbopfor_cpp PRIVATE -gline-tables-only)

target_include_directories(_turbopfor_cpp SYSTEM PUBLIC "${LIBRARY_DIR}/include")
target_include_directories(_turbopfor_cpp PRIVATE "${LIBRARY_DIR}/src")

# x86-64: abpfor requires AVX2 as a hard baseline for its interleaved codec.
# When ClickHouse targets a lower arch level, build with the scalar path
# explicitly allowed (correct, ~3-4x slower on decode).
if (ARCH_AMD64)
    if (DEFINED X86_ARCH_LEVEL AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 3)
        target_compile_options(_turbopfor_cpp PRIVATE -mavx2)
    else ()
        target_compile_definitions(_turbopfor_cpp PRIVATE ABPFOR_ALLOW_SCALAR_FALLBACK)
    endif ()
endif ()

add_library(ch_contrib::turbopfor_cpp ALIAS _turbopfor_cpp)
