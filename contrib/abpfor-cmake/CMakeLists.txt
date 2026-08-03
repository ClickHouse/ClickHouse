set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/abpfor")

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

add_library(_abpfor ${SRCS})

# Build without debug info. These TUs instantiate a bit-width-indexed dispatch
# table per codec, so DWARF dwarfs the code: ~2MB of .text carries ~46MB of
# DWARF. Largest object file, measured per build variant:
#
#                -g0   -gline-tables-only       -g
#   release    2.6MB                 17MB   48.6MB   <- 50MB limit is right here
#   debug      2.6MB                 17MB   49.1MB
#   ubsan      2.6MB                 17MB   48.6MB   (contrib gets -fno-sanitize=undefined)
#   tsan       8.0MB                   --   55.9MB
#   msan      11.1MB                   --   88.2MB
#   asan      14.7MB                   --   96.2MB
#
# utils/check-large-objects.sh rejects any object over 50MB, so release and
# debug sit one added function away from failing the build. abpfor is a leaf
# integer codec that never calls back into ClickHouse, so a frame with no line
# number costs nothing when reading a stack trace, and function names still
# resolve because -g0 leaves .symtab alone.
#
# Must come after the -g that CMAKE_CXX_FLAGS_<CONFIG> (release/debug) and
# cmake/sanitize.cmake (sanitizer builds) add, hence target_compile_options.
target_compile_options(_abpfor PRIVATE -g0)

target_include_directories(_abpfor SYSTEM PUBLIC "${LIBRARY_DIR}/include")
target_include_directories(_abpfor PRIVATE "${LIBRARY_DIR}/src")

# x86-64: abpfor's interleaved codec uses AVX2 when available. Below x86-64-v3
# (the compat build passes -DX86_ARCH_LEVEL=1) src/core/arch.h selects the
# scalar codec instead: same output bytes, ~3-4x slower on decode. The define
# only acknowledges abpfor's "you are on the slow path" #error — the library
# has to keep building at every arch level ClickHouse supports.
if (ARCH_AMD64)
    if (DEFINED X86_ARCH_LEVEL AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 3)
        target_compile_options(_abpfor PRIVATE -mavx2)
    else ()
        target_compile_definitions(_abpfor PRIVATE ABPFOR_ALLOW_SCALAR_FALLBACK)
    endif ()
endif ()

add_library(ch_contrib::abpfor ALIAS _abpfor)
