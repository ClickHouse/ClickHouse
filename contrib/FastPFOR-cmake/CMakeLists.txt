# FastPFOR requires SSE4.2 on x86-64. On AArch64 it uses native NEON
# intrinsics (see headers/fastpfor_neon.h). Other architectures are
# not supported.
if ((ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 2) OR ARCH_AARCH64)
    option (ENABLE_FASTPFOR "Enable FastPFOR" ${ENABLE_LIBRARIES})
endif()

if (NOT ENABLE_FASTPFOR)
    message (STATUS "Not using FastPFOR")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/FastPFOR")

set (SRCS
    "${LIBRARY_DIR}/src/bitpacking.cpp"
    "${LIBRARY_DIR}/src/bitpackingaligned.cpp"
    "${LIBRARY_DIR}/src/bitpackingunaligned.cpp"
    "${LIBRARY_DIR}/src/horizontalbitpacking.cpp"
    "${LIBRARY_DIR}/src/simdunalignedbitpacking.cpp"
    "${LIBRARY_DIR}/src/codecfactory.cpp"
    "${LIBRARY_DIR}/src/simdbitpacking.cpp"
    "${LIBRARY_DIR}/src/varintdecode.c"
    "${LIBRARY_DIR}/src/streamvbyte.c"
)

add_library (_fastpfor ${SRCS})

# The library keeps its public headers in "headers/", but the installed
# layout (and therefore the conventional include style) is <fastpfor/*.h>.
# Recreate that layout via a symlink in the binary directory.
set (FASTPFOR_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/include")
file (MAKE_DIRECTORY "${FASTPFOR_INCLUDE_DIR}")
file (CREATE_LINK "${LIBRARY_DIR}/headers" "${FASTPFOR_INCLUDE_DIR}/fastpfor" SYMBOLIC COPY_ON_ERROR)

target_include_directories (_fastpfor PRIVATE "${LIBRARY_DIR}/headers")
target_include_directories (_fastpfor SYSTEM BEFORE PUBLIC "${FASTPFOR_INCLUDE_DIR}")

add_library (ch_contrib::fastpfor ALIAS _fastpfor)
