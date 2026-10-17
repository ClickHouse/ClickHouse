# String routines from ARM's optimized-routines (AOR), the upstream that
# glibc's aarch64 ifunc-selected string routines derive from. Built only for
# aarch64, where AOR measurably beats what the static binary otherwise
# resolves - musl generic C, musl's vendored 2020 AOR asm, and llvm-libc weak
# definitions (benchmarked on Graviton4 / Neoverse-V2, see the perf
# comparison of PR 109239): memcpy +26-37%, memmove ~2x small sizes,
# memcmp +50-80%, strlen 2.9x, memchr 3x.
#
# This archive is linked with --whole-archive next to libllvmlibc (see
# cmake/linux/default_libs.cmake), so the definitions win the link
# structurally. The string asm is strong and wins over llvm-libc's weak
# memmove/memcmp; musl's own copies in libmusl.a are ordinary archive
# members and are simply never extracted.
#
# Math routines are deliberately NOT taken from AOR even though its scalar
# exp/exp2/log/log10/atan/atan2 are 1.6-2.2x faster than llvm-libc's on
# Neoverse: they are 0.5-2.5 ULP instead of correctly rounded, and since
# x86_64 keeps llvm-libc's correctly-rounded math, the same query would
# print different last digits per architecture. The stateless tests caught
# exactly that (s2 functions via atan2, log10(0.1) != -1). Correct rounding
# is what makes results architecture-independent; keep it.
#
# The *-mte variants are used for strlen/memchr/strchr: they never read
# beyond the granule containing the terminator, are valid on any core, and
# measured fastest on Neoverse-V2. The SVE variants are deliberately not
# used: on 128-bit-SVE cores (Graviton4) they are up to 3x slower than the
# NEON/MTE ones.
#
# On x86_64 llvm-libc's implementations (built with AVX2/FMA) are
# competitive, so this library is not built there.

if (NOT ARCH_AARCH64)
    return()
endif()

set(AOR_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/optimized-routines")

set(SRCS
    "${AOR_SOURCE_DIR}/string/aarch64/memcpy-advsimd.S"
    "${AOR_SOURCE_DIR}/string/aarch64/memcmp.S"
    "${AOR_SOURCE_DIR}/string/aarch64/memchr-mte.S"
    "${AOR_SOURCE_DIR}/string/aarch64/strlen-mte.S"
    "${AOR_SOURCE_DIR}/string/aarch64/strcmp.S"
    "${AOR_SOURCE_DIR}/string/aarch64/strchr-mte.S"
    "${AOR_SOURCE_DIR}/string/aarch64/strnlen.S"
)

add_library(aor ${SRCS})
set_target_properties(aor PROPERTIES FOLDER "contrib/optimized-routines-cmake")

# AOR asm entry points carry internal names (__memcpy_aarch64_simd, ...);
# rename them to the public symbols during preprocessing of the .S files.
set_source_files_properties("${AOR_SOURCE_DIR}/string/aarch64/memcpy-advsimd.S"
    PROPERTIES COMPILE_DEFINITIONS "__memcpy_aarch64_simd=memcpy;__memmove_aarch64_simd=memmove")
set_source_files_properties("${AOR_SOURCE_DIR}/string/aarch64/memcmp.S"
    PROPERTIES COMPILE_DEFINITIONS "__memcmp_aarch64=memcmp")
set_source_files_properties("${AOR_SOURCE_DIR}/string/aarch64/memchr-mte.S"
    PROPERTIES COMPILE_DEFINITIONS "__memchr_aarch64_mte=memchr")
set_source_files_properties("${AOR_SOURCE_DIR}/string/aarch64/strlen-mte.S"
    PROPERTIES COMPILE_DEFINITIONS "__strlen_aarch64_mte=strlen")
set_source_files_properties("${AOR_SOURCE_DIR}/string/aarch64/strcmp.S"
    PROPERTIES COMPILE_DEFINITIONS "__strcmp_aarch64=strcmp")
set_source_files_properties("${AOR_SOURCE_DIR}/string/aarch64/strchr-mte.S"
    PROPERTIES COMPILE_DEFINITIONS "__strchr_aarch64_mte=strchr")
set_source_files_properties("${AOR_SOURCE_DIR}/string/aarch64/strnlen.S"
    PROPERTIES COMPILE_DEFINITIONS "__strnlen_aarch64=strnlen")

target_include_directories(aor PRIVATE
    "${AOR_SOURCE_DIR}/string/aarch64"
)

target_compile_options(aor PRIVATE -ffreestanding -fno-builtin)
