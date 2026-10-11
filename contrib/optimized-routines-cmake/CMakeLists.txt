# String routines from ARM's optimized-routines (AOR), the upstream that
# glibc's aarch64 ifunc-selected string routines derive from. Built only for
# aarch64, where AOR measurably beats what the binary otherwise resolves -
# llvm-libc's weak generic definitions, or with musl its generic C and vendored
# 2020 AOR asm (benchmarked on Graviton4 / Neoverse-V2, see the perf comparison
# of PR 109239): memcpy +26-37%, memmove ~2x small sizes, memcmp +50-80%,
# strlen 2.9x, memchr 3x. Against glibc the routines are the same code, minus
# the ifunc indirection.
#
# This archive is linked with --whole-archive next to libllvmlibc (see
# cmake/linux/default_libs.cmake), so the definitions win the link
# structurally. The string asm is strong and wins over llvm-libc's weak
# memmove/memcmp; the libc's own copies are never extracted from its archive
# or, with a dynamic glibc, never bound since the symbols are already defined.
#
# Math routines are deliberately NOT taken from AOR even though its scalar
# exp/exp2/log/log10/atan/atan2 are 1.6-2.2x faster than llvm-libc's on
# Neoverse: they are 0.5-2.5 ULP instead of correctly rounded, and since
# x86_64 keeps llvm-libc's correctly-rounded math, the same query would
# print different last digits per architecture. The stateless tests caught
# exactly that (s2 functions via atan2, log10(0.1) != -1). Correct rounding
# is what makes results architecture-independent; keep it.
#
# The *-mte variants are used for memchr/strchr: they never read beyond the
# granule containing the terminator, are valid on any core, and measured
# fastest on Neoverse-V2. strlen uses the plain strlen.S: strlen-mte.S is
# 20-40% slower above 512 bytes and ~20% slower on mixed sizes, while
# strlen.S matches glibc's __strlen_asimd, and ClickHouse does not enable
# MTE. The SVE str*/memchr variants are deliberately not used: on 128-bit-SVE
# cores (Graviton4) they are up to 3x slower than the NEON/MTE ones.
#
# memcpy/memmove are the exception and are selected at runtime, see
# memcpy_dispatch.c. The AdvSIMD memcpy alone was 12-17% slower than glibc's
# ifunc-selected __memcpy_sve on Graviton 3/4 (isValidUTF8, upperUTF8,
# Parquet/ORC reads in the perf suite); memcpy-sve.S, which uses SVE only for
# the small-copy tail, is on par with it. SVE is not in the aarch64 baseline,
# so both variants are built and a trampoline picks one on first call. It is
# a function pointer rather than an ifunc because a static musl binary cannot
# process IRELATIVE relocations.
#
# On x86_64 llvm-libc's implementations (built with AVX2/FMA) are
# competitive, so this library is not built there. It is not built under
# sanitizers either, where these functions must stay with the libc so the
# interceptors wrap them, and the target must not exist for the Rust symbol
# localization in contrib/corrosion-cmake to pick up as a reference archive.

if (NOT ARCH_AARCH64 OR NOT ENABLE_LLVM_LIBC_MATH OR SANITIZE)
    return()
endif()

set(AOR_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/optimized-routines")

set(SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/memcpy_dispatch.c"
    "${AOR_SOURCE_DIR}/string/aarch64/memcpy-advsimd.S"
    "${AOR_SOURCE_DIR}/string/aarch64/memcpy-sve.S"
    "${AOR_SOURCE_DIR}/string/aarch64/memcmp.S"
    "${AOR_SOURCE_DIR}/string/aarch64/memchr-mte.S"
    "${AOR_SOURCE_DIR}/string/aarch64/strlen.S"
    "${AOR_SOURCE_DIR}/string/aarch64/strcmp.S"
    "${AOR_SOURCE_DIR}/string/aarch64/strchr-mte.S"
    "${AOR_SOURCE_DIR}/string/aarch64/strnlen.S"
)

add_library(aor ${SRCS})
set_target_properties(aor PROPERTIES FOLDER "contrib/optimized-routines-cmake")

# AOR asm entry points carry internal names (__memcmp_aarch64, ...); rename
# them to the public symbols during preprocessing of the .S files. The two
# memcpy variants keep their internal names: the public memcpy/memmove are
# the trampolines in memcpy_dispatch.c.
set_source_files_properties("${AOR_SOURCE_DIR}/string/aarch64/memcmp.S"
    PROPERTIES COMPILE_DEFINITIONS "__memcmp_aarch64=memcmp")
set_source_files_properties("${AOR_SOURCE_DIR}/string/aarch64/memchr-mte.S"
    PROPERTIES COMPILE_DEFINITIONS "__memchr_aarch64_mte=memchr")
set_source_files_properties("${AOR_SOURCE_DIR}/string/aarch64/strlen.S"
    PROPERTIES COMPILE_DEFINITIONS "__strlen_aarch64=strlen")
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
