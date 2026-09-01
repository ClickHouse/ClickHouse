# Selected routines from ARM's optimized-routines (AOR), the upstream that
# glibc's aarch64 libm and string routines derive from. Built only for
# aarch64, and only where AOR measurably beats what the static binary
# otherwise resolves (benchmarked on Graviton4 / Neoverse-V2, see the perf
# comparison of PR 109239):
#
#   math (vs llvm-libc generic):   exp 2.1x, exp2 2.2x, log, log10,
#                                  atan 1.7x, atan2 1.6x
#   string (vs musl generic C, musl's vendored 2020 AOR asm, and llvm-libc):
#                                  memcpy +26-37%, memmove ~2x small sizes,
#                                  memcmp +50-80%, strlen 2.9x, memchr 3x
#
# The corresponding math sources are excluded from libllvmlibc on aarch64
# (see contrib/libllvmlibc-cmake), and this archive is linked with
# --whole-archive next to it (see cmake/linux/default_libs.cmake), so these
# definitions win the link the same way. The string asm is strong and wins
# over llvm-libc's weak memmove/memcmp; musl's own copies in libmusl.a are
# ordinary archive members and are simply never extracted.
#
# Accuracy: AOR math scalars are 0.5-2.5 ULP (the file names encode the
# bound, e.g. atan2_2u5.c = 2.5 ULP) instead of llvm-libc's correct
# rounding; this matches the accuracy class of glibc's aarch64 libm.
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
    "${AOR_SOURCE_DIR}/math/exp.c"
    "${AOR_SOURCE_DIR}/math/exp_data.c"
    "${AOR_SOURCE_DIR}/math/exp2.c"
    "${AOR_SOURCE_DIR}/math/log.c"
    "${AOR_SOURCE_DIR}/math/log_data.c"
    "${AOR_SOURCE_DIR}/math/math_err.c"
    "${AOR_SOURCE_DIR}/math/aarch64/experimental/atan_2u5.c"
    "${AOR_SOURCE_DIR}/math/aarch64/experimental/atan2_2u5.c"
    "${AOR_SOURCE_DIR}/math/aarch64/experimental/atan_data.c"
    "${AOR_SOURCE_DIR}/math/aarch64/experimental/log10_2u.c"
    "${AOR_SOURCE_DIR}/math/aarch64/experimental/log10_data.c"

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

# The atan/atan2/log10 scalars live under math/aarch64/experimental and their
# support structures in math_config.h are guarded by WANT_EXPERIMENTAL_MATH.
target_compile_definitions(aor PRIVATE WANT_EXPERIMENTAL_MATH=1)

# WANT_ROUNDING/WANT_ERRNO/HAVE_FAST_ROUND/HAVE_FAST_FMA take their correct
# values from math_config.h defaults on aarch64. The test_sig.h/test_defs.h
# includes in the sources are no-ops outside the AOR test harness but must
# resolve, hence math/include.
target_include_directories(aor PRIVATE
    "${AOR_SOURCE_DIR}/math"
    "${AOR_SOURCE_DIR}/math/include"
    "${AOR_SOURCE_DIR}/string/aarch64"
)

target_compile_options(aor PRIVATE -ffreestanding -fno-builtin)
