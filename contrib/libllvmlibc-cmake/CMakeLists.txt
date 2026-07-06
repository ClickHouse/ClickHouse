set(LIBLLVMLIBC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/libc")

# Only build on supported architectures
if (NOT (ARCH_AMD64 OR ARCH_AARCH64))
    return()
endif()

set(SRCS
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/acos.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/asin.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/asinf.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/atan.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/atan2.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/cbrt.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/ceill.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/copysignl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/cos.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/exp.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/exp2.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/expm1.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/fabs.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/fabsl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/floor.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/floorl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/fma.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/fmod.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/fmodl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/frexp.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/frexpl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/hypot.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/ldexp.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/ldexpf.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/ldexpl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/llround.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/log.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/log10.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/log1p.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/log2.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/logf.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/lround.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/modf.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/nan.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/nanf.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/nanl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/nextafter.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/pow.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/scalbn.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/scalbnl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/sin.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/sqrt.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/sqrtf.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/sqrtl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/tan.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/truncl.cpp"
)

set(LIBC_NAMESPACE "__llvm_libc")
set(LLVMLIBC_DEFS_COMMON
    LIBC_MATH_HAS_NO_ERRNO=1
    LIBC_MATH_HAS_NO_EXCEPT=1
    LIBC_MATH_HAS_SKIP_ACCURATE_PASS=1
    LIBC_ERRNO_MODE=6 # Needed to make building with -O0 work. Details in https://github.com/ClickHouse/ClickHouse/pull/100023
)

if (ARCH_AMD64)
    add_library(libllvmlibc ${SRCS})
    set_target_properties(libllvmlibc PROPERTIES FOLDER "contrib/libllvmlibc-cmake")

    if (X86_ARCH_LEVEL VERSION_LESS 3)
        # Compat build: SSE2 baseline
        target_compile_definitions(libllvmlibc PRIVATE
            ${LLVMLIBC_DEFS_COMMON}
            LIBC_NAMESPACE=${LIBC_NAMESPACE}
            LIBC_COPT_PUBLIC_PACKAGING=1
            LIBC_TARGET_CPU_HAS_SSE2=1
            LIBC_TARGET_CPU_HAS_FPU_FLOAT=1
            LIBC_TARGET_CPU_HAS_FPU_DOUBLE=1
        )
    else()
        # v3+: single library with AVX/AVX2/FMA
        target_compile_definitions(libllvmlibc PRIVATE
            ${LLVMLIBC_DEFS_COMMON}
            LIBC_NAMESPACE=${LIBC_NAMESPACE}
            LIBC_COPT_PUBLIC_PACKAGING=1
            LIBC_TARGET_CPU_HAS_SSE2=1
            LIBC_TARGET_CPU_HAS_SSE4_2=1
            LIBC_TARGET_CPU_HAS_FPU_FLOAT=1
            LIBC_TARGET_CPU_HAS_FPU_DOUBLE=1
            LIBC_TARGET_CPU_HAS_FMA=1
            LIBC_TARGET_CPU_HAS_FMA_FLOAT=1
            LIBC_TARGET_CPU_HAS_FMA_DOUBLE=1
            LIBC_TARGET_CPU_HAS_NEAREST_INT=1
            LIBC_TARGET_CPU_HAS_AVX=1
            LIBC_TARGET_CPU_HAS_AVX2=1
        )
    endif()

    target_include_directories(libllvmlibc SYSTEM BEFORE
        PRIVATE $<BUILD_INTERFACE:${LIBLLVMLIBC_SOURCE_DIR}>
    )

elseif (ARCH_AARCH64)
    # aarch64: Single variant, FMA is always available (ARMv8 baseline)
    # Uses LIBC_COPT_PUBLIC_PACKAGING to emit public C symbols directly
    add_library(libllvmlibc ${SRCS})
    set_target_properties(libllvmlibc PROPERTIES FOLDER "contrib/libllvmlibc-cmake")
    target_compile_definitions(libllvmlibc PRIVATE
        ${LLVMLIBC_DEFS_COMMON}
        LIBC_NAMESPACE=${LIBC_NAMESPACE}
        LIBC_COPT_PUBLIC_PACKAGING=1
        LIBC_TARGET_CPU_HAS_FMA=1
        LIBC_TARGET_CPU_HAS_FMA_DOUBLE=1
        LIBC_TARGET_CPU_HAS_FMA_HALF=1
        LIBC_TARGET_CPU_HAS_FMA_FLOAT=1
        LIBC_TARGET_CPU_HAS_NEAREST_INT=1
        LIBC_TARGET_CPU_HAS_FULLFP16=1
        LIBC_TARGET_CPU_HAS_FAST_FLOAT16_OPS=1
    )
    target_include_directories(libllvmlibc SYSTEM BEFORE
        PRIVATE $<BUILD_INTERFACE:${LIBLLVMLIBC_SOURCE_DIR}>
    )

endif()
