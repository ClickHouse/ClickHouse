set(LIBLLVMLIBC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/libc")

# Only build on supported architectures
if (NOT (ARCH_AMD64 OR ARCH_AARCH64))
    return()
endif()

# List of exported functions should be also updated in ci/jobs/compatibility_check.py
set(SRCS
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/common_constants.cpp"

    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/acos.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/asin.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/asinf.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/atan.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/atan2.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/cbrt.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/ceill.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/cos.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/floorl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/fma.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/fmod.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/frexp.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/frexpl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/hypot.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/ldexp.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/ldexpf.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/ldexpl.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/llround.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/log10.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/log1p.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/lround.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/modf.cpp"
    "${LIBLLVMLIBC_SOURCE_DIR}/src/math/generic/nextafter.cpp"
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
)

if (ARCH_AMD64)
    if (X86_ARCH_LEVEL VERSION_LESS 2)
        # Compat build: Single library without dispatch, minimal CPU features (SSE2 baseline)
        add_library(libllvmlibc ${SRCS})
        set_target_properties(libllvmlibc PROPERTIES FOLDER "contrib/libllvmlibc-cmake")
        target_compile_definitions(libllvmlibc PRIVATE
            ${LLVMLIBC_DEFS_COMMON}
            LIBC_NAMESPACE=${LIBC_NAMESPACE}
            LIBC_COPT_PUBLIC_PACKAGING=1
            LIBC_TARGET_CPU_HAS_SSE2=1
            LIBC_TARGET_CPU_HAS_FPU_FLOAT=1
            LIBC_TARGET_CPU_HAS_FPU_DOUBLE=1
        )
        target_include_directories(libllvmlibc SYSTEM BEFORE
            PRIVATE $<BUILD_INTERFACE:${LIBLLVMLIBC_SOURCE_DIR}>
        )
    else()
        # x86-64: Build two variants for runtime dispatch
        # v3 = AVX/AVX2/FMA (Haswell+)
        # v2 = SSE4.2 (Nehalem+)

        # x86-64-v3 variant (with FMA)
        add_library(libllvmlibc_x86_64_v3 ${SRCS})
        set_target_properties(libllvmlibc_x86_64_v3 PROPERTIES FOLDER "contrib/libllvmlibc-cmake")
        target_compile_options(libllvmlibc_x86_64_v3 PRIVATE -mfma)
        target_compile_definitions(libllvmlibc_x86_64_v3 PRIVATE
            ${LLVMLIBC_DEFS_COMMON}
            LIBC_NAMESPACE=${LIBC_NAMESPACE}__x86_64_v3
            LIBC_TARGET_CPU_HAS_SSE2=1
            LIBC_TARGET_CPU_HAS_SSE4_2=1
            LIBC_TARGET_CPU_HAS_FPU_FLOAT=1
            LIBC_TARGET_CPU_HAS_FPU_DOUBLE=1
            LIBC_TARGET_CPU_HAS_FMA=1
            LIBC_TARGET_CPU_HAS_FMA_DOUBLE=1
            LIBC_TARGET_CPU_HAS_NEAREST_INT=1
            LIBC_TARGET_CPU_HAS_AVX=1
            LIBC_TARGET_CPU_HAS_AVX2=1
        )
        target_include_directories(libllvmlibc_x86_64_v3 SYSTEM BEFORE
            PRIVATE $<BUILD_INTERFACE:${LIBLLVMLIBC_SOURCE_DIR}>
        )

        # x86-64-v2 variant (SSE4.2, no FMA)
        add_library(libllvmlibc_x86_64_v2 ${SRCS})
        set_target_properties(libllvmlibc_x86_64_v2 PROPERTIES FOLDER "contrib/libllvmlibc-cmake")
        target_compile_options(libllvmlibc_x86_64_v2 PRIVATE -msse4.2)
        target_compile_definitions(libllvmlibc_x86_64_v2 PRIVATE
            ${LLVMLIBC_DEFS_COMMON}
            LIBC_NAMESPACE=${LIBC_NAMESPACE}__x86_64_v2
            LIBC_TARGET_CPU_HAS_SSE2=1
            LIBC_TARGET_CPU_HAS_SSE4_2=1
            LIBC_TARGET_CPU_HAS_FPU_FLOAT=1
            LIBC_TARGET_CPU_HAS_FPU_DOUBLE=1
            LIBC_TARGET_CPU_HAS_NEAREST_INT=1
        )
        target_include_directories(libllvmlibc_x86_64_v2 SYSTEM BEFORE
            PRIVATE $<BUILD_INTERFACE:${LIBLLVMLIBC_SOURCE_DIR}>
        )

        add_library(llvmlibc_dispatch STATIC
            "${CMAKE_CURRENT_SOURCE_DIR}/llvmlibc_dispatch.cpp"
        )
        target_include_directories(llvmlibc_dispatch PRIVATE
            ${ClickHouse_SOURCE_DIR}/src
            ${ClickHouse_SOURCE_DIR}/base
        )

        add_library(libllvmlibc INTERFACE)
        target_link_libraries(libllvmlibc INTERFACE
            llvmlibc_dispatch
            libllvmlibc_x86_64_v3
            libllvmlibc_x86_64_v2
        )
    endif()

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
