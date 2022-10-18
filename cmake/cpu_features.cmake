# https://software.intel.com/sites/landingpage/IntrinsicsGuide/

include (CheckCXXSourceCompiles)
include (CMakePushCheckState)

cmake_push_check_state ()

# The variables HAVE_* determine if compiler has support for the flag to use the corresponding instruction set.
# The options ENABLE_* determine if we will tell compiler to actually use the corresponding instruction set if compiler can do it.

# All of them are unrelated to the instruction set at the host machine
# (you can compile for newer instruction set on old machines and vice versa).

option (ARCH_NATIVE "Add -march=native compiler flag. This makes your binaries non-portable but more performant code may be generated. This option overrides ENABLE_* options for specific instruction set. Highly not recommended to use." 0)

if (ARCH_NATIVE)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=native")

elseif (ARCH_AARCH64)
    # ARM publishes almost every year a new revision of it's ISA [1]. Each version comes with new mandatory and optional features from
    # which CPU vendors can pick and choose. This creates a lot of variability ... We provide two build "profiles", one for maximum
    # compatibility intended to run on all 64-bit ARM hardware released after 2013 (e.g. Raspberry Pi 4), and one for modern ARM server
    # CPUs, (e.g. Graviton).
    #
    # [1] https://en.wikipedia.org/wiki/AArch64
    option (NO_ARMV81_OR_HIGHER "Disable ARMv8.1 or higher on Aarch64 for maximum compatibility with older/embedded hardware." 0)

    if (NO_ARMV81_OR_HIGHER)
        # crc32 is optional in v8.0 and mandatory in v8.1. Enable it as __crc32()* is used in lot's of places and even very old ARM CPUs
        # support it.
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=armv8+crc")
    else ()
        # ARMv8.2 is quite ancient but the lowest common denominator supported by both Graviton 2 and 3 processors [1]. In particular, it
        # includes LSE (made mandatory with ARMv8.1) which provides nice speedups without having to fall back to compat flag
        # "-moutline-atomics" for v8.0 [2, 3, 4] that requires a recent glibc with runtime dispatch helper, limiting our ability to run on
        # old OSs.
        #
        # simd:    NEON, introduced as optional in v8.0, A few extensions were added with v8.1 but it's still not mandatory. Enables the
        #          compiler to auto-vectorize.
        # sve:     Scalable Vector Extensions, introduced as optional in v8.2. Available in Graviton 3 but not in Graviton 2, and most likely
        #          also not in CI machines. Compiler support for autovectorization is rudimentary at the time of writing, see [5]. Can be
        #          enabled one-fine-day (TM) but not now.
        # ssbs:    "Speculative Store Bypass Safe". Optional in v8.0, mandatory in v8.5. Meltdown/spectre countermeasure.
        # crypto:  SHA1, SHA256, AES. Optional in v8.0. In v8.4, further algorithms were added but it's still optional, see [6].
        # dotprod: Scalar vector product (SDOT and UDOT instructions). Probably the most obscure extra flag with doubtful performance benefits
        #          but it has been activated since always, so why not enable it. It's not 100% clear in which revision this flag was
        #          introduced as optional, either in v8.2 [7] or in v8.4 [8].
        # ldapr:   Load-Acquire RCpc Register. Better support of release/acquire of atomics. Good for allocators and high contention code.
        #          Optional in v8.2, mandatory in v8.3 [9]. Supported in Graviton 2+, Azure and GCP instances. Generated from clang 15.
        #
        # [1] https://github.com/aws/aws-graviton-getting-started/blob/main/c-c%2B%2B.md
        # [2] https://community.arm.com/arm-community-blogs/b/tools-software-ides-blog/posts/making-the-most-of-the-arm-architecture-in-gcc-10
        # [3] https://mysqlonarm.github.io/ARM-LSE-and-MySQL/
        # [4] https://dev.to/aws-builders/large-system-extensions-for-aws-graviton-processors-3eci
        # [5] https://developer.arm.com/tools-and-software/open-source-software/developer-tools/llvm-toolchain/sve-support
        # [6] https://developer.arm.com/documentation/100067/0612/armclang-Command-line-Options/-mcpu?lang=en
        # [7] https://gcc.gnu.org/onlinedocs/gcc/ARM-Options.html
        # [8] https://developer.arm.com/documentation/102651/a/What-are-dot-product-intructions-
        # [9] https://developer.arm.com/documentation/dui0801/g/A64-Data-Transfer-Instructions/LDAPR?lang=en
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=armv8.2-a+simd+crypto+dotprod+ssbs -Xclang=-target-feature -Xclang=+ldapr -Wno-unused-command-line-argument")
    endif ()

elseif (ARCH_PPC64LE)
    # By Default, build for power8 and up, allow building for power9 and up
    # Note that gcc and clang have support for x86 SSE2 intrinsics when building for PowerPC
    option (POWER9 "Build for Power 9 CPU and above" 0)
    if(POWER9)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -maltivec -mcpu=power9 -D__SSE2__=1 -DNO_WARN_X86_INTRINSICS")
    else ()
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -maltivec -mcpu=power8 -D__SSE2__=1 -DNO_WARN_X86_INTRINSICS")
    endif ()

elseif (ARCH_AMD64)
    option (ENABLE_SSSE3 "Use SSSE3 instructions on x86_64" 1)
    option (ENABLE_SSE41 "Use SSE4.1 instructions on x86_64" 1)
    option (ENABLE_SSE42 "Use SSE4.2 instructions on x86_64" 1)
    option (ENABLE_PCLMULQDQ "Use pclmulqdq instructions on x86_64" 1)
    option (ENABLE_POPCNT "Use popcnt instructions on x86_64" 1)
    option (ENABLE_AVX "Use AVX instructions on x86_64" 0)
    option (ENABLE_AVX2 "Use AVX2 instructions on x86_64" 0)
    option (ENABLE_AVX512 "Use AVX512 instructions on x86_64" 0)
    option (ENABLE_AVX512_VBMI "Use AVX512_VBMI instruction on x86_64 (depends on ENABLE_AVX512)" 0)
    option (ENABLE_BMI "Use BMI instructions on x86_64" 0)
    option (ENABLE_AVX2_FOR_SPEC_OP "Use avx2 instructions for specific operations on x86_64" 0)
    option (ENABLE_AVX512_FOR_SPEC_OP "Use avx512 instructions for specific operations on x86_64" 0)

    option (NO_SSE3_OR_HIGHER "Disable SSE3 or higher on x86_64 for maximum compatibility with older/embedded hardware." 0)
    if (NO_SSE3_OR_HIGHER)
        SET(ENABLE_SSSE3 0)
        SET(ENABLE_SSE41 0)
        SET(ENABLE_SSE42 0)
        SET(ENABLE_PCLMULQDQ 0)
        SET(ENABLE_POPCNT 0)
        SET(ENABLE_AVX 0)
        SET(ENABLE_AVX2 0)
        SET(ENABLE_AVX512 0)
        SET(ENABLE_AVX512_VBMI 0)
        SET(ENABLE_BMI 0)
        SET(ENABLE_AVX2_FOR_SPEC_OP 0)
        SET(ENABLE_AVX512_FOR_SPEC_OP 0)
    endif()

    set (TEST_FLAG "-mssse3")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <tmmintrin.h>
        int main() {
            __m64 a = _mm_abs_pi8(__m64());
            (void)a;
            return 0;
        }
    " HAVE_SSSE3)
    if (HAVE_SSSE3 AND ENABLE_SSSE3)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-msse4.1")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <smmintrin.h>
        int main() {
            auto a = _mm_insert_epi8(__m128i(), 0, 0);
            (void)a;
            return 0;
        }
    " HAVE_SSE41)
    if (HAVE_SSE41 AND ENABLE_SSE41)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-msse4.2")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <nmmintrin.h>
        int main() {
            auto a = _mm_crc32_u64(0, 0);
            (void)a;
            return 0;
        }
    " HAVE_SSE42)
    if (HAVE_SSE42 AND ENABLE_SSE42)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-mpclmul")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <wmmintrin.h>
        int main() {
            auto a = _mm_clmulepi64_si128(__m128i(), __m128i(), 0);
            (void)a;
            return 0;
        }
    " HAVE_PCLMULQDQ)
    if (HAVE_PCLMULQDQ AND ENABLE_PCLMULQDQ)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-mpopcnt")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        int main() {
            auto a = __builtin_popcountll(0);
            (void)a;
            return 0;
        }
    " HAVE_POPCNT)
    if (HAVE_POPCNT AND ENABLE_POPCNT)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-mavx")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <immintrin.h>
        int main() {
            auto a = _mm256_insert_epi8(__m256i(), 0, 0);
            (void)a;
            return 0;
        }
    " HAVE_AVX)
    if (HAVE_AVX AND ENABLE_AVX)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-mavx2")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <immintrin.h>
        int main() {
            auto a = _mm256_add_epi16(__m256i(), __m256i());
            (void)a;
            return 0;
        }
    " HAVE_AVX2)
    if (HAVE_AVX2 AND ENABLE_AVX2)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-mavx512f -mavx512bw -mavx512vl")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <immintrin.h>
        int main() {
            auto a = _mm512_setzero_epi32();
            (void)a;
            auto b = _mm512_add_epi16(__m512i(), __m512i());
            (void)b;
            auto c = _mm_cmp_epi8_mask(__m128i(), __m128i(), 0);
            (void)c;
            return 0;
        }
    " HAVE_AVX512)
    if (HAVE_AVX512 AND ENABLE_AVX512)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-mavx512vbmi")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <immintrin.h>
        int main() {
            auto a = _mm512_permutexvar_epi8(__m512i(), __m512i());
            (void)a;
            return 0;
        }
    " HAVE_AVX512_VBMI)
    if (HAVE_AVX512 AND ENABLE_AVX512 AND HAVE_AVX512_VBMI AND ENABLE_AVX512_VBMI)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-mbmi")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <immintrin.h>
        int main() {
            auto a = _blsr_u32(0);
            (void)a;
            return 0;
        }
    " HAVE_BMI)
    if (HAVE_BMI AND ENABLE_BMI)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    # Limit avx2/avx512 flag for specific source build
    set (X86_INTRINSICS_FLAGS "")
    if (ENABLE_AVX2_FOR_SPEC_OP)
        if (HAVE_BMI)
            set (X86_INTRINSICS_FLAGS "${X86_INTRINSICS_FLAGS} -mbmi")
        endif ()
        if (HAVE_AVX AND HAVE_AVX2)
            set (X86_INTRINSICS_FLAGS "${X86_INTRINSICS_FLAGS} -mavx -mavx2")
        endif ()
    endif ()

    if (ENABLE_AVX512_FOR_SPEC_OP)
        set (X86_INTRINSICS_FLAGS "")
        if (HAVE_BMI)
            set (X86_INTRINSICS_FLAGS "${X86_INTRINSICS_FLAGS} -mbmi")
        endif ()
        if (HAVE_AVX512)
            set (X86_INTRINSICS_FLAGS "${X86_INTRINSICS_FLAGS} -mavx512f -mavx512bw -mavx512vl -mprefer-vector-width=256")
        endif ()
    endif ()
else ()
    # RISC-V + exotic platforms
endif ()

cmake_pop_check_state ()
