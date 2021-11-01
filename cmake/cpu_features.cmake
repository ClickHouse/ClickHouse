# https://software.intel.com/sites/landingpage/IntrinsicsGuide/

include (CheckCXXSourceCompiles)
include (CMakePushCheckState)

cmake_push_check_state ()

# The variables HAVE_* determine if compiler has support for the flag to use the corresponding instruction set.
# The options ENABLE_* determine if we will tell compiler to actually use the corresponding instruction set if compiler can do it.

# All of them are unrelated to the instruction set at the host machine
# (you can compile for newer instruction set on old machines and vice versa).

option (ENABLE_SSSE3 "Use SSSE3 instructions on x86_64" 1)
option (ENABLE_SSE41 "Use SSE4.1 instructions on x86_64" 1)
option (ENABLE_SSE42 "Use SSE4.2 instructions on x86_64" 1)
option (ENABLE_PCLMULQDQ "Use pclmulqdq instructions on x86_64" 1)
option (ENABLE_POPCNT "Use popcnt instructions on x86_64" 1)
option (ENABLE_AVX "Use AVX instructions on x86_64" 0)
option (ENABLE_AVX2 "Use AVX2 instructions on x86_64" 0)
option (ENABLE_AVX512 "Use AVX512 instructions on x86_64" 0)
option (ENABLE_BMI "Use BMI instructions on x86_64" 0)
option (ENABLE_AVX2_FOR_SPEC_OP "Use avx2 instructions for specific operations on x86_64" 0)
option (ENABLE_AVX512_FOR_SPEC_OP "Use avx512 instructions for specific operations on x86_64" 0)

option (ARCH_NATIVE "Add -march=native compiler flag. This makes your binaries non-portable but more performant code may be generated. This option overrides ENABLE_* options for specific instruction set. Highly not recommended to use." 0)

if (ARCH_NATIVE)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=native")

else ()
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

    if (ARCH_PPC64LE)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -maltivec -D__SSE2__=1 -DNO_WARN_X86_INTRINSICS")
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

    set (TEST_FLAG "-mavx512f -mavx512bw")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <immintrin.h>
        int main() {
            auto a = _mm512_setzero_epi32();
            (void)a;            
            auto b = _mm512_add_epi16(__m512i(), __m512i());
            (void)b;
            return 0;
        }
    " HAVE_AVX512)
    if (HAVE_AVX512 AND ENABLE_AVX512)
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

#Limit avx2/avx512 flag for specific source build
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
            set (X86_INTRINSICS_FLAGS "${X86_INTRINSICS_FLAGS} -mavx512f -mavx512bw -mprefer-vector-width=256")
        endif ()
    endif ()
endif ()

cmake_pop_check_state ()
