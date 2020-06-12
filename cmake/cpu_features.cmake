# https://software.intel.com/sites/landingpage/IntrinsicsGuide/

include (CheckCXXSourceCompiles)
include (CMakePushCheckState)

cmake_push_check_state ()

# gcc -dM -E -mno-sse2 - < /dev/null | sort > gcc-dump-nosse2
# gcc -dM -E -msse2 - < /dev/null | sort > gcc-dump-sse2
#define __SSE2__ 1
#define __SSE2_MATH__ 1

# gcc -dM -E -msse4.1 - < /dev/null | sort > gcc-dump-sse41
#define __SSE4_1__ 1

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
if (HAVE_SSE41)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
endif ()

if (ARCH_PPC64LE)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -maltivec -D__SSE2__=1 -DNO_WARN_X86_INTRINSICS")
endif ()

# gcc -dM -E -msse4.2 - < /dev/null | sort > gcc-dump-sse42
#define __SSE4_2__ 1

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
if (HAVE_SSE42)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
endif ()

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

# gcc -dM -E -mpopcnt - < /dev/null | sort > gcc-dump-popcnt
#define __POPCNT__ 1

set (TEST_FLAG "-mpopcnt")

set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
check_cxx_source_compiles("
    int main() {
        auto a = __builtin_popcountll(0);
        (void)a;
        return 0;
    }
" HAVE_POPCNT)

if (HAVE_POPCNT AND NOT ARCH_AARCH64)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
endif ()

cmake_pop_check_state ()
