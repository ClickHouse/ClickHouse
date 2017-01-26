
# https://software.intel.com/sites/landingpage/IntrinsicsGuide/

include (CheckCXXSourceRuns)

check_cxx_source_runs("
	#include <emmintrin.h>
	int main() {
		_mm_loadu_si128(nullptr);
	}
" HAVE_SSE2)


set (TEST_FLAG "-msse4.1")
set (CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${TEST_FLAG}")
check_cxx_source_runs("
	#include <smmintrin.h>
	int main() {
		_mm_insert_epi8(__m128i(), 0, 0);
	}
" HAVE_SSE41)
if (HAVE_SSE41)
	set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
endif ()


set (TEST_FLAG "-msse4.2")
set (CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${TEST_FLAG}")
check_cxx_source_runs("
	#include <nmmintrin.h>
	int main() {
		_mm_crc32_u64(0, 0);
	}
" HAVE_SSE42)
if (HAVE_SSE42)
	set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
endif ()
