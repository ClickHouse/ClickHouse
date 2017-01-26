include (CheckCXXSourceRuns)

set (TEST_FLAG "-mpopcnt")

set (CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${TEST_FLAG}")
check_cxx_source_runs("
	int main() {
		__builtin_popcountll(0);
	}
" HAVE_POPCNT)

if (HAVE_POPCNT)
	set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
endif ()
