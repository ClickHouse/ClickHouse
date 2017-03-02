include (CheckCXXSourceCompiles)

set (TEST_FLAG "-no-pie")
set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG}")

check_cxx_source_compiles("
	int main() {
		return 0;
	}
" HAVE_NO_PIE)

set (CMAKE_REQUIRED_FLAGS "")

if (HAVE_NO_PIE)
	set (FLAG_NO_PIE ${TEST_FLAG})
endif ()
