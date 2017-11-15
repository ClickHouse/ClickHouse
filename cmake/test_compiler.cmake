include (CheckCXXSourceCompiles)
include (CMakePushCheckState)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
# clang4 : -no-pie cause error
# clang6 : -no-pie cause warning
else ()

    cmake_push_check_state ()

    set (TEST_FLAG "-no-pie")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG}")

    check_cxx_source_compiles("
        int main() {
            return 0;
        }
        " HAVE_NO_PIE)

    if (HAVE_NO_PIE)
        set (FLAG_NO_PIE ${TEST_FLAG})
    endif ()

    cmake_pop_check_state ()

endif ()
