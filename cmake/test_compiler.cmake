include (CheckCXXSourceCompiles)
include (CMakePushCheckState)

cmake_push_check_state ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
# clang4 : -no-pie cause error
# clang6 : -no-pie cause warning

    if (MAKE_STATIC_LIBRARIES)
        set (TEST_FLAG "-Wl,-Bstatic -stdlib=libc++ -lc++ -lc++abi -Wl,-Bdynamic")
    else ()
        set (TEST_FLAG "-stdlib=libc++ -lc++ -lc++abi")
    endif ()

    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG}")

    check_cxx_source_compiles("
        #include <iostream>
        int main() {
            std::cerr << std::endl;
            return 0;
        }
        " HAVE_LIBCXX)

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


endif ()

cmake_pop_check_state ()
