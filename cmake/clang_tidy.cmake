# https://clang.llvm.org/extra/clang-tidy/
option (ENABLE_CLANG_TIDY "Use clang-tidy static analyzer" OFF)

if (ENABLE_CLANG_TIDY)

    find_program (CLANG_TIDY_CACHE_PATH NAMES "clang-tidy-cache")
    if (CLANG_TIDY_CACHE_PATH)
        find_program (_CLANG_TIDY_PATH NAMES "clang-tidy-18" "clang-tidy-17" "clang-tidy-16" "clang-tidy")

        # Why do we use ';' here?
        # It's a cmake black magic: https://cmake.org/cmake/help/latest/prop_tgt/LANG_CLANG_TIDY.html#prop_tgt:%3CLANG%3E_CLANG_TIDY
        # The CLANG_TIDY_PATH is passed to CMAKE_CXX_CLANG_TIDY, which follows CXX_CLANG_TIDY syntax.
        set (CLANG_TIDY_PATH "${CLANG_TIDY_CACHE_PATH};${_CLANG_TIDY_PATH}" CACHE STRING "A combined command to run clang-tidy with caching wrapper")
    else ()
        find_program (CLANG_TIDY_PATH NAMES "clang-tidy-18" "clang-tidy-17" "clang-tidy-16" "clang-tidy")
    endif ()

    if (CLANG_TIDY_PATH)
        message (STATUS
            "Using clang-tidy: ${CLANG_TIDY_PATH}.
            The checks will be run during the build process.
            See the .clang-tidy file in the root directory to configure the checks.")

        set (USE_CLANG_TIDY ON)

        # clang-tidy requires assertions to guide the analysis
        # Note that NDEBUG is set implicitly by CMake for non-debug builds
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -UNDEBUG")

        # The variable CMAKE_CXX_CLANG_TIDY will be set inside the following directories with non third-party code.
        # - base
        # - programs
        # - src
        # - utils
        # set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
    else ()
        message (${RECONFIGURE_MESSAGE_LEVEL} "clang-tidy is not found")
    endif ()
endif ()
