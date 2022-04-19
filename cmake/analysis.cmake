# https://clang.llvm.org/extra/clang-tidy/
option (ENABLE_CLANG_TIDY "Use clang-tidy static analyzer" OFF)

if (ENABLE_CLANG_TIDY)
    if (${CMAKE_VERSION} VERSION_LESS "3.6.0")
        message(FATAL_ERROR "clang-tidy requires CMake version at least 3.6.")
    endif()

    find_program (CLANG_TIDY_PATH NAMES "clang-tidy" "clang-tidy-13" "clang-tidy-12" "clang-tidy-11" "clang-tidy-10" "clang-tidy-9" "clang-tidy-8")

    if (CLANG_TIDY_PATH)
        message(STATUS
            "Using clang-tidy: ${CLANG_TIDY_PATH}.
            The checks will be run during build process.
            See the .clang-tidy file at the root directory to configure the checks.")

        set (USE_CLANG_TIDY ON)

        # clang-tidy requires assertions to guide the analysis
        # Note that NDEBUG is set implicitly by CMake for non-debug builds
        set(COMPILER_FLAGS "${COMPILER_FLAGS} -UNDEBUG")

        # The variable CMAKE_CXX_CLANG_TIDY will be set inside src and base directories with non third-party code.
        # set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
    elseif (FAIL_ON_UNSUPPORTED_OPTIONS_COMBINATION)
        message(FATAL_ERROR "clang-tidy is not found")
    else ()
        message(STATUS
            "clang-tidy is not found.
            This is normal - the tool is only used for code static analysis and isn't essential for the build.")
    endif ()
endif ()
