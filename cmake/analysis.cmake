# This file configures static analysis tools that can be integrated to the build process

option (ENABLE_CLANG_TIDY "Use 'clang-tidy' static analyzer if present" OFF)
if (ENABLE_CLANG_TIDY)
    if (${CMAKE_VERSION} VERSION_LESS "3.6.0")
        message(FATAL_ERROR "clang-tidy requires CMake version at least 3.6.")
    endif()

    find_program (CLANG_TIDY_PATH NAMES "clang-tidy" "clang-tidy-10" "clang-tidy-9" "clang-tidy-8")
    if (CLANG_TIDY_PATH)
        message(STATUS "Using clang-tidy: ${CLANG_TIDY_PATH}. The checks will be run during build process. See the .clang-tidy file at the root directory to configure the checks.")
        set (USE_CLANG_TIDY 1)
        # The variable CMAKE_CXX_CLANG_TIDY will be set inside src and base directories with non third-party code.
        # set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
    else ()
        message(STATUS "clang-tidy is not found. This is normal - the tool is used only for static code analysis and not essential for build.")
    endif ()
endif ()
