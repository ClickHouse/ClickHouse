if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

add_library(pcg_random INTERFACE)
target_include_directories(pcg_random INTERFACE .)
