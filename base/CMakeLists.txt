if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

add_subdirectory (base)
add_subdirectory (pcg-random)
add_subdirectory (widechar_width)
add_subdirectory (readpassphrase)
