if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

add_library(widechar_width widechar_width.cpp)
target_include_directories(widechar_width PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
