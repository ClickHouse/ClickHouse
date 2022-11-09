add_library(widechar_width widechar_width.cpp)
target_link_libraries(widechar_width global-group)
target_include_directories(widechar_width PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
