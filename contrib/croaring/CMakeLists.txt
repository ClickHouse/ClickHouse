add_library(roaring
    roaring.c
    roaring/roaring.h
    roaring/roaring.hh)

target_include_directories (roaring SYSTEM PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
