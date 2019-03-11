add_library(roaring
	roaring.c
	roaring.h
	roaring.hh)

target_include_directories (roaring PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
