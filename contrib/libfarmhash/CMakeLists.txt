add_library(farmhash
	farmhash.cc
	farmhash.h)

target_include_directories (farmhash PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
