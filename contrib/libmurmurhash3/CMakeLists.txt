add_library(murmurhash3
	src/murmurhash3.cpp
	include/murmurhash3.h)

target_include_directories (murmurhash3 PUBLIC include)
target_include_directories (murmurhash3 PUBLIC src)

