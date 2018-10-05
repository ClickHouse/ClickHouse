add_library(cityhash
	src/city.cc

	include/citycrc.h
	include/city.h
	src/config.h)

target_include_directories (cityhash BEFORE PUBLIC include)
target_include_directories (cityhash PRIVATE src)
