add_library(cityhash
	src/city.cc
	include/citycrc.h
	include/city.h
	src/config.h)

target_include_directories(cityhash SYSTEM BEFORE PUBLIC include)
target_include_directories(cityhash SYSTEM PRIVATE src)
