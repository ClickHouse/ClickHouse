add_library(_cityhash
	src/city.cc
	include/citycrc.h
	include/city.h
	src/config.h)

target_include_directories(_cityhash SYSTEM BEFORE PUBLIC include)
target_include_directories(_cityhash SYSTEM PRIVATE src)

add_library(ch_contrib::cityhash ALIAS _cityhash)
