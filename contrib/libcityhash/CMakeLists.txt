include_directories (include)

add_library(cityhash
	city.cc

	include/citycrc.h
	include/city.h
	config.h)
