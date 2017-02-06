include_directories (${CMAKE_CURRENT_BINARY_DIR})

if (NOT AARCH64) # Not used. Pretty easy to port.
	set (SOURCES_ONLY_ON_X86_64 src/metrohash128crc.cpp)
endif ()

add_library(metrohash
	src/metrohash.h
	src/testvector.h

	src/metrohash64.cpp
	src/metrohash128.cpp
	${SOURCES_ONLY_ON_X86_64})
