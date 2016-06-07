include_directories (${CMAKE_CURRENT_BINARY_DIR})

IF (NOT AARCH64)	# Не используется. Портировать не сложно.
	SET(SOURCES_ONLY_ON_X86_64 src/metrohash128crc.cpp)
ENDIF()

add_library(metrohash
	src/metrohash.h
	src/testvector.h

	src/metrohash64.cpp
	src/metrohash128.cpp
	${SOURCES_ONLY_ON_X86_64})
