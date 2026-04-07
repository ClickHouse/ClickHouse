# module crc32-vpmsum gets build along with the files vec_crc32.h and crc32_constants.h in crc32-vpmsum-cmake
# Please see README.md for information about how to generate crc32_constants.h
if (NOT ARCH_PPC64LE)
	message (STATUS "crc32-vpmsum library is only supported on ppc64le")
    return()
endif()

SET(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/crc32-vpmsum")

add_library(_crc32-vpmsum 
	"${LIBRARY_DIR}/vec_crc32.c"
	)
target_include_directories(_crc32-vpmsum SYSTEM BEFORE PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
add_library(ch_contrib::crc32-vpmsum ALIAS _crc32-vpmsum)
