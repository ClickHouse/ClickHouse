option(ENABLE_BZIP2 "Enable bzip2 compression support" ${ENABLE_LIBRARIES})
if (NOT ENABLE_BZIP2)
    message (STATUS "Not using bzip2")
    return()
endif()

set(BZIP2_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/bzip2")
set(BZIP2_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/bzip2")

set(SRCS
    "${BZIP2_SOURCE_DIR}/blocksort.c"
    "${BZIP2_SOURCE_DIR}/huffman.c"
    "${BZIP2_SOURCE_DIR}/crctable.c"
    "${BZIP2_SOURCE_DIR}/randtable.c"
    "${BZIP2_SOURCE_DIR}/compress.c"
    "${BZIP2_SOURCE_DIR}/decompress.c"
    "${BZIP2_SOURCE_DIR}/bzlib.c"
)

# From bzip2/CMakeLists.txt 
set(BZ_VERSION "1.0.7")
configure_file (
    "${BZIP2_SOURCE_DIR}/bz_version.h.in"
    "${BZIP2_BINARY_DIR}/bz_version.h"
)

add_library(_bzip2 ${SRCS})
add_library(ch_contrib::bzip2 ALIAS _bzip2)
target_include_directories(_bzip2 SYSTEM BEFORE PUBLIC "${BZIP2_SOURCE_DIR}" "${BZIP2_BINARY_DIR}")
