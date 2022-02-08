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

add_library(bzip2 ${SRCS})

target_include_directories(bzip2 PUBLIC "${BZIP2_SOURCE_DIR}" "${BZIP2_BINARY_DIR}")
