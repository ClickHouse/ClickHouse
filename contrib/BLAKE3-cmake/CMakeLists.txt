set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/BLAKE3)

add_library (BLAKE3 ${SRCS})


target_include_directories(BLAKE3 ${LIBRARY_DIR}/include)