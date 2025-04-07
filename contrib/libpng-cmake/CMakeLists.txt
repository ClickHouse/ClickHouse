set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpng")

file(GLOB SRCS "${LIBRARY_DIR}/*.c")

add_library(_libpng ${SRCS})

target_link_libraries (_libpng PRIVATE ch_contrib::zlib)

target_include_directories(_libpng SYSTEM BEFORE PUBLIC
    "${LIBRARY_DIR}"
)

add_library(ch_contrib::libpng ALIAS _libpng)
