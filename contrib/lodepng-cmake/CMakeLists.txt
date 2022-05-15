set (SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/lodepng)

add_library(_lodepng
    "${SOURCE_DIR}/lodepng.h"
    "${SOURCE_DIR}/lodepng.cpp"
)

target_include_directories(_lodepng PUBLIC "${SOURCE_DIR}")

add_library(ch_contrib::lodepng ALIAS _lodepng)
