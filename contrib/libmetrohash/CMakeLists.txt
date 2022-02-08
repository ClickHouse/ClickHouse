set (SRCS
    src/metrohash64.cpp
    src/metrohash128.cpp
)
add_library(metrohash ${SRCS})
target_include_directories(metrohash PUBLIC src)
