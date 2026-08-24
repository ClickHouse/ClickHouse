set (SRCS
    src/metrohash64.cpp
    src/metrohash128.cpp
)
add_library(_metrohash ${SRCS})
target_include_directories(_metrohash PUBLIC src)
add_library(ch_contrib::metrohash ALIAS _metrohash)
