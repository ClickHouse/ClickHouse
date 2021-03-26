set (SRCS
    src/metrohash64.cpp
    src/metrohash128.cpp
)
if (HAVE_SSE42) # Not used. Pretty easy to port.
    list (APPEND SRCS src/metrohash128crc.cpp)
endif ()

add_library(metrohash ${SRCS})
target_include_directories(metrohash PUBLIC src)
