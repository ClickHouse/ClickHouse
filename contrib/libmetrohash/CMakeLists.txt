if (HAVE_SSE42) # Not used. Pretty easy to port.
    set (SOURCES_SSE42_ONLY src/metrohash128crc.cpp)
endif ()

add_library(metrohash
    src/metrohash.h
    src/testvector.h

    src/metrohash64.cpp
    src/metrohash128.cpp
    ${SOURCES_SSE42_ONLY})

target_include_directories(metrohash PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
