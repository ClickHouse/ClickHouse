set(SPOTIFY_ANNOY_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/spotify-annoy")
set(SPOTIFY_ANNOY_SOURCE_DIR "${SPOTIFY_ANNOY_PROJECT_DIR}/src")
set(SPOTIFY_ANNOY_INCLUDE_DIR "${SPOTIFY_ANNOY_PROJECT_DIR}/include")

set(SPOTIFY_ANNOY_HEADERS 
    ${SPOTIFY_ANNOY_SOURCE_DIR}/annoylib.h
    ${SPOTIFY_ANNOY_SOURCE_DIR}/kissrandom.h
)

set(SPOTIFY_ANNOY_SRC
    ${SPOTIFY_ANNOY_SOURCE_DIR}/mman.h
)

add_library(_spotify_annoy ${SPOTIFY_ANNOY_SRC})
target_include_directories(_spotify_annoy SYSTEM PUBLIC ${SPOTIFY_ANNOY_SOURCE_DIR})

add_library(ch_contrib::spotify-annoy ALIAS _spotify_annoy)
