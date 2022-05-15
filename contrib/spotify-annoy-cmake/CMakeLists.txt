set(SPOTIFY_ANNOY_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/spotify-annoy")
set(SPOTIFY_ANNOY_SOURCE_DIR "${SPOTIFY_ANNOY_PROJECT_DIR}/src")
set(SPOTIFY_ANNOY_INCLUDE_DIR "${SPOTIFY_ANNOY_PROJECT_DIR}/src")

set(SPOTIFY_ANNOY_SRC
    ${SPOTIFY_ANNOY_SOURCE_DIR}/mman.h
)

add_library(_spotify_annoy ${SPOTIFY_ANNOY_SRC})
target_include_directories(_spotify_annoy SYSTEM PUBLIC ${SPOTIFY_ANNOY_INCLUDE_DIR})
set_target_properties(_spotify_annoy PROPERTIES LINKER_LANGUAGE CXX)

add_library(ch_contrib::spotify-annoy ALIAS _spotify_annoy)
