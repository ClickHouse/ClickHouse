cmake_minimum_required(VERSION 3.14)

add_library(SpotifyAnnoy INTERFACE)

set(SPOTIFY_ANNOY_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/sporify-annoy")
set(SPOTIFY_ANNOY_SOURCE_DIR "${SPOTIFY_ANNOY_PROJECT_DIR}/src")
set(SPOTIFY_ANNOY_INCLUDE_DIR ${SPOTIFY_ANNOY_PROJECT_DIR}/include/annoy)

file(MAKE_DIRECTORY ${SPOTIFY_ANNOY_INCLUDE_DIR})
foreach (HEADER annoylib.h kissrandom.h mman.h)
    file(COPY ${SPOTIFY_ANNOY_PROJECT_DIR}/src/${HEADER} DESTINATION ${SPOTIFY_ANNOY_INCLUDE_DIR})
endforeach()

target_include_directories(SpotifyAnnoy INTERFACE include/)

add_library(ch::contrib::spotify-annoy ALIAS SpotifyAnnoy)
