include(${ClickHouse_SOURCE_DIR}/cmake/embed_binary.cmake)

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/nlp-data")

add_library (_nlp_data INTERFACE)

clickhouse_embed_binaries(
    TARGET nlp_dictionaries
    RESOURCE_DIR "${LIBRARY_DIR}"
    RESOURCES charset.zst tonality_ru.zst programming.zst
)

add_dependencies(_nlp_data nlp_dictionaries)
target_link_libraries(_nlp_data INTERFACE "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:nlp_dictionaries> -Wl,${NO_WHOLE_ARCHIVE}")
add_library(ch_contrib::nlp_data ALIAS _nlp_data)
