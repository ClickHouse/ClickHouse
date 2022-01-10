include(${ClickHouse_SOURCE_DIR}/cmake/embed_binary.cmake)

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/nlp-data")

add_library (nlp_data INTERFACE)

clickhouse_embed_binaries(
    TARGET nlp_dicts
    RESOURCE_DIR "${LIBRARY_DIR}"
    RESOURCES charset.zst tonality_ru.zst programming.zst
)

add_dependencies(nlp_data nlp_dicts)
target_link_libraries(nlp_data INTERFACE "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:nlp_dicts> -Wl,${NO_WHOLE_ARCHIVE}")
