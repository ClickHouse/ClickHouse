
if (NOT(
        CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME
        AND CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR
    )
)
    set (COMPRESSOR "${PROJECT_BINARY_DIR}/native/utils/self-extracting-executable/pre_compressor")
    set (DECOMPRESSOR "--decompressor=${PROJECT_BINARY_DIR}/utils/self-extracting-executable/decompressor")
else ()
    set (COMPRESSOR "${PROJECT_BINARY_DIR}/utils/self-extracting-executable/compressor")
endif ()

add_custom_target (self-extracting-server ALL
    ${CMAKE_COMMAND} -E remove clickhouse clickhouse-stripped
    COMMAND ${COMPRESSOR} ${DECOMPRESSOR} clickhouse ../clickhouse
    COMMAND ${COMPRESSOR} ${DECOMPRESSOR} clickhouse-stripped ../clickhouse-stripped
    DEPENDS clickhouse clickhouse-stripped compressor
)

set(self_extracting_deps "self-extracting-server")

if (BUILD_STANDALONE_KEEPER)
    add_custom_target (self-extracting-keeper ALL
        ${CMAKE_COMMAND} -E remove clickhouse-keeper
        COMMAND ${COMPRESSOR} ${DECOMPRESSOR} clickhouse-keeper ../clickhouse-keeper
        DEPENDS compressor clickhouse-keeper
    )
    list(APPEND self_extracting_deps "self-extracting-keeper")
endif()

add_custom_target (self-extracting ALL
    DEPENDS ${self_extracting_deps}
)
