
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

add_custom_target (self-extracting ALL
    ${CMAKE_COMMAND} -E remove clickhouse
    COMMAND ${COMPRESSOR} ${DECOMPRESSOR} clickhouse ../clickhouse
    DEPENDS clickhouse compressor
)

