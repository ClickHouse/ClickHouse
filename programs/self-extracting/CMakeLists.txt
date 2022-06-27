add_custom_target (self-extracting ALL
    ${CMAKE_BINARY_DIR}/utils/self-extracting-executable/compressor clickhouse ../clickhouse
    DEPENDS clickhouse compressor
)

