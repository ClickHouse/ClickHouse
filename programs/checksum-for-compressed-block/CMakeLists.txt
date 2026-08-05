set (CLICKHOUSE_CHECKSUM_FOR_COMPRESSED_BLOCK_SOURCES ChecksumForCompressedBlock.cpp)

set (CLICKHOUSE_CHECKSUM_FOR_COMPRESSED_BLOCK_LINK
    PRIVATE
        clickhouse_functions
        dbms
)

clickhouse_program_add(checksum-for-compressed-block)
