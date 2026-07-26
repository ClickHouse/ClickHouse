set (CLICKHOUSE_PACKED_IO_SOURCES PackedIO.cpp)

set (CLICKHOUSE_PACKED_IO_LINK
    PRIVATE
        boost::program_options
        dbms
)

clickhouse_program_add(packed-io)
