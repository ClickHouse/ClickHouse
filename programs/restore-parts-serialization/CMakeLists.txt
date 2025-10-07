set (CLICKHOUSE_RESTORE_PARTS_SERIALIZATION_SOURCES RestorePartsSerialization.cpp)

set (CLICKHOUSE_RESTORE_PARTS_SERIALIZATION_LINK
    PRIVATE
        boost::program_options
        dbms
)

clickhouse_program_add(restore-parts-serialization)
