set (CLICKHOUSE_OBFUSCATOR_SOURCES Obfuscator.cpp)

set (CLICKHOUSE_OBFUSCATOR_LINK
    PRIVATE
        boost::program_options
        clickhouse_dictionaries
        dbms
)

clickhouse_program_add(obfuscator)
