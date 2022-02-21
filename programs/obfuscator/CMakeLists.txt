set (CLICKHOUSE_OBFUSCATOR_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/Obfuscator.cpp)

set (CLICKHOUSE_OBFUSCATOR_LINK
    PRIVATE
        boost::program_options
        dbms
)

clickhouse_program_add(obfuscator)
