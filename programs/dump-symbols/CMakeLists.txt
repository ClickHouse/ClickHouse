set (CLICKHOUSE_DUMP_SYMBOLS_SOURCES dump-symbols.cpp)
set (CLICKHOUSE_DUMP_SYMBOLS_LINK PRIVATE dbms)

clickhouse_program_add(dump-symbols)
