set (CLICKHOUSE_SU_SOURCES clickhouse-su.cpp)
set (CLICKHOUSE_SU_LINK PRIVATE dbms)
clickhouse_program_add(su)
