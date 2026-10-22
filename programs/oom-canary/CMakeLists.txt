set (CLICKHOUSE_OOM_CANARY_SOURCES clickhouse-oom-canary.cpp OomCanary.cpp)
set (CLICKHOUSE_OOM_CANARY_LINK PRIVATE dbms)
clickhouse_program_add(oom-canary)
