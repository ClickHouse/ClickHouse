set (CLICKHOUSE_DOCUMENTATION_SOURCES
    Generator.cpp
)

set (CLICKHOUSE_DOCUMENTATION_LINK
    PRIVATE
    boost::program_options
    dbms
)

clickhouse_program_add(documentation)