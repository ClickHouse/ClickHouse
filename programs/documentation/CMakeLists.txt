set (CLICKHOUSE_DOCUMENTATION_SOURCES
    Generator.cpp
)

set (CLICKHOUSE_DOCUMENTATION_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
        string_utils
)

# Always use internal readpassphrase
list(APPEND CLICKHOUSE_DOCUMENTATION_LINK PRIVATE readpassphrase)

clickhouse_program_add(documentation)