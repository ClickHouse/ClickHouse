set (CLICKHOUSE_TOP_SOURCES
    Top.cpp
)

set (CLICKHOUSE_TOP_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
        string_utils
)

if (TARGET ch_rust::skim)
    list(APPEND CLICKHOUSE_TOP_LINK PRIVATE ch_rust::skim)
endif()

# Always use internal readpassphrase
list(APPEND CLICKHOUSE_TOP_LINK PRIVATE readpassphrase)

clickhouse_program_add(top)

install (FILES clickhouse-top.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-top" COMPONENT clickhouse-top RENAME config.xml)
