set (CLICKHOUSE_EXTRACT_FROM_CONFIG_SOURCES ExtractFromConfig.cpp)

set (CLICKHOUSE_EXTRACT_FROM_CONFIG_LINK
    PRIVATE
        boost::program_options
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_common_zookeeper
)

clickhouse_program_add(extract-from-config)
