add_library (clickhouse-extract-from-config-lib ${SPLIT_SHARED} ExtractFromConfig.cpp)
target_link_libraries (clickhouse-extract-from-config-lib clickhouse_common_config clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-extract-from-config clickhouse-extract-from-config.cpp)
    target_link_libraries (clickhouse-extract-from-config clickhouse-extract-from-config-lib)
endif ()
