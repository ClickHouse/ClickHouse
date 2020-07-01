set(CLICKHOUSE_SERVER_SOURCES
    MetricsTransmitter.cpp
    Server.cpp
)

set (CLICKHOUSE_SERVER_LINK
    PRIVATE
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_common_zookeeper
        clickhouse_dictionaries
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions
        string_utils

    PUBLIC
        daemon
)

clickhouse_program_add(server)

if (GLIBC_COMPATIBILITY)
    set (GLIBC_MAX_REQUIRED 2.4 CACHE INTERNAL "")
    # temporary disabled. to enable - change 'exit 0' to 'exit $a'
    add_test(NAME GLIBC_required_version COMMAND bash -c "readelf -s ${CMAKE_CURRENT_BINARY_DIR}/../clickhouse-server | perl -nE 'END {exit 0 if $a} ++$a, print if /\\x40GLIBC_(\\S+)/ and pack(q{C*}, split /\\./, \$1) gt pack q{C*}, split /\\./, q{${GLIBC_MAX_REQUIRED}}'")

    #add_test(NAME GLIBC_required_version COMMAND bash -c "readelf -s ${CMAKE_CURRENT_BINARY_DIR}/../clickhouse-server | grep '@GLIBC' | grep -oP 'GLIBC_[\\d\\.]+' | sort | uniq | sort --version-sort --reverse | perl -lnE 'warn($_), exit 1 if $_ gt q{GLIBC_${GLIBC_MAX_REQUIRED}}'") # old
endif ()

install(FILES config.xml users.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-server COMPONENT clickhouse)
