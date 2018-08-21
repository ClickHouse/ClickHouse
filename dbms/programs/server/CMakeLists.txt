add_library (clickhouse-server-lib
    HTTPHandler.cpp
    InterserverIOHTTPHandler.cpp
    MetricsTransmitter.cpp
    NotFoundHandler.cpp
    PingRequestHandler.cpp
    ReplicasStatusHandler.cpp
    RootRequestHandler.cpp
    Server.cpp
    TCPHandler.cpp
    )

target_link_libraries (clickhouse-server-lib clickhouse_common_io daemon clickhouse_storages_system clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions)
target_include_directories (clickhouse-server-lib PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-server clickhouse-server.cpp)
    target_link_libraries (clickhouse-server clickhouse-server-lib)
    install (TARGETS clickhouse-server ${CLICKHOUSE_ALL_TARGETS} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
endif ()

if (OS_LINUX)
    set (GLIBC_MAX_REQUIRED 2.4)
    add_test(NAME GLIBC_required_version COMMAND bash -c "readelf -s ${CMAKE_CURRENT_BINARY_DIR}/../clickhouse-server | grep '@GLIBC' | grep -oP 'GLIBC_[\\d\\.]+' | sort | uniq | sort -r | perl -lnE 'exit 1 if $_ gt q{GLIBC_${GLIBC_MAX_REQUIRED}}'")
endif ()

install (
    FILES config.xml users.xml
    DESTINATION  ${CLICKHOUSE_ETC_DIR}/clickhouse-server
    COMPONENT clickhouse)
