set(CLICKHOUSE_SERVER_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/HTTPHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/InterserverIOHTTPHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/MetricsTransmitter.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/NotFoundHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/PingRequestHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ReplicasStatusHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/RootRequestHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/Server.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/TCPHandler.cpp
   )

set(CLICKHOUSE_SERVER_LINK PRIVATE clickhouse_dictionaries clickhouse_common_io PUBLIC daemon PRIVATE clickhouse_storages_system clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions ${Poco_Net_LIBRARY})
if (USE_POCO_NETSSL)
    set(CLICKHOUSE_SERVER_LINK ${CLICKHOUSE_SERVER_LINK} PRIVATE ${Poco_NetSSL_LIBRARY} ${Poco_Crypto_LIBRARY})
endif ()
set(CLICKHOUSE_SERVER_INCLUDE PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)

clickhouse_program_add(server)

if (GLIBC_COMPATIBILITY)
    set (GLIBC_MAX_REQUIRED 2.4 CACHE INTERNAL "")
    # temporary disabled. to enable - change 'exit 0' to 'exit $a'
    add_test(NAME GLIBC_required_version COMMAND bash -c "readelf -s ${CMAKE_CURRENT_BINARY_DIR}/../clickhouse-server | perl -nE 'END {exit 0 if $a} ++$a, print if /\\x40GLIBC_(\\S+)/ and pack(q{C*}, split /\\./, \$1) gt pack q{C*}, split /\\./, q{${GLIBC_MAX_REQUIRED}}'")

    #add_test(NAME GLIBC_required_version COMMAND bash -c "readelf -s ${CMAKE_CURRENT_BINARY_DIR}/../clickhouse-server | grep '@GLIBC' | grep -oP 'GLIBC_[\\d\\.]+' | sort | uniq | sort --version-sort --reverse | perl -lnE 'warn($_), exit 1 if $_ gt q{GLIBC_${GLIBC_MAX_REQUIRED}}'") # old
endif ()

install(FILES config.xml users.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-server COMPONENT clickhouse)
