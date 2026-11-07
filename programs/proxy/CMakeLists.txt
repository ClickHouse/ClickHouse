set (CLICKHOUSE_PROXY_SOURCES
    Proxy.cpp
    ProxyConfig.cpp
    Backend.cpp
    LoadBalancing.cpp
    BackendPool.cpp
    RoutingTable.cpp
    Router.cpp
    SocketIO.cpp
    TLSSupport.cpp
    Relay.cpp
    Frontend.cpp
    StatusPage.cpp
    HTTPFrontend.cpp
    NativeFrontend.cpp
    MySQLFrontend.cpp
    PostgreSQLFrontend.cpp
    PassthroughFrontend.cpp
    SSHFrontend.cpp
    HealthMonitor.cpp
    ProxyServer.cpp
)

set (CLICKHOUSE_PROXY_LINK
    PRIVATE
        boost::program_options
        clickhouse_common_config
        clickhouse_common_io
        daemon
        dbms
)

if (TARGET ch_contrib::silk_fibers)
    list(APPEND CLICKHOUSE_PROXY_LINK ch_contrib::silk_fibers)
endif()

clickhouse_program_add(proxy)
