PROGRAM(clickhouse-server)

PEERDIR(
    clickhouse/base/daemon
    clickhouse/base/loggers
    clickhouse/dbms/src
)

SRCS(
    clickhouse-server.cpp

    HTTPHandler.cpp
    HTTPHandlerFactory.cpp
    InterserverIOHTTPHandler.cpp
    MetricsTransmitter.cpp
    MySQLHandler.cpp
    MySQLHandlerFactory.cpp
    NotFoundHandler.cpp
    PingRequestHandler.cpp
    PrometheusMetricsWriter.cpp
    PrometheusRequestHandler.cpp
    ReplicasStatusHandler.cpp
    RootRequestHandler.cpp
    Server.cpp
    TCPHandler.cpp
)

END()
