PROGRAM(clickhouse-server)

PEERDIR(
    clickhouse/base/common
    clickhouse/base/daemon
    clickhouse/base/loggers
    clickhouse/src
    contrib/libs/poco/NetSSL_OpenSSL
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
    PrometheusMetricsWriter.cpp
    PrometheusRequestHandler.cpp
    ReplicasStatusHandler.cpp
    StaticRequestHandler.cpp
    Server.cpp
    TCPHandler.cpp
)

END()
