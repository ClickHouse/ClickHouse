LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/Util
)

SRCS(
    HTTPHandler.cpp
    HTTPHandlerFactory.cpp
    InterserverIOHTTPHandler.cpp
    MySQLHandler.cpp
    MySQLHandlerFactory.cpp
    NotFoundHandler.cpp
    PrometheusMetricsWriter.cpp
    PrometheusRequestHandler.cpp
    ReplicasStatusHandler.cpp
    StaticRequestHandler.cpp
    TCPHandler.cpp
)

END()
