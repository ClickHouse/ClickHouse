# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/Util
)

CFLAGS(-g0)

SRCS(
    HTTPHandler.cpp
    HTTPHandlerFactory.cpp
    InterserverIOHTTPHandler.cpp
    MySQLHandler.cpp
    MySQLHandlerFactory.cpp
    NotFoundHandler.cpp
    PostgreSQLHandler.cpp
    PostgreSQLHandlerFactory.cpp
    PrometheusMetricsWriter.cpp
    PrometheusRequestHandler.cpp
    ReplicasStatusHandler.cpp
    StaticRequestHandler.cpp
    TCPHandler.cpp
    WebUIRequestHandler.cpp

)

END()
