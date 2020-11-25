# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/Util
)


SRCS(
    GRPCServer.cpp
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
    ProtocolServerAdapter.cpp
    ReplicasStatusHandler.cpp
    StaticRequestHandler.cpp
    TCPHandler.cpp
    WebUIRequestHandler.cpp

)

END()
