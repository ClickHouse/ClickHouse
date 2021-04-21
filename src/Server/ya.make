# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/Util
)


SRCS(
    GRPCServer.cpp
    HTTP/HTMLForm.cpp
    HTTP/HTTPServer.cpp
    HTTP/HTTPServerConnection.cpp
    HTTP/HTTPServerConnectionFactory.cpp
    HTTP/HTTPServerRequest.cpp
    HTTP/HTTPServerResponse.cpp
    HTTP/ReadHeaders.cpp
    HTTP/WriteBufferFromHTTPServerResponse.cpp
    HTTPHandler.cpp
    HTTPHandlerFactory.cpp
    InterserverIOHTTPHandler.cpp
    KeeperTCPHandler.cpp
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
