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
    HTTPInterfaceConfig.cpp
    HTTPProxyConfig.cpp
    HTTPProxyProtocolHandler.cpp
    IndirectServerConnection.cpp
    InterfaceConfig.cpp
    InterfaceConfigUtil.cpp
    InterserverHTTPInterfaceConfig.cpp
    InterserverIOHTTPHandler.cpp
    KeeperTCPHandler.cpp
    KeeperTCPInterfaceConfig.cpp
    MySQLHandler.cpp
    MySQLHandlerFactory.cpp
    MySQLInterfaceConfig.cpp
    NativeGRPCInterfaceConfig.cpp
    NativeHTTPInterfaceConfig.cpp
    NativeTCPInterfaceConfig.cpp
    NotFoundHandler.cpp
    PostgreSQLHandler.cpp
    PostgreSQLHandlerFactory.cpp
    PostgreSQLInterfaceConfig.cpp
    PrometheusInterfaceConfig.cpp
    PrometheusMetricsWriter.cpp
    PrometheusRequestHandler.cpp
    ProtocolServerAdapter.cpp
    ProxyConfig.cpp
    ProxyProtocolHandler.cpp
    PROXYProxyConfig.cpp
    PROXYProxyProtocolHandler.cpp
    ReplicasStatusHandler.cpp
    StaticRequestHandler.cpp
    TCPHandler.cpp
    TCPInterfaceConfig.cpp
    WebUIRequestHandler.cpp
)

END()
