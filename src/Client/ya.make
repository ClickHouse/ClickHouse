# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/NetSSL_OpenSSL
)


SRCS(
    Connection.cpp
    ConnectionEstablisher.cpp
    ConnectionPool.cpp
    ConnectionPoolWithFailover.cpp
    HedgedConnections.cpp
    HedgedConnectionsFactory.cpp
    IConnections.cpp
    MultiplexedConnections.cpp

)

END()
