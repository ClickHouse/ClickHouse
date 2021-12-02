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
    ConnectionPoolWithFailover.cpp
    HedgedConnections.cpp
    HedgedConnectionsFactory.cpp
    MultiplexedConnections.cpp

)

END()
