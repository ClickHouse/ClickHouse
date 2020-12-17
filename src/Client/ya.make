# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/NetSSL_OpenSSL
)


SRCS(
    Connection.cpp
    ConnectionPoolWithFailover.cpp
    MultiplexedConnections.cpp
    TimeoutSetter.cpp

)

END()
