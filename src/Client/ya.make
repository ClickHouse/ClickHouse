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
