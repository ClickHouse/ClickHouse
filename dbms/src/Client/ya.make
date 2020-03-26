LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
    contrib/libs/poco/NetSSL_OpenSSL
)

SRCS(
    Connection.cpp
    ConnectionPoolWithFailover.cpp
    MultiplexedConnections.cpp
    TimeoutSetter.cpp
)

END()
