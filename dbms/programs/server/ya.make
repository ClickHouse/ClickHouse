PROGRAM(clickhouse-server)

PEERDIR(
    clickhouse/base/daemon
    clickhouse/base/loggers
    clickhouse/dbms/src
)

SRCS(
    clickhouse-server.cpp

    InterserverIOHTTPHandler.cpp
    MetricsTransmitter.cpp
    ReplicasStatusHandler.cpp
    RootRequestHandler.cpp
    Server.cpp
    TCPHandler.cpp
)

END()
