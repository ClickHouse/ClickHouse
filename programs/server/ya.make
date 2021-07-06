OWNER(g:clickhouse)

PROGRAM(clickhouse-server)

PEERDIR(
    clickhouse/base/common
    clickhouse/base/daemon
    clickhouse/base/loggers
    clickhouse/src
    contrib/libs/poco/NetSSL_OpenSSL
)

CFLAGS(-g0)

SRCS(
    clickhouse-server.cpp

    MetricsTransmitter.cpp
    Server.cpp
)

END()
