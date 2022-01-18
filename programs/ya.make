OWNER(g:clickhouse)

PROGRAM(clickhouse)

CFLAGS(
    -DENABLE_CLICKHOUSE_CLIENT
    -DENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG
    -DENABLE_CLICKHOUSE_SERVER
)

ADDINCL(
    # It's not used due to a lack of -DUSE_BASE64 option. But ya.make still tries to resolve it.
    contrib/restricted/turbo_base64
)

PEERDIR(
    clickhouse/base/daemon
    clickhouse/base/loggers
    clickhouse/src
)

CFLAGS(-g0)

SRCS(
    main.cpp

    client/Client.cpp
    client/QueryFuzzer.cpp
    client/ConnectionParameters.cpp
    client/Suggest.cpp
    client/TestHint.cpp
    extract-from-config/ExtractFromConfig.cpp
    server/Server.cpp
    server/MetricsTransmitter.cpp
)

END()
