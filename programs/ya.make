PROGRAM(clickhouse)

CFLAGS(
    -DENABLE_CLICKHOUSE_CLIENT
    -DENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG
    -DENABLE_CLICKHOUSE_SERVER
)

PEERDIR(
    clickhouse/base/daemon
    clickhouse/base/loggers
    clickhouse/programs/client/readpassphrase
    clickhouse/src
)

SRCS(
    main.cpp

    client/Client.cpp
    client/QueryFuzzer.cpp
    client/ConnectionParameters.cpp
    client/Suggest.cpp
    extract-from-config/ExtractFromConfig.cpp
    server/Server.cpp
    server/MetricsTransmitter.cpp
)

END()
