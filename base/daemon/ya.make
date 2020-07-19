LIBRARY()

NO_COMPILER_WARNINGS()

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    BaseDaemon.cpp
    GraphiteWriter.cpp
    SentryWriter.cpp
)

END()
