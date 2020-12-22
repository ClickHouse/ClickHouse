OWNER(g:clickhouse)

LIBRARY()

NO_COMPILER_WARNINGS()

PEERDIR(
    clickhouse/src/Common
)

CFLAGS(-g0)

SRCS(
    BaseDaemon.cpp
    GraphiteWriter.cpp
    SentryWriter.cpp
)

END()
