LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    BaseDaemon.cpp
    GraphiteWriter.cpp
)

END()
