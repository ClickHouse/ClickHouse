LIBRARY()

CXXFLAGS(-Wno-c++2a-extensions)

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    BaseDaemon.cpp
    GraphiteWriter.cpp
)

END()
