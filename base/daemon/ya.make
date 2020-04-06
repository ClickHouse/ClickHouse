LIBRARY()

CXXFLAGS(-Wno-c++2a-extensions)

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    BaseDaemon.cpp
    GraphiteWriter.cpp
)

END()
