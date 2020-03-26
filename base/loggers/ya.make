LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    ExtendedLogChannel.cpp
    Loggers.cpp
    OwnFormattingChannel.cpp
    OwnPatternFormatter.cpp
    OwnSplitChannel.cpp
)

END()
