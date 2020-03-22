LIBRARY()

PEERDIR(
    clickhouse/base/common
)

SRCS(
    ExtendedLogChannel.cpp
    Loggers.cpp
    OwnFormattingChannel.cpp
    OwnPatternFormatter.cpp
    OwnSplitChannel.cpp
)

END()
