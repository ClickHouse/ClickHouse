OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

CFLAGS(-g0)

SRCS(
    ExtendedLogChannel.cpp
    Loggers.cpp
    OwnFormattingChannel.cpp
    OwnPatternFormatter.cpp
    OwnSplitChannel.cpp
)

END()
