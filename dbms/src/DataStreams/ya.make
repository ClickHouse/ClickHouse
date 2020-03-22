LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    AsynchronousBlockInputStream.cpp
    BlockIO.cpp
    BlockStreamProfileInfo.cpp
    ColumnGathererStream.cpp
    ExecutionSpeedLimits.cpp
    IBlockInputStream.cpp
    NativeBlockInputStream.cpp
    NativeBlockOutputStream.cpp
    SizeLimits.cpp
)

END()
