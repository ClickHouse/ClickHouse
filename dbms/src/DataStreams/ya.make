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
    materializeBlock.cpp
    NativeBlockInputStream.cpp
    NativeBlockOutputStream.cpp
    ParallelParsingBlockInputStream.cpp
    RemoteBlockInputStream.cpp
    SizeLimits.cpp
    SquashingBlockOutputStream.cpp
    SquashingTransform.cpp
)

END()
