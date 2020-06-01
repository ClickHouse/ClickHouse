LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

NO_COMPILER_WARNINGS()

SRCS(
    AddingDefaultBlockOutputStream.cpp
    AddingDefaultsBlockInputStream.cpp
    AsynchronousBlockInputStream.cpp
    BlockIO.cpp
    BlockStreamProfileInfo.cpp
    CheckConstraintsBlockOutputStream.cpp
    CheckSortedBlockInputStream.cpp
    ColumnGathererStream.cpp
    ConvertingBlockInputStream.cpp
    copyData.cpp
    CountingBlockOutputStream.cpp
    CreatingSetsBlockInputStream.cpp
    DistinctSortedBlockInputStream.cpp
    ExecutionSpeedLimits.cpp
    ExpressionBlockInputStream.cpp
    FilterBlockInputStream.cpp
    finalizeBlock.cpp
    IBlockInputStream.cpp
    InputStreamFromASTInsertQuery.cpp
    InternalTextLogsRowOutputStream.cpp
    LimitBlockInputStream.cpp
    materializeBlock.cpp
    MaterializingBlockInputStream.cpp
    MergingSortedBlockInputStream.cpp
    narrowBlockInputStreams.cpp
    NativeBlockInputStream.cpp
    NativeBlockOutputStream.cpp
    ParallelParsingBlockInputStream.cpp
    PushingToViewsBlockOutputStream.cpp
    RemoteBlockInputStream.cpp
    RemoteBlockOutputStream.cpp
    SizeLimits.cpp
    SquashingBlockInputStream.cpp
    SquashingBlockOutputStream.cpp
    SquashingTransform.cpp
    TTLBlockInputStream.cpp
)

END()
