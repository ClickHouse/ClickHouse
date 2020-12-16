# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/MongoDB
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
    CountingBlockOutputStream.cpp
    DistinctSortedBlockInputStream.cpp
    ExecutionSpeedLimits.cpp
    ExpressionBlockInputStream.cpp
    IBlockInputStream.cpp
    InputStreamFromASTInsertQuery.cpp
    InternalTextLogsRowOutputStream.cpp
    LimitBlockInputStream.cpp
    MaterializingBlockInputStream.cpp
    MergingSortedBlockInputStream.cpp
    MongoDBBlockInputStream.cpp
    NativeBlockInputStream.cpp
    NativeBlockOutputStream.cpp
    ParallelParsingBlockInputStream.cpp
    PushingToViewsBlockOutputStream.cpp
    RemoteBlockInputStream.cpp
    RemoteBlockOutputStream.cpp
    RemoteQueryExecutor.cpp
    SizeLimits.cpp
    SquashingBlockInputStream.cpp
    SquashingBlockOutputStream.cpp
    SquashingTransform.cpp
    TTLBlockInputStream.cpp
    copyData.cpp
    finalizeBlock.cpp
    materializeBlock.cpp
    narrowBlockInputStreams.cpp

)

END()
