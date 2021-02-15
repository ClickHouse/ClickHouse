# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
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
    MongoDBBlockInputStream.cpp
    narrowBlockInputStreams.cpp
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

)

END()
