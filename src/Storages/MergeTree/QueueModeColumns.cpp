#include <memory>

#include <Core/Block.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <Storages/MergeTree/QueueModeColumns.h>

namespace DB
{

namespace
{

ASTPtr getCompressionCodecDeltaLZ4()
{
    return makeASTFunction("CODEC",
        makeASTFunction("Delta", std::make_shared<ASTLiteral>(static_cast<UInt64>(8))),
        std::make_shared<ASTIdentifier>("LZ4"));
}

ASTPtr getCompressionCodecDoubleDeltaLZ4()
{
    return makeASTFunction("CODEC",
        std::make_shared<ASTIdentifier>("DoubleDelta"),
        std::make_shared<ASTIdentifier>("LZ4"));
}

ColumnWithTypeAndName createPartitionIdColumn(size_t rows_count, const String & partition_id)
{
    return {
        QueuePartitionIdColumn::type->createColumnConst(rows_count, partition_id)->convertToFullColumnIfConst(),
        QueuePartitionIdColumn::type,
        QueuePartitionIdColumn::name,
    };
}

ColumnWithTypeAndName createBlockNumberColumn(size_t rows_count, int64_t block_number)
{
    return {
        QueueBlockNumberColumn::type->createColumnConst(rows_count, block_number)->convertToFullColumnIfConst(),
        QueueBlockNumberColumn::type,
        QueueBlockNumberColumn::name,
    };
}

ColumnWithTypeAndName createBlockOffsetColumn(size_t rows_count)
{
    auto block_offset_column = ColumnUInt64::create(rows_count);
    ColumnUInt64::Container & vec = block_offset_column->getData();

    UInt64 start_value = 0;
    UInt64 * pos = vec.data();
    UInt64 * end = &vec[rows_count];
    iota(pos, static_cast<size_t>(end - pos), start_value);

    return {
        std::move(block_offset_column),
        std::make_shared<DataTypeUInt64>(),
        QueueBlockOffsetColumn::name,
    };
}

}

CompressionCodecPtr getCompressionCodecDelta(UInt8 data_bytes_size);
CompressionCodecPtr getCompressionCodecDoubleDelta(UInt8 data_bytes_size);

const String QueueBlockNumberColumn::name = "_queue_block_number";
const DataTypePtr QueueBlockNumberColumn::type = std::make_shared<DataTypeUInt64>();
const ASTPtr QueueBlockNumberColumn::codec = getCompressionCodecDeltaLZ4();

const String QueueBlockOffsetColumn::name = "_queue_block_offset";
const DataTypePtr QueueBlockOffsetColumn::type = std::make_shared<DataTypeUInt64>();
const ASTPtr QueueBlockOffsetColumn::codec = getCompressionCodecDoubleDeltaLZ4();

const String QueuePartitionIdColumn::name = "_queue_partition_id";
const DataTypePtr QueuePartitionIdColumn::type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

bool isQueueModeColumn(const String & column_name)
{
    return column_name == QueueBlockNumberColumn::name
        || column_name == QueueBlockOffsetColumn::name
        || column_name == QueuePartitionIdColumn::name;
}

void materializeSortingColumnsForQueueMode(Block & block, const String & partition_id, int64_t block_number)
{
    /// because queue columns are materialized
    /// in insert they will be filled with default value
    block.erase(QueuePartitionIdColumn::name);
    block.erase(QueueBlockNumberColumn::name);
    block.erase(QueueBlockOffsetColumn::name);

    size_t rows_count = block.rows();

    block.insert(createPartitionIdColumn(rows_count, partition_id));
    block.insert(createBlockNumberColumn(rows_count, block_number));
    block.insert(createBlockOffsetColumn(rows_count));
}

}
