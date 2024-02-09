#include <memory>
#include <Core/Block.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <Storages/BlockNumberColumn.h>
#include <Storages/QueueModeColumns.h>

#include <Compression/CompressionCodecMultiple.h>

namespace DB
{

namespace
{

size_t getBlockRowsCount(const Block & block)
{
    size_t total_rows = 0;

    for (const auto & column_with_type : block.getColumnsWithTypeAndName())
        if (column_with_type.column)
            total_rows = std::max(total_rows, column_with_type.column->size());

    return total_rows;
}

ColumnWithTypeAndName createReplicaColumn(size_t rows_count, const String & replica_name)
{
    return {
        QueueReplicaColumn::type->createColumnConst(rows_count, replica_name)->convertToFullColumnIfConst(),
        QueueReplicaColumn::type,
        QueueReplicaColumn::name,
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

CompressionCodecPtr getCompressionCodecDoubleDelta(UInt8 data_bytes_size);

const String QueueBlockNumberColumn::name = "_queue_block_number";
const DataTypePtr QueueBlockNumberColumn::type = BlockNumberColumn::type;
const CompressionCodecPtr QueueBlockNumberColumn::compression_codec = BlockNumberColumn::compression_codec;

const String QueueBlockOffsetColumn::name = "_queue_block_offset";
const DataTypePtr QueueBlockOffsetColumn::type = std::make_shared<DataTypeUInt64>();
const CompressionCodecPtr QueueBlockOffsetColumn::compression_codec = getCompressionCodecDoubleDelta(QueueBlockOffsetColumn::type->getSizeOfValueInMemory());

const String QueueReplicaColumn::name = "_queue_replica";
const DataTypePtr QueueReplicaColumn::type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

bool isQueueModeColumn(const String & column_name)
{
    return column_name == QueueBlockNumberColumn::name
        || column_name == QueueBlockOffsetColumn::name
        || column_name == QueueReplicaColumn::name;
}

void materializeQueueSortingColumns(Block & block, int64_t block_number)
{
    // because queue columns are materialized
    // in insert they will be filled with default value
    block.erase(QueueBlockNumberColumn::name);
    block.erase(QueueBlockOffsetColumn::name);

    size_t rows_count = getBlockRowsCount(block);

    block.insert(createBlockNumberColumn(rows_count, block_number));
    block.insert(createBlockOffsetColumn(rows_count));
}

void materializeQueuePartitionColumns(Block & block, const String & replica_name)
{
    block.erase(QueueReplicaColumn::name);

    size_t rows_count = getBlockRowsCount(block);

    block.insert(createReplicaColumn(rows_count, replica_name));
}

}
