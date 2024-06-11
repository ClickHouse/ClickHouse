#pragma once

#include <DataTypes/DataTypesNumber.h>

#include <Compression/ICompressionCodec.h>

namespace DB
{

struct QueueBlockNumberColumn
{
    static const String name;
    static const DataTypePtr type;
    static const ASTPtr codec;
};

struct QueueBlockOffsetColumn
{
    static const String name;
    static const DataTypePtr type;
    static const ASTPtr codec;
};

struct QueuePartitionIdColumn
{
    static const String name;
    static const DataTypePtr type;
};

bool isQueueModeColumn(const String & column_name);

void materializeSortingColumnsForQueueMode(Block & block, const String & partition_id, int64_t block_number);

}
