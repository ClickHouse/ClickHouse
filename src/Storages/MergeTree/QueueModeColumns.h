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

struct QueueReplicaColumn
{
    static const String name;
    static const DataTypePtr type;
};

bool isQueueModeColumn(const String & column_name);

void materializeQueueSortingColumns(Block & block, int64_t block_number);
void materializeQueuePartitionColumns(Block & block, const String & replica_name);

}
