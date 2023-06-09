#pragma once

#include <Storages/SelectQueryInfo.h>

namespace DB
{

enum PartitionType : uint8_t
{
    UNPARTITIONED,
    RANDOM,
    HASH_PARTITIONED,
    RANGE_PARTITIONED,
    BUCKET_SHFFULE_HASH_PARTITIONED,
};

struct DataPartition
{
    PartitionType type;
    ManyExpressionActions expressions;
};


}
