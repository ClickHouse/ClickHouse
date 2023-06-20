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
    Names keys;            /// keys for partition
    size_t keys_size = 0;

    bool partition_by_bucket_num = false;
};

}
