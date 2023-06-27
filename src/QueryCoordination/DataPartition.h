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
    Names keys; /// keys for partition
    size_t keys_size = 0;

    bool partition_by_bucket_num = false;

    String toString() const
    {
        String res;
        switch (type)
        {
            case UNPARTITIONED:
                res += "UNPARTITIONED";
                break;
            case RANDOM:
                res += "RANDOM";
                break;
            case HASH_PARTITIONED:
                res += "HASH_PARTITIONED";
                break;
            case RANGE_PARTITIONED:
                res += "RANGE_PARTITIONED";
                break;
            case BUCKET_SHFFULE_HASH_PARTITIONED:
                res += "BUCKET_SHFFULE_HASH_PARTITIONED";
        }

        if (keys_size)
        {
            res += ", partition keys:";

            for (const auto & key : keys)
            {
                res += (key + ", ");
            }
        }

        return res;
    }
};

}
