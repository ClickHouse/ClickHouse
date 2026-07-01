#pragma once

#include <cstdint>

namespace DB
{

enum class MergeTreeSerializationInfoVersion : uint8_t
{
    BASIC = 0,
    WITH_TYPES = 1,
    /// Same on-disk shape as `WITH_TYPES`. The per-column counts (num_rows/num_defaults) are always
    /// written inline in `serialization.json`, reconciled with the exact counts from the explicit
    /// statistics at write time (see `EstimatesBuilder` and `IMergeTreeDataPart::getEstimates`).
    WITH_EXTERNAL_STATISTICS = 2,
};

enum class MergeTreeStringSerializationVersion : uint8_t
{
    SINGLE_STREAM = 0,
    WITH_SIZE_STREAM = 1,
};

enum class MergeTreeNullableSerializationVersion : uint8_t
{
    BASIC = 0,
    ALLOW_SPARSE = 1,
};

enum class MergeTreeObjectSerializationVersion : uint8_t
{
    V1,
    V2,
    V3,
};

enum class MergeTreeObjectSharedDataSerializationVersion : uint8_t
{
    MAP,
    MAP_WITH_BUCKETS,
    ADVANCED,
};

enum class MergeTreeDynamicSerializationVersion : uint8_t
{
    V1,
    V2,
    V3,
};

enum class MergeTreeMapSerializationVersion : uint8_t
{
    BASIC = 0,
    WITH_BUCKETS = 1,
};

enum class MergeTreeMapBucketsStrategy : uint8_t
{
    CONSTANT = 0,
    SQRT = 1,
    LINEAR = 2,
};


}
