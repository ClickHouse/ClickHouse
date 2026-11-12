#pragma once

#include <cstdint>

namespace DB
{

enum class MergeTreeSerializationInfoVersion : uint8_t
{
    BASIC = 0,
    WITH_TYPES = 1,
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

/// The maximum number of buckets in the shared data of a `JSON` / `Object` column
/// (`MAP_WITH_BUCKETS` / `ADVANCED` shared-data serialization). Legitimate bucket counts are written
/// from the small MergeTree settings `object_shared_data_buckets_for_compact_part` /
/// `object_shared_data_buckets_for_wide_part`, both non-zero and capped at this value, so a bucket
/// count read from a stream that is outside `1 .. MAX_OBJECT_SHARED_DATA_BUCKETS` can only be data
/// corruption. This constant is the single source of truth for both the writer-side setting bound
/// and the reader-side validation.
inline constexpr uint64_t MAX_OBJECT_SHARED_DATA_BUCKETS = 256;


}
