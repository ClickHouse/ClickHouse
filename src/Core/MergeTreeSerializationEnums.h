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

}
