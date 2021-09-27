#pragma once

#include <common/types.h>
#include <common/StrongTypedef.h>

namespace DB
{
using MergeTreeDataFormatVersion = StrongTypedef<UInt32, struct MergeTreeDataFormatVersionTag>;
constexpr MergeTreeDataFormatVersion MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING {1};
}
