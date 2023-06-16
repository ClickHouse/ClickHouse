#pragma once

#include <base/types.h>
#include <base/strong_typedef.h>

namespace DB
{

STRONG_TYPEDEF(UInt32, MergeTreeDataFormatVersion)

const MergeTreeDataFormatVersion MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING {1};

}
