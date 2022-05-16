#pragma once

#include <base/types.h>

namespace DB
{

/// Type of Merge. Used to control amount of different merges during merges
/// assignment. Also allows to apply special logic during merge process
/// Stored in FutureMergedMutatedPart and
/// ReplicatedMergeTreeLogEntry.
///
/// Order is important, don't try to change it.
enum class MergeType
{
    /// Just regular merge
    Regular = 1,
    /// Merge assigned to delete some data from parts (with TTLMergeSelector)
    TTLDelete = 2,
    /// Merge with recompression
    TTLRecompress = 3,
};

/// Check parsed merge_type from raw int and get enum value.
MergeType checkAndGetMergeType(UInt64 merge_type);

/// Check this merge assigned with TTL
bool isTTLMergeType(MergeType merge_type);

}
