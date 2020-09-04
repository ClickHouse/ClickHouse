#pragma once

#include <Core/Types.h>

namespace DB
{

/// Type of Merge. Used to control amount of different merges during merges
/// assignment. Also allows to apply special logic during merge process
/// (mergePartsToTemporaryPart). Stored in FutureMergedMutatedPart and
/// ReplicatedMergeTreeLogEntry.
///
/// Order is important, don't try to change it.
enum class MergeType
{
    REGULAR = 1,
    TTL_DELETE = 2,
};

/// Check parsed merge_type from raw int and get enum value.
MergeType checkAndGetMergeType(UInt64 merge_type);

String toString(MergeType merge_type);

/// Check this merge assigned with TTL
bool isTTLMergeType(MergeType merge_type);

}
