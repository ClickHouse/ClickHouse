#pragma once

#include <Storages/MergeTree/MarkRange.h>
#include <vector>

namespace DB
{

/// Partition `ranges` into contiguous sub-ranges, each containing exactly
/// `target_marks_per_chunk` marks (the last chunk may contain fewer).
/// A single MarkRange is split across a chunk boundary when needed.
/// `target_marks_per_chunk == 0` means "do not split": returns one chunk equal to `ranges`
/// (or no chunks if `ranges` is empty). The concatenation of the returned chunks, in order,
/// covers exactly the same marks as `ranges`.
std::vector<MarkRanges> splitMarkRanges(const MarkRanges & ranges, size_t target_marks_per_chunk);

}
