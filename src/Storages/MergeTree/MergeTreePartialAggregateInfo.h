#pragma once

#include <Interpreters/Cache/PartialAggregateInfo.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/// Build `PartialAggregateInfo` from a MergeTree data part (projection names use `parent:projection` form).
/// `skip_execution_time_cache_lookup` stays false here; set it from `MergeTreeReaderSettings` in `readInOrder` /
/// `LazyReadFromMergeTreeSource`, or in `buildPartialAggregateInfoFromCurrentTask` for pooled reads.
PartialAggregateInfoPtr partialAggregateInfoFromMergeTreePart(const IMergeTreeDataPart & data_part);

}
