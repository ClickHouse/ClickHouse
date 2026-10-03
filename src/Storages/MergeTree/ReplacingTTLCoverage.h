#pragma once

#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct StorageInMemoryMetadata;
struct MergeTreeSettings;

/**
  * A merge or a mutation applies a row `TTL ... DELETE` to the rows that it reads. `ReplacingMergeTree` resolves a
  * key among the versions that are stored, so if these rows include the newest version of a key and an older version
  * of the key is stored in another part of the partition, deleting the newest version makes the older one visible
  * again (https://github.com/Clickhouse/Clickhouse/issues/122528)
  *
  * This cannot happen when an older version has expired whenever a newer version has:
  * - the `TTL` expression and its `WHERE` condition read only columns that all versions of a key in a partition share:
  *   the stored columns of the sorting key, and the stored columns that are the partition key or its elements;
  * - the `TTL` expression is the version column of a date or time type plus positive intervals, e.g.
  *   `ver + INTERVAL 30 DAY`, and the `WHERE` condition, if any, reads only such shared columns.
  *
  * Returns true if the table is a `ReplacingMergeTree` with a row `TTL` of another form and the setting
  * `replacing_ttl_whole_partition_only` is enabled. Then only `TTLDrop` and `TTLDelete` merges delete rows by row `TTL`,
  * and every such merge of the table includes all parts of its partition (see `MergeSelectorApplier` and
  * `MergeTreeDataMergerMutator::selectAllPartsToMergeWithinPartition`). Other merges and mutations keep the expired rows.
  */

bool rowTTLNeedsWholePartitionMerge(const StorageInMemoryMetadata & metadata, const MergeTreeData::MergingParams & merging_params, const MergeTreeSettings & settings);
}
