#pragma once

#include <Interpreters/Cache/PartAggregationCache.h>
#include <Interpreters/Aggregator.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class MergeTreeData;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

/// Eagerly populates the PartAggregationCache for a set of MergeTree parts.
/// For each part, reads the data via MergeTreeSequentialSource, runs aggregation
/// (final=false), and stores the intermediate aggregation state in the cache.
///
/// Called from the query plan optimization when cache writes are enabled
/// but the cache is cold for the given query.
void populatePartAggregationCache(
    const PartAggregationCachePtr & cache,
    const IASTHash & query_hash,
    const RangesInDataParts & parts,
    const Aggregator::Params & params,
    const Block & input_header,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context);

}
