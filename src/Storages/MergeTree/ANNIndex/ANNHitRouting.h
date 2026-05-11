#pragma once

#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNIndexManager.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

/// Dispatch the flat list of ANN hits returned by the table-level manager back to the individual
/// parts that contain the rows. Uses a `partition_hash -> [part index]` inverted index to avoid
/// an O(hits * parts) scan.
///
/// After the routing pass every element of `parts_with_ranges` carries a tri-state:
///   - `read_hints.ann_search_results == nullopt`                       -> part is unindexed
///   - `read_hints.ann_search_results->block_coords.empty()`            -> covered but no hits
///   - `read_hints.ann_search_results->block_coords.size() > 0`         -> covered with hits
///
/// The lambdas abstract away two dependencies so the helper can be unit-tested without a live
/// ANN manager or a full-fat `IMergeTreeDataPart`:
///   - `hash_partition_id`: maps a part's partition id string to the same UInt64 the manager used
///     on the build side, so that the routing lookup keys line up.
///   - `is_part_covered`: classifies a part whose partition bucket exists but that received no
///     hits for this query as "covered" (empty hit list) vs "unindexed" (nullopt).
inline void routeANNHitsToParts(
    RangesInDataParts & parts_with_ranges,
    const std::vector<ANNSearchHit> & hits,
    const std::function<UInt64(const String &)> & hash_partition_id,
    const std::function<bool(const DataPartPtr &)> & is_part_covered)
{
    /// Step A: invert parts_with_ranges by their partition hash.
    std::unordered_map<UInt64, std::vector<size_t>> bucket;
    bucket.reserve(parts_with_ranges.size());
    for (size_t i = 0; i < parts_with_ranges.size(); ++i)
    {
        const String & partition_id = parts_with_ranges[i].data_part->info.getPartitionId();
        UInt64 ph = hash_partition_id(partition_id);
        bucket[ph].push_back(i);
    }

    /// Step B: route each hit to the unique part that covers its (partition_hash, block_number).
    /// Active parts in the same partition have disjoint block-number intervals, so the first
    /// (and only) match inside the partition bucket is the correct one.
    std::unordered_set<size_t> parts_with_hits;
    for (const auto & hit : hits)
    {
        auto it = bucket.find(hit.row.partition_hash);
        if (it == bucket.end())
            continue;
        for (size_t part_idx : it->second)
        {
            const auto & info = parts_with_ranges[part_idx].data_part->info;
            const Int64 block_no = static_cast<Int64>(hit.row.block_number);
            if (info.min_block <= block_no && block_no <= info.max_block)
            {
                auto & hints = parts_with_ranges[part_idx].read_hints;
                if (!hints.ann_search_results.has_value())
                    hints.ann_search_results.emplace();
                hints.ann_search_results->block_coords.emplace_back(hit.row.block_number, hit.row.block_offset);
                hints.ann_search_results->distances.push_back(hit.distance);
                parts_with_hits.insert(part_idx);
                break; /// coverage intervals are disjoint within a partition
            }
        }
    }

    /// Step C: parts that are covered but got zero hits get an empty `ann_search_results` so the
    /// range reader can distinguish them from unindexed parts (which stay `nullopt`).
    for (size_t i = 0; i < parts_with_ranges.size(); ++i)
    {
        if (parts_with_hits.contains(i))
            continue;
        if (is_part_covered(parts_with_ranges[i].data_part))
        {
            auto & hints = parts_with_ranges[i].read_hints;
            if (!hints.ann_search_results.has_value())
                hints.ann_search_results.emplace();
        }
    }
}

}

#endif
