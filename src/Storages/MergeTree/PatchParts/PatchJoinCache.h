#pragma once
#include <future>
#include <Common/SharedMutex.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <IO/SharedThreadPools.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

namespace DB
{

struct RangesInPatchParts;

/**  We use two-level map (_block_number -> (_block_offset -> (block_idx, row_idx))).
  *  Block numbers are usually the same for large ranges of consecutive rows.
  *  Therefore, we rarely switch between maps for blocks.
  *  It makes two-level map more cache-friendly than single-level ((_block_number, _block_offset) -> (block_idx, row_idx)).
  *
  *  There are four facts about block offsets:
  *  1. Block offsets are unique within a block number in regular parts.
  *  2. Block offsets are sorted within a block number in regular parts.
  *  3. Block offsets have large sorted ranges within a block number in patch parts.
  *  4. Block offsets are not globally sorted even within a block number and may have duplicates in patch parts.
  *
  *  We build a sorted map _block_offset -> (block_idx, row_idx) for each block number to resolve (4).
  *  When applying a patch, the order of rows in the read block is not violated, and (1) and (2) are true.
  *  Then we build a hash table (_block_number -> iterator in sorted map)
  *  and apply the patch using a two-iterators-like algorithm (see applyPatchJoin function).
  *
  *  Because of (3), values are mostly inserted at the end of the map, and we can utilize
  *  the emplace hint iterator, to make insertion complexity O(1) on average instead of O(log n).
  */

using PatchOffsetsMap = absl::btree_map<UInt64, std::pair<UInt32, UInt32>>;
using PatchHashMap = absl::node_hash_map<UInt64, PatchOffsetsMap, HashCRC32<UInt64>>;

/**  A cache of maps and blocks for applying patch parts in Join mode.
  *  It avoids re-reading the same ranges of patch parts and rebuilding maps multiple times.
  *  The cache (and patch map) is optimized for the case when patch parts are almost sorted by _block_number,
  *  i.e., when data is inserted almost in the order of the order key (the order key has a timestamp value), which is the typical case.
  *
  *  The cache allows reading data from patches in small ranges and lowers the amount of patch parts
  *  to apply by aggregating data from multiple read blocks into a single map in the entry.
  *
  *  A cache entry is created for each patch part. To lower lock contention, each entry is split into buckets
  *  by the patch part's ranges. Each bucket has contiguous ranges of the patch part, and is sorted by the range's beginning.
  */
struct PatchJoinCache
{
    using Reader = std::function<Block(const MarkRanges &)>;
    explicit PatchJoinCache(size_t num_buckets_) : num_buckets(num_buckets_) {}

    struct Entry
    {
        PatchHashMap hash_map;
        std::vector<BlockPtr> blocks;
        absl::node_hash_map<MarkRange, std::shared_future<void>, MarkRangeHash> ranges_futures;

        UInt64 min_block = std::numeric_limits<UInt64>::max();
        UInt64 max_block = 0;
        mutable SharedMutex mutex;

        void addBlock(Block read_block);
        std::vector<std::shared_future<void>> addRangesAsync(const MarkRanges & ranges, Reader reader);
    };

    struct PatchStatsEntry
    {
        bool initialized = false;
        PatchStatsMap stats;
        mutable std::mutex mutex;
    };

    using EntryPtr = std::shared_ptr<Entry>;
    using Entries = std::vector<EntryPtr>;
    using PatchStatsEntryPtr = std::shared_ptr<PatchStatsEntry>;

    /// Initializes the cache, creates a mapping from the ranges to buckets.
    /// Cache entries should be get for the same ranges later.
    void init(const RangesInPatchParts & ranges_in_patches);

    PatchStatsEntryPtr getStatsEntry(const DataPartPtr & patch_part, const MergeTreeReaderSettings & settings);
    Entries getEntries(const String & patch_name, const MarkRanges & ranges, Reader reader);

private:
    std::pair<Entries, std::vector<MarkRanges>> getEntriesAndRanges(const String & patch_name, const MarkRanges & ranges);
    PatchStatsEntryPtr getOrCreatePatchStats(const String & patch_name);

    size_t num_buckets;
    mutable std::mutex mutex;

    absl::node_hash_map<String, Entries> cache TSA_GUARDED_BY(mutex);
    absl::node_hash_map<String, PatchStatsEntryPtr> stats_cache TSA_GUARDED_BY(mutex);
    /// Ranges are filled on initialization and then are read-only and don't require a lock.
    absl::node_hash_map<String, absl::node_hash_map<MarkRange, size_t, MarkRangeHash>> ranges_to_buckets;
};

using PatchJoinCachePtr = std::shared_ptr<PatchJoinCache>;

}
