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
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

namespace DB
{

struct RangesInPatchParts;

/**  We use two-level hash map (_block_number -> (_block_offset -> (block_idx, row_idx))).
  *  Block numbers are usually the same for large ranges of consecutive rows.
  *  Therefore, we rarely switch between hash maps for blocks.
  *  It makes two-level hash map more cache-friendly than single-level ((_block_number, _block_offset) -> (block_idx, row_idx)).
  */
using OffsetsHashMap = absl::flat_hash_map<UInt64, std::pair<UInt32, UInt32>, DefaultHash<UInt64>>;
using PatchHashMap = absl::flat_hash_map<UInt64, OffsetsHashMap, DefaultHash<UInt64>>;

/**  A cache of hash tables and blocks for applying patch parts in Join mode.
  *  It avoids re-reading the same ranges of patch parts and rebuilding hash tables multiple times.
  *  The cache (and patch hash table) is optimized for the case when patch parts are almost sorted by _block_number,
  *  i.e., when data is inserted almost in the order of the order key (the order key has a timestamp value), which is the typical case.
  *
  *  The cache allows reading data from patches by small ranges and lowers the amount of patch parts (hash tables)
  *  to apply by aggregating data from multiple patch parts into a single hash table in the entry.
  *
  *  A cache entry is created for each patch part. To lower lock contention, each entry is split into buckets by the patch part's ranges.
  *  Each bucket has contiguous ranges of the patch part, and is sorted by the range's beginning.
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
