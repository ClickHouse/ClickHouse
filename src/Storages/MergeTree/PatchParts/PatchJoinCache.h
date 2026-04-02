#pragma once
#include <mutex>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/SharedMutex.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

namespace DB
{

struct RangesInPatchParts;

/**  We use two-level map (_block_number -> (_block_offset -> row_idx)).
  *  Block numbers are usually the same for large ranges of consecutive rows.
  *  Therefore, we rarely switch between maps for blocks.
  *  It makes two-level map more cache-friendly than single-level ((_block_number, _block_offset) -> row_idx).
  *
  *  There are four facts about block offsets:
  *  1. Block offsets are unique within a block number in regular parts.
  *  2. Block offsets are sorted within a block number in regular parts.
  *  3. Block offsets have large sorted ranges within a block number in patch parts.
  *  4. Block offsets are not globally sorted even within a block number and may have duplicates in patch parts.
  *
  *  We build a sorted map _block_offset -> row_idx for each block number to resolve (4).
  *  When applying a patch, the order of rows in the read block is not violated, and (1) and (2) are true.
  *  Then we build a hash table (_block_number -> iterator in sorted map)
  *  and apply the patch using a two-iterators-like algorithm (see `applyPatchJoin`).
  *
  *  Because of (3), values are mostly inserted at the end of the map, and we can utilize
  *  the emplace hint iterator, to make insertion complexity O(1) on average instead of O(log n).
  *
  *  All patch rows are accumulated into a single block per entry, so the map value is
  *  a single UInt32 row index into that accumulated block.
  */

using PatchOffsetsMap = absl::btree_map<
    UInt64,
    UInt32,
    std::less<>,
    AllocatorWithMemoryTracking<std::pair<const UInt64, UInt32>>>;

using PatchHashMap = absl::node_hash_map<
    UInt64,
    PatchOffsetsMap,
    HashCRC32<UInt64>,
    std::equal_to<>,
    AllocatorWithMemoryTracking<std::pair<const UInt64, PatchOffsetsMap>>>;

/**  A cache of maps and blocks for applying patch parts in Join mode.
  *  It avoids re-reading the same ranges of patch parts and rebuilding maps multiple times.
  *  The cache (and patch map) is optimized for the case when patch parts are almost sorted by _block_number,
  *  i.e., when data is inserted almost in the order of the order key (the order key has a timestamp value), which is the typical case.
  *
  *  The cache allows reading data from patches in small ranges and lowers the amount of patch parts
  *  to apply by aggregating data from multiple read blocks into a single map in the entry.
  *
  *  A single cache entry is created for each patch part. All read ranges are accumulated into
  *  a single block and hash map within the entry.
  */
struct PatchJoinCache
{
    using Reader = std::function<Block(const MarkRanges &)>;
    PatchJoinCache() = default;

    struct Entry
    {
        PatchHashMap hash_map;
        Block block;
        MarkRanges read_ranges;

        UInt64 min_block = std::numeric_limits<UInt64>::max();
        UInt64 max_block = 0;
        std::exception_ptr error;
        mutable SharedMutex mutex;

        void addBlock(Block read_block, const MarkRanges & new_ranges = {});
        MarkRanges getUnreadRanges(const MarkRanges & ranges) const;
    };

    struct PatchStatsEntry
    {
        bool initialized = false;
        PatchStatsMap stats;
        mutable std::mutex mutex;
    };

    using EntryPtr = std::shared_ptr<Entry>;
    using PatchStatsEntryPtr = std::shared_ptr<PatchStatsEntry>;

    /// Initializes the cache, creates one entry per patch name.
    void init(const RangesInPatchParts & ranges_in_patches);

    PatchStatsEntryPtr getStatsEntry(const DataPartPtr & patch_part, const MergeTreeReaderSettings & settings);
    EntryPtr getEntry(const String & patch_name);

private:
    PatchStatsEntryPtr getOrCreatePatchStats(const String & patch_name);

    mutable std::mutex mutex;

    absl::node_hash_map<String, EntryPtr> cache TSA_GUARDED_BY(mutex);
    absl::node_hash_map<String, PatchStatsEntryPtr> stats_cache TSA_GUARDED_BY(mutex);
    /// Ranges are filled on initialization and then are read-only and don't require a lock.
    absl::node_hash_map<String, MarkRanges> all_ranges_by_name;
};

using PatchJoinCachePtr = std::shared_ptr<PatchJoinCache>;

}
