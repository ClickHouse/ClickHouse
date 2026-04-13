#pragma once
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Storages/MergeTree/PatchParts/PatchBlockIndex.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

namespace DB
{

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
  *  The cache is a pure data structure: `init` sets up empty entries,
  *  and the pipeline fills them via `Entry::addBlock`.
  *
  *  Each patch part has a single Entry. When the on-disk PatchBlockIndex
  *  is available, the entry's `index` field is set and the hash_map is
  *  not built. Otherwise, the legacy hash-map-based path is used.
  *
  *  After the cache is built, it is read-only and no locking is needed.
  *
  *  Entries are kept per patch part name because different patches may have
  *  different column schemas (different UPDATE statements).
  */
struct PatchJoinCache
{
    PatchJoinCache();
    ~PatchJoinCache();

    struct Entry
    {
        /// On-disk index loaded from patch part files (when available).
        std::shared_ptr<PatchBlockIndex> index;

        /// Legacy: hash map built at runtime (when index absent).
        PatchHashMap hash_map;
        Block block;

        UInt64 min_block = std::numeric_limits<UInt64>::max();
        UInt64 max_block = 0;

        bool hasIndex() const { return index && index->loaded(); }

        /// Lock-free: used during build when each entry has a single writer.
        void addBlock(Block read_block);
    };

    using EntryPtr = std::shared_ptr<Entry>;

    /// Initialize empty cache structure (one entry per patch part name). No I/O.
    void init(const RangesInPatchParts & ranges_in_patches);

    /// Returns the entry for a specific patch part. Returns nullptr if not found.
    EntryPtr getEntry(const String & patch_name) const;

    const MarkRanges & getAllRanges(const String & patch_name) const
    {
        auto it = all_ranges_by_name.find(patch_name);
        if (it == all_ranges_by_name.end())
        {
            static const MarkRanges empty;
            return empty;
        }
        return it->second;
    }

private:
    /// One entry per patch part name. Immutable after build.
    absl::node_hash_map<String, EntryPtr> cache;

    /// Ranges are filled on initialization and then are read-only.
    absl::node_hash_map<String, MarkRanges> all_ranges_by_name;
};

using PatchJoinCachePtr = std::shared_ptr<PatchJoinCache>;

struct PatchJoinReadResult : public PatchReadResult
{
    PatchJoinCache::EntryPtr entry;
    const PatchJoinCache * cache = nullptr;

    bool empty() const override { return !entry; }
};

PatchToApplyPtr applyPatchJoin(const Block & result_block, const PatchJoinCache::Entry & entry);

}
