#pragma once
#include <Common/AllocatorWithMemoryTracking.h>
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
  *  The cache is pre-built as a separate step before reading from MergeTree table.
  *  Data is distributed into `num_buckets` entries per patch part by `_block_number % num_buckets`,
  *  so that each entry can be built independently without contention.
  *  After the cache is built, it is read-only and no locking is needed.
  *
  *  Build is parallelized by mark ranges (not by patch parts), so even a single
  *  large patch part benefits from multiple threads. Each bucket is filled
  *  by exactly one thread, so no locking is required.
  *
  *  Entries are kept per patch part name because different patches may have
  *  different column schemas (different UPDATE statements).
  */
struct PatchJoinCache
{
    using Reader = std::function<Block(const MarkRanges &)>;
    PatchJoinCache() = default;

    struct Entry
    {
        PatchHashMap hash_map;
        Block block;

        UInt64 min_block = std::numeric_limits<UInt64>::max();
        UInt64 max_block = 0;

        /// Lock-free: used during pre-build when each bucket has a single writer.
        void addBlock(Block read_block);
    };

    using EntryPtr = std::shared_ptr<Entry>;
    using Entries = std::vector<EntryPtr>;

    /// A callback that creates a reader function for a given patch part name.
    /// The returned Reader reads a set of mark ranges and returns a Block with patch data.
    using ReaderFactory = std::function<Reader(const String & patch_name)>;

    /// Initializes the cache with `num_buckets` entries per patch part.
    void init(const RangesInPatchParts & ranges_in_patches, size_t num_buckets);

    /// Pre-builds the cache by reading all patch data for Join-mode patches.
    /// Parallelized by ranges across `num_threads`. Each bucket is filled lock-free.
    void build(const ReaderFactory & reader_factory, size_t num_threads);

    bool isBuilt() const { return built; }

    /// Returns the entries for a specific patch part (all buckets).
    const Entries & getEntries(const String & patch_name) const;
    size_t getNumBuckets() const { return num_buckets; }

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
    size_t num_buckets = 1;
    bool built = false;

    /// Per-patch-name buckets, each vector has size = num_buckets. Immutable after build.
    absl::node_hash_map<String, Entries> cache;

    /// Ranges are filled on initialization and then are read-only.
    absl::node_hash_map<String, MarkRanges> all_ranges_by_name;
};

using PatchJoinCachePtr = std::shared_ptr<PatchJoinCache>;

}
