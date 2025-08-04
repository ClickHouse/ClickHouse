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
#include <absl/container/flat_hash_map.h>

namespace DB
{

struct RangesInPatchParts;

/// We use two-level hash map (_block_number -> (_block_offset -> (patch_block_idx, row_number))).
/// Block numbers are usually the same for the large ranges of consecutive rows.
/// Therefore, we switch between hash maps for blocks rarely.
/// It makes two-level hash map more cache-friendly than single-level ((_block_number, _block_offset) -> (patch_block_idx, row_number)).
using OffsetsHashMap = absl::flat_hash_map<UInt64, std::pair<UInt64, UInt64>, DefaultHash<UInt64>>;
using PatchHashMap = absl::flat_hash_map<UInt64, OffsetsHashMap, DefaultHash<UInt64>>;

/// A cache for hash tables and blocks for applying patch parts in Join mode.
/// It avoids re-reading the same ranges of patch parts and rebuilding hash tables multiple times.
/// The cache (and patch hash table) is optimized for the case when patch parts are almost sorted by _block_number,
/// i.e., when data is inserted almost in the order of the order key (the order key has a timestamp value), which is the typical case.
/// Cache entries are created for each patch part. Each entry is split into buckets by ranges of the patch part to avoid lock contention.
/// Each bucket has contiguous ranges of the patch part and all buckets are sorted by the mark numbers in ranges.
struct PatchJoinCache
{
    using Reader = std::function<Block(const MarkRanges &)>;
    explicit PatchJoinCache(size_t num_buckets_) : num_buckets(num_buckets_) {}

    struct Entry
    {
        PatchHashMap hash_map;
        std::vector<BlockPtr> blocks;
        std::map<MarkRange, std::shared_future<void>> ranges_futures;

        UInt64 min_block = std::numeric_limits<UInt64>::max();
        UInt64 max_block = 0;

        mutable SharedMutex mutex;

        void addBlock(Block read_block);
        std::vector<std::shared_future<void>> addRangesAsync(const MarkRanges & ranges, Reader reader);
    };

    using EntryPtr = std::shared_ptr<Entry>;
    using Entries = std::vector<EntryPtr>;

    void init(const RangesInPatchParts & ranges_in_pathces);
    Entries getEntries(const String & patch_name, const MarkRanges & ranges, Reader reader);

private:
    std::pair<Entries, std::vector<MarkRanges>> getEntriesAndRanges(const String & patch_name, const MarkRanges & ranges);

    size_t num_buckets;
    mutable std::mutex mutex;
    absl::flat_hash_map<String, Entries> cache;
    absl::flat_hash_map<String, std::map<MarkRange, size_t>> ranges_to_buckets;
};

using PatchJoinCachePtr = std::shared_ptr<PatchJoinCache>;

}
