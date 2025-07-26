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

/// We use two-level hash map (_block_number -> (_block_offset -> row_number)).
/// Block number are usually the same for the large ranges of consecutive rows.
/// Therefore we switch between hash maps for blocks rarely.
/// It makes two-level hash map more cache-friendly than single-level ((_block_number, _block_offset) -> row_number).
using OffsetsHashMap = absl::flat_hash_map<UInt64, std::pair<UInt64, UInt64>, DefaultHash<UInt64>>;
using PatchHashMap = absl::flat_hash_map<UInt64, OffsetsHashMap, DefaultHash<UInt64>>;

struct PatchJoinCache
{
    using Reader = std::function<Block(const MarkRanges &)>;
    PatchJoinCache(size_t num_buckets_, ThreadPool & thread_pool_);

    struct Entry
    {
        PatchHashMap hash_map;
        std::vector<BlockPtr> blocks;
        std::map<MarkRange, std::shared_future<void>> ranges_futures;

        UInt64 min_block = std::numeric_limits<UInt64>::max();
        UInt64 max_block = 0;

        mutable SharedMutex mutex;

        void addBlock(Block read_block);
        std::vector<std::shared_future<void>> addRangesAsync(const MarkRanges & ranges, ThreadPool & pool, Reader reader);
    };

    using EntryPtr = std::shared_ptr<Entry>;
    EntryPtr getEntry(const String & patch_name, const MarkRanges & ranges, Reader reader);

private:
    EntryPtr getOrEmplaceEntry(const String & patch_name);

    size_t num_buckets;
    ThreadPool & thread_pool;

    mutable std::mutex mutex;
    absl::flat_hash_map<String, EntryPtr> cache;
    absl::flat_hash_map<String, std::map<MarkRange, size_t>> ranges_to_buckets;
};

using PatchJoinCachePtr = std::shared_ptr<PatchJoinCache>;

}
