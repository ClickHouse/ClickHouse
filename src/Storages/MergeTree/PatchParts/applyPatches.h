#pragma once
#include <future>
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Common/SharedMutex.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Core/Block.h>
#include <IO/SharedThreadPools.h>
#include <absl/container/flat_hash_map.h>

namespace DB
{

struct PatchToApply
{
    PaddedPODArray<UInt64> result_indices;
    PaddedPODArray<UInt64> patch_indices;
    Block patch_block;

    bool empty() const
    {
        chassert(result_indices.size() == patch_indices.size());
        return result_indices.empty();
    }

    size_t rows() const
    {
        chassert(result_indices.size() == patch_indices.size());
        return result_indices.size();
    }
};

using PatchToApplyPtr = std::shared_ptr<const PatchToApply>;
using PatchesToApply = std::vector<PatchToApplyPtr>;

struct PatchReadResult
{
    virtual ~PatchReadResult() = default;
};

using PatchReadResultPtr = std::shared_ptr<const PatchReadResult>;

struct PatchMergeReadResult : public PatchReadResult
{
    Block block;
    UInt64 min_part_offset = 0;
    UInt64 max_part_offset = 0;
};

/// We use two-level hash map (_block_number -> (_block_offset -> row_number)).
/// Block number are usually the same for the large ranges of consecutive rows.
/// Therefore we switch between hash maps for blocks rarely.
/// It makes two-level hash map more cache-friendly than single-level ((_block_number, _block_offset) -> row_number).
using OffsetsHashMap = absl::flat_hash_map<UInt64, UInt64, DefaultHash<UInt64>>;
using PatchHashMap = absl::flat_hash_map<UInt64, OffsetsHashMap, DefaultHash<UInt64>>;

struct PatchJoinCache
{
    using Reader = std::function<Block(const MarkRanges &)>;
    PatchJoinCache(size_t num_buckets_, ThreadPool & thread_pool_);

    struct Entry
    {
        Block block;
        PatchHashMap hash_map;
        std::map<MarkRange, std::shared_future<void>> ranges_futures;

        UInt64 min_block = std::numeric_limits<UInt64>::max();
        UInt64 max_block = 0;

        mutable SharedMutex mutex;

        void addBlock(Block read_block);
        std::vector<std::shared_future<void>> addRangesAsync(const MarkRanges & ranges, ThreadPool & pool, Reader reader);
    };

    using EntryPtr = std::shared_ptr<Entry>;
    using Entries = std::vector<EntryPtr>;

    void init(const RangesInPatchParts & ranges_in_pathces);
    Entries getEntries(const String & patch_name, const MarkRanges & ranges, Reader reader);

private:
    std::pair<Entries, std::vector<MarkRanges>> getEntriesAndRanges(const String & patch_name, const MarkRanges & ranges);

    size_t num_buckets;
    ThreadPool & thread_pool;

    mutable std::mutex mutex;
    absl::flat_hash_map<String, Entries> cache;
    absl::flat_hash_map<String, std::map<MarkRange, size_t>> ranges_to_buckets;
};

using PatchJoinCachePtr = std::shared_ptr<PatchJoinCache>;

struct PatchJoinReadResult : public PatchReadResult
{
    PatchJoinCache::Entries entries;
};

/// Applies patch. Returns indices in result and patch blocks for rows that should be updated.
PatchToApplyPtr applyPatchMerge(const Block & result_block, const Block & patch_block, const PatchPartInfoForReader & patch);
PatchToApplyPtr applyPatchJoin(const Block & result_block, const Block & patch_block, const PatchJoinCache::Entry & join_entry);

/// Updates rows in result_block from patch_block at specified indices.
/// versions_block is a shared block with current versions of rows for each updated column.
void applyPatchesToBlock(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    UInt64 source_data_version);

}
