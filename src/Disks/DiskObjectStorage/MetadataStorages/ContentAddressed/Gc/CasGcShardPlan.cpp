#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcShardPlan.h>
#include <Common/Exception.h>
#include <base/defines.h>
#include <functional>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace DB::Cas
{

uint64_t manifestCleanupShard(const ManifestId & id, uint64_t gc_shards)
{
    /// gc_shards >= 1 is enforced by GcState decode (CORRUPTED_DATA on 0).
    /// Hash the qualified id (namespace plus all three `ManifestRef` components) using the same
    /// mixing as `std::hash<ManifestId>`. Two namespaces can legally carry the same
    /// `ManifestRef` without addressing the same object, so their cleanup work must never be
    /// merged.
    const size_t h = std::hash<ManifestId>{}(id);
    return static_cast<uint64_t>(h) % gc_shards;
}

ShardReducer::ShardReducer(uint64_t shard_, uint64_t gc_shards_)
    : shard(shard_), gc_shards(gc_shards_)
{
    if (gc_shards_ == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ShardReducer: gc_shards must be >= 1");
    if (shard_ >= gc_shards_)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ShardReducer: shard {} is out of range [0, {})", shard_, gc_shards_);
}

bool ShardReducer::owns(const BlobRef & ref) const
{
    return blobShard(ref, gc_shards) == shard;
}

std::vector<RunRef> ShardReducer::reduce(Backend & backend, const Layout & layout,
                                         const std::vector<RunRef> & prior_runs,
                                         uint64_t new_generation, uint64_t attempt,
                                         std::vector<BlobDelta> shard_deltas,
                                         uint64_t current_round, uint64_t condemn_round,
                                         const std::function<std::optional<HeadResult>(const BlobRef &)> & head_blob,
                                         const std::function<std::optional<HeadResult>(const BlobRef &)> & peek_head,
                                         const std::function<bool(const RetiredEntry &)> & confirm_condemned_marker,
                                         RetiredMergeResult * out_retired,
                                         bool suppress_destructive,
                                         std::vector<uint8_t> * out_applied_by_txn_ordinal,
                                         GcRoundWorkBudget * work_budget) const
{
    std::vector<RunRef> out_runs;
    foldDeltasIntoGeneration(backend, layout, prior_runs, new_generation, attempt, shard,
                             std::move(shard_deltas), out_runs,
                             current_round, condemn_round, head_blob, peek_head,
                             confirm_condemned_marker, out_retired,
                             suppress_destructive, out_applied_by_txn_ordinal, {}, work_budget);
    return out_runs;
}

}
