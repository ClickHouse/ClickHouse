#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

namespace DB::Cas
{

/// Blob-target shard for a blob identity.
///
/// Deterministic and total over all hashes: the blob-target sharding axis uses the high 64 bits of
/// the 128-bit blob hash modulo `gc_shards`. The high bits are taken rather than the low bits so
/// that blobs whose hashes differ only in the low 64 bits — an adversarial corner case — still
/// spread across shards. `CityHash128` output has high entropy in BOTH halves, so both choices
/// are equivalent for organic workloads; high bits are the documented canonical choice.
///
/// Properties:
///   - Deterministic/stable: same inputs always yield the same shard.
///   - Total: result is in [0, gc_shards).
///   - Single-shard equivalence: gc_shards == 1 always returns 0.
///
/// The argument is the complete `BlobRef`, so callers cannot silently discard its algorithm while
/// routing. The shard number deliberately uses only `ref.digest`: distribution comes from the
/// digest bytes, while the algorithm remains part of the identity carried through the GC pipeline.
inline uint64_t blobShard(const BlobRef & ref, uint64_t gc_shards)
{
    /// gc_shards >= 1 is enforced by GcState decode (CORRUPTED_DATA on 0).
    /// BE-u64 of bytes[0:8] — an EXPLICIT big-endian read, bit-identical to the old
    /// `static_cast<uint64_t>(blob_hash >> 64)` for every 128-bit digest (`fromU128` writes the
    /// UInt128 big-endian into bytes[0:16], so bytes[0:8] IS the old high 64 bits). MUST stay an
    /// explicit big-endian read, never a native-endian memcpy (would silently reshard on an LE host).
    uint64_t high64 = 0;
    for (int i = 0; i < 8; ++i)
        high64 = (high64 << 8) | ref.digest.bytes[static_cast<size_t>(i)];
    return high64 % gc_shards;
}

/// Route a part-manifest cleanup bundle to a worker by its namespace-qualified `ManifestId`. Workers
/// own disjoint ranges; routing by `ManifestRef` alone would merge cleanup work from two namespaces
/// that happen to reuse the same reference components. `gc_shards == 1` routes every `ManifestId`
/// to owner shard 0.
///
/// The hash mixes both the `root_namespace` string and the three `ManifestRef` components — the
/// same mixing used by `std::hash<ManifestId>`. Two `ManifestId`s that share the same `ManifestRef`
/// but carry different namespaces produce independent hash values and may route to different shards.
///
/// Properties:
///   - Deterministic/stable: same inputs always yield the same shard.
///   - Total: result is in [0, gc_shards).
///   - Single-shard equivalence: gc_shards == 1 always returns 0.
uint64_t manifestCleanupShard(const ManifestId & id, uint64_t gc_shards);

/// Per-shard in-degree reducer for the sharded GC fold.
///
/// `ShardReducer` owns exactly ONE target shard (`shard` in [0, `gc_shards`)). It accepts the
/// caller's per-shard slice of `BlobDelta`s — produced by `foldManifestEdges` and bucketed by
/// `blobShard` — and merges them into a per-shard `CasBlobInDegree` generation run via
/// `foldDeltasIntoGeneration`.
///
/// Ownership invariant: a reducer touches ONLY blobs it owns — i.e. `blobShard(h, gc_shards) == shard`.
/// Two reducers for DIFFERENT shards may run concurrently; their key namespaces are disjoint
/// (`blobTargetRunKey(gen, shard0, seq)` vs `blobTargetRunKey(gen, shard1, seq)`).
///
/// The `reduce` method delegates to `foldDeltasIntoGeneration` (the same path the non-sharded fold
/// uses with `shard == 0`), so `gc_shards == 1` with `shard == 0` reproduces the non-sharded fold
/// byte-for-byte. This keeps the one-shard configuration compatible with the original fold path.
///
/// NOTE on durable writes: `reduce` writes the per-shard in-degree run directly via `backend`
/// (under `blobTargetRunKey(new_generation, shard, 0)`), exactly as `foldDeltasIntoGeneration`
/// does. Returning the durable write here (rather than an in-memory map) keeps the round driver
/// stateless: it simply constructs a `ShardReducer` per shard, calls `reduce`, and the sealed
/// run is already present for the `zeroInDegree` consumer (and the fold's two-cursor merge). An
/// in-memory return value is unnecessary because the backend is directly queryable; tests read the
/// sealed run back over an `InMemoryBackend`.
class ShardReducer
{
public:
    /// Construct a reducer that owns `shard` (in [0, `gc_shards`)).
    ShardReducer(uint64_t shard_, uint64_t gc_shards_);

    /// True iff this reducer owns `ref` — i.e. `blobShard(ref, gc_shards) == shard`.
    bool owns(const BlobRef & ref) const;

    /// Merge `shard_deltas` (the caller's per-shard `BlobDelta` slice produced by `foldManifestEdges`
    /// and bucketed by `blobShard`) into a new in-degree generation for this shard. Writes the sealed
    /// run under `blobTargetRunKey(new_generation, shard, 0)` via `backend`, appends its `RunRef` to
    /// `out_runs`, and returns the `RunRef`. The call is idempotent (write-once via `putIfAbsent`).
    ///
    /// `prior_runs` are the parent generation's run segments for this shard, resolved BY THE CALLER from
    /// the parent fold seal's `blob_target_runs` filtered to `shard`. An empty vector is the
    /// fresh-pool / empty baseline.
    ///
    /// PRECONDITION: every `BlobDelta` in `shard_deltas` must be owned by this reducer
    /// (`blobShard(d.ref, gc_shards) == shard`). This is a caller contract; there is no
    /// underflow throw backstopping it — pass a misbucketed delta and the fold silently misroutes it.
    std::vector<RunRef> reduce(Backend & backend, const Layout & layout,
                               const std::vector<RunRef> & prior_runs,
                               uint64_t new_generation, uint64_t attempt,
                               std::vector<BlobDelta> shard_deltas,
                               uint64_t current_round = 0, uint64_t condemn_round = 0,
                               const std::function<std::optional<HeadResult>(const BlobRef &)> & head_blob = {},
                               const std::function<std::optional<HeadResult>(const BlobRef &)> & peek_head = {},
                               const std::function<bool(const RetiredEntry &)> & confirm_condemned_marker = {},
                               RetiredMergeResult * out_retired = nullptr,
                               bool suppress_destructive = false,
                               /// PROBE B2: forwarded verbatim to `foldDeltasIntoGeneration` — see its
                               /// declaration and `Cas::TxnApplyLedger`.
                               std::vector<uint8_t> * out_applied_by_txn_ordinal = nullptr,
                               /// Forwarded verbatim to `foldDeltasIntoGeneration`; the caller shares
                               /// one instance across every shard's reduce within a round.
                               GcRoundWorkBudget * work_budget = nullptr) const;

private:
    uint64_t shard;
    uint64_t gc_shards;
};

/// Role split of a sharded GC round (`gc_shards > 1`):
///
///   - COORDINATOR (exactly one per round — the lease holder): owns input-seal, round-visibility,
///     the single GLOBAL fence (over all LIST-discovered shards), and generation-advance. These
///     steps span the whole fence universe and must NOT be sharded: a publish into one root shard
///     can protect a blob assigned to ANY target shard, so an independent per-reducer fence is
///     unsafe. `Gc::fence` therefore stays the single coordinator fence over the entire universe.
///
///   - REDUCERS / CLEANUP WORKERS (one per disjoint shard): own ONLY their shard's blob-target reduce
///     (`ShardReducer`) or part-manifest cleanup (`manifestCleanupShard`). Their key namespaces are
///     disjoint, so two replicas may reduce DIFFERENT shards concurrently. Reducer work needs NO lease:
///     the lease is work-dedup only (see `CasGcScheduler`), not a coordination primitive.

}
