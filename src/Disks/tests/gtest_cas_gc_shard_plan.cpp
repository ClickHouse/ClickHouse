#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcShardPlan.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include "cas_test_helpers.h"

using namespace DB::Cas;
using namespace DB::Cas::tests;

TEST(CASGCShardConfig, DefaultIsSingleShard)
{
    PoolConfig cfg;
    EXPECT_EQ(cfg.gc_shards, 1u);
    EXPECT_EQ(cfg.manifest_sweep_list_budget_keys, 1000u);
    EXPECT_EQ(cfg.manifest_sweep_delete_budget_keys, 100u);
}

TEST(CASGCShardConfig, GcStateRoundTripPreservesShardCount)
{
    GcState s;
    s.gc_shards = 4;
    s.round = 7;
    const GcState d = decodeGcState(encodeGcState(s));
    EXPECT_EQ(d.gc_shards, 4u);
    EXPECT_EQ(d.round, 7u);
}

/// ---- blobShard tests (Phase 4, Task 3) ----

TEST(CASGCShardScatter, DeterministicAndStable)
{
    /// A fixed hash — the same bytes every run. blobShard must return the same value twice,
    /// must be strictly less than gc_shards=4, and must be 0 when gc_shards=1.
    const UInt128 h = hexToU128("0102030405060708090a0b0c0d0e0f10");
    const BlobRef hd{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h)};

    const uint64_t s4a = blobShard(hd, 4);
    const uint64_t s4b = blobShard(hd, 4);

    EXPECT_EQ(s4a, s4b) << "blobShard must be deterministic";
    EXPECT_LT(s4a, 4u) << "blobShard result must be < gc_shards";
    EXPECT_EQ(blobShard(hd, 1), 0u) << "gc_shards==1 must route every hash to shard 0";
}

TEST(CASGCShardScatter, DisjointCoverageOverManyHashes)
{
    /// Over 4096 spread-out hashes with gc_shards=4: every result in [0,4) and every shard
    /// gets at least one hash (no dead shard).
    constexpr uint64_t kNumHashes = 4096;
    constexpr uint64_t kShards = 4;

    std::vector<bool> seen(kShards, false);
    for (uint64_t i = 0; i < kNumHashes; ++i)
    {
        /// Spread: use i in the high and low halves to avoid clustering.
        const UInt128 h = (static_cast<UInt128>(i * 0x9e3779b97f4a7c15ULL) << 64)
                        | static_cast<UInt128>(i * 0x6c62272e07bb0142ULL);
        const uint64_t s = blobShard(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h)}, kShards);
        ASSERT_LT(s, kShards) << "blobShard out of range at i=" << i;
        seen[s] = true;
    }

    for (uint64_t s = 0; s < kShards; ++s)
        EXPECT_TRUE(seen[s]) << "shard " << s << " received no hashes (dead shard)";
}

/// ---- ShardReducer tests (Phase 4, Task 4) ----

/// Build two blob hashes that route to DIFFERENT shards under gc_shards=2.
/// Returns {hash_for_shard0, hash_for_shard1}.
static std::pair<BlobRef, BlobRef> makeTwoShardHashes()
{
    /// Scan pairs (i, j): find hash_a -> shard 0, hash_b -> shard 1 under gc_shards=2.
    /// We construct candidates by setting the high 64 bits and leaving the low 64 bits zero
    /// so blobShard = high64 % 2.  i=0 => shard 0, i=1 => shard 1.
    const UInt128 h0 = static_cast<UInt128>(0ULL) << 64;   /// high64=0 => shard 0
    const UInt128 h1 = static_cast<UInt128>(1ULL) << 64;   /// high64=1 => shard 1
    return {BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h0)},
            BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h1)}};
}

/// `ShardReducer::reduce` merges deltas into the correct per-shard in-degree run.
///
/// Scenario: scatter (+1 b1, +1 b1, -1 b1, +1 b2) across two shards.
///   - b1 routes to shard 0; net = +2 - 1 = 1; in-degree after reduce = 1.
///   - b2 routes to shard 1; net = +1; in-degree after reduce = 1.
///   - Each reducer touches ONLY its own shard's key space.
TEST(CASGCShardReducer, MergesDeltasToInDegree)
{
    const auto [b1, b2] = makeTwoShardHashes();
    ASSERT_EQ(blobShard(b1, 2), 0u) << "b1 must route to shard 0";
    ASSERT_EQ(blobShard(b2, 2), 1u) << "b2 must route to shard 1";

    /// Construct source-edge deltas directly (the production fold produces these via
    /// `foldManifestEdges`, bucketed by `blobShard`):
    /// b1 shard=0: source 1 activates, source 2 activates, source 1 removes => 1 active edge
    /// b2 shard=1: source 3 activates => 1 active edge
    std::vector<std::vector<BlobDelta>> buckets(2);
    buckets[0] = {
        BlobDelta{.ref = b1, .source_id = UInt128(1), .remove = false},
        BlobDelta{.ref = b1, .source_id = UInt128(2), .remove = false},
        BlobDelta{.ref = b1, .source_id = UInt128(1), .remove = true},
    };
    buckets[1] = {
        BlobDelta{.ref = b2, .source_id = UInt128(3), .remove = false},
    };

    /// Verify bucket net effects (source 2 survives for b1; source 3 survives for b2).
    ASSERT_EQ(buckets.size(), 2u);
    {
        int64_t net_b1 = 0;
        for (const auto & d : buckets[0])
            if (d.ref == b1)
                net_b1 += d.remove ? -1 : +1;
        EXPECT_EQ(net_b1, 1) << "shard-0 bucket net delta for b1 must be +1";
    }
    {
        int64_t net_b2 = 0;
        for (const auto & d : buckets[1])
            if (d.ref == b2)
                net_b2 += d.remove ? -1 : +1;
        EXPECT_EQ(net_b2, 1) << "shard-1 bucket net delta for b2 must be +1";
    }

    /// Reduce: each reducer merges its shard's deltas into generation 1 (prior = 0 = fresh).
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");

    ShardReducer r0(0, 2);
    ShardReducer r1(1, 2);

    EXPECT_TRUE(r0.owns(b1)) << "r0 must own b1";
    EXPECT_FALSE(r0.owns(b2)) << "r0 must not own b2";
    EXPECT_TRUE(r1.owns(b2)) << "r1 must own b2";
    EXPECT_FALSE(r1.owns(b1)) << "r1 must not own b1";

    const auto runs0 = r0.reduce(*backend, layout, /*prior_runs=*/{}, /*new_generation=*/1, /*attempt=*/0,
                                 std::move(buckets[0]));
    const auto runs1 = r1.reduce(*backend, layout, /*prior_runs=*/{}, /*new_generation=*/1, /*attempt=*/0,
                                 std::move(buckets[1]));

    ASSERT_EQ(runs0.size(), 1u) << "shard-0 reduce must produce exactly one RunRef";
    ASSERT_EQ(runs1.size(), 1u) << "shard-1 reduce must produce exactly one RunRef";

    /// The keys must be distinct (disjoint shard namespaces).
    EXPECT_NE(runs0[0].key, runs1[0].key) << "shard-0 and shard-1 run keys must be distinct";

    /// Read back in-degree from the sealed runs (resolved via each reduce's returned refs).
    const int64_t indeg_b1 = inDegreeInRuns(*backend, runs0, b1);
    const int64_t indeg_b2 = inDegreeInRuns(*backend, runs1, b2);
    EXPECT_EQ(indeg_b1, 1) << "b1 in-degree after reduce must be 1";
    EXPECT_EQ(indeg_b2, 1) << "b2 in-degree after reduce must be 1";

    /// Cross-shard reads: shard-0's run must not contain b2; shard-1's run must not contain b1.
    EXPECT_EQ(inDegreeInRuns(*backend, runs0, b2), 0)
        << "shard-0 run must not mention b2";
    EXPECT_EQ(inDegreeInRuns(*backend, runs1, b1), 0)
        << "shard-1 run must not mention b1";
}

/// `ShardReducer::owns` partitions the blob hash space: for any hash, exactly ONE reducer among
/// {r0, r1} owns it (union == all, intersection == empty).
TEST(CASGCShardReducer, TwoReducersCoverDisjointShards)
{
    constexpr uint64_t kNumHashes = 4096;
    constexpr uint64_t kGcShards = 2;

    ShardReducer r0(0, kGcShards);
    ShardReducer r1(1, kGcShards);

    for (uint64_t i = 0; i < kNumHashes; ++i)
    {
        const UInt128 h = (static_cast<UInt128>(i * 0x9e3779b97f4a7c15ULL) << 64)
                        | static_cast<UInt128>(i * 0x6c62272e07bb0142ULL);
        const BlobRef href{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h)};
        const bool o0 = r0.owns(href);
        const bool o1 = r1.owns(href);

        /// Exactly one of the two reducers must own every hash.
        ASSERT_TRUE(o0 || o1)
            << "hash " << i << " is owned by neither shard (gap in coverage)";
        ASSERT_FALSE(o0 && o1)
            << "hash " << i << " is owned by BOTH shards (overlap in coverage)";
    }
}

/// ---- manifestCleanupShard tests (Phase 4, Task 5) ----

/// Two `ManifestId`s with the SAME `ManifestRef` but DIFFERENT namespaces must be unequal (proving
/// qualified identity), and `manifestCleanupShard` must depend on the namespace — not just the ref.
///
/// Phase 0 `SabotageKeyByRefNotId`: if routing used only the `ManifestRef`, two namespaces sharing
/// the same ref would land on the same worker, merging cleanup work that belongs to distinct objects.
TEST(CASGCShardCleanup, RoutesByQualifiedManifestIdNotRef)
{
    /// Shared ManifestRef: identical across both ManifestIds.
    const ManifestRef shared_ref{
        .writer_epoch = 1,
        .build_sequence = 7,
        .manifest_ordinal = 1,
    };

    const ManifestId id_a{RootNamespace("ns_alpha"), shared_ref};
    const ManifestId id_b{RootNamespace("ns_beta"), shared_ref};

    /// The two ids are unequal (different namespace => different qualified identity).
    EXPECT_NE(id_a, id_b) << "ManifestIds with different namespaces must be unequal";

    /// Both results must be in range.
    constexpr uint64_t kShards = 4;
    const uint64_t shard_a = manifestCleanupShard(id_a, kShards);
    const uint64_t shard_b = manifestCleanupShard(id_b, kShards);
    EXPECT_LT(shard_a, kShards) << "shard for id_a must be < gc_shards";
    EXPECT_LT(shard_b, kShards) << "shard for id_b must be < gc_shards";

    /// Deterministic: same id always routes to the same shard.
    EXPECT_EQ(manifestCleanupShard(id_a, kShards), shard_a) << "manifestCleanupShard must be deterministic";
    EXPECT_EQ(manifestCleanupShard(id_b, kShards), shard_b) << "manifestCleanupShard must be deterministic";

    /// Single-shard equivalence: gc_shards==1 routes everything to shard 0.
    EXPECT_EQ(manifestCleanupShard(id_a, 1), 0u) << "gc_shards==1 must route to shard 0";
    EXPECT_EQ(manifestCleanupShard(id_b, 1), 0u) << "gc_shards==1 must route to shard 0";

    /// KEY ASSERTION: routing depends on the namespace, not the ref alone.
    /// Scan namespace-pair candidates (varying only the namespace string) until we find two that
    /// route to different shards under gc_shards=8. This directly demonstrates that
    /// `manifestCleanupShard` is NOT a function of `ManifestRef` alone.
    bool found_namespace_split = false;
    for (uint64_t i = 0; i < 256 && !found_namespace_split; ++i)
    {
        const ManifestId probe_a{RootNamespace("namespace_probe_" + std::to_string(i)), shared_ref};
        for (uint64_t j = i + 1; j < 256 && !found_namespace_split; ++j)
        {
            const ManifestId probe_b{RootNamespace("namespace_probe_" + std::to_string(j)), shared_ref};
            if (manifestCleanupShard(probe_a, 8) != manifestCleanupShard(probe_b, 8))
                found_namespace_split = true;
        }
    }
    EXPECT_TRUE(found_namespace_split)
        << "could not find two namespace variants of the same ManifestRef that route to different "
           "shards — routing is not namespace-sensitive (SabotageKeyByRefNotId hazard)";
}

/// Over many `ManifestId`s with `gc_shards=4`: every owner shard is covered, and each id lands in
/// exactly one shard (total, disjoint coverage).
TEST(CASGCShardCleanup, DisjointWorkerCoverage)
{
    constexpr uint64_t kNumIds = 4096;
    constexpr uint64_t kShards = 4;

    std::vector<bool> seen(kShards, false);
    for (uint64_t i = 0; i < kNumIds; ++i)
    {
        /// Vary both namespace and ManifestRef fields to spread the distribution.
        const ManifestId id{
            RootNamespace("ns_" + std::to_string(i % 16)),
            ManifestRef{
                .writer_epoch = 1 + i / 16,
                .build_sequence = i,
                .manifest_ordinal = static_cast<uint32_t>(i % kMaxManifestOrdinal + 1),
            },
        };

        const uint64_t s = manifestCleanupShard(id, kShards);
        ASSERT_LT(s, kShards) << "manifestCleanupShard out of range at i=" << i;
        seen[s] = true;
    }

    for (uint64_t s = 0; s < kShards; ++s)
        EXPECT_TRUE(seen[s]) << "owner shard " << s << " received no ManifestIds (dead shard)";
}

/// The sharded fold (gc_shards > 1) partitions a flat `BlobDelta` stream by `blobShard` and folds
/// each bucket via its own `ShardReducer`, exactly as `Gc::fold` does. This test replicates that
/// partition-and-reduce step over `gc_shards = 2` and asserts each blob's in-degree lands in its
/// owning shard's run and nowhere else. (The full two-replica round is covered by Task 8.)
TEST(CASGCShardCoordinator, ShardedFoldRoutesDeltasToOwningShards)
{
    constexpr uint64_t kGcShards = 2;
    const auto [b0, b1] = makeTwoShardHashes();
    ASSERT_EQ(blobShard(b0, kGcShards), 0u);
    ASSERT_EQ(blobShard(b1, kGcShards), 1u);

    /// A flat delta stream as produced by `foldManifestEdges`: b0 net +1 (two +1, one -1), b1 net +1.
    std::vector<BlobDelta> deltas{
        BlobDelta{.ref = b0, .source_id = UInt128(1), .remove = false},
        BlobDelta{.ref = b1, .source_id = UInt128(2), .remove = false},
        BlobDelta{.ref = b0, .source_id = UInt128(3), .remove = false},
        BlobDelta{.ref = b0, .source_id = UInt128(1), .remove = true},
    };

    /// Partition by blobShard — the exact step the sharded fold runs before reducing.
    std::vector<std::vector<BlobDelta>> buckets(kGcShards);
    for (BlobDelta & d : deltas)
        buckets[blobShard(d.ref, kGcShards)].push_back(d);

    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");

    std::vector<std::vector<RunRef>> shard_runs(kGcShards);
    for (uint64_t shard = 0; shard < kGcShards; ++shard)
    {
        ShardReducer reducer{shard, kGcShards};
        shard_runs[shard] = reducer.reduce(*backend, layout, /*prior_runs=*/{}, /*new_generation=*/1, /*attempt=*/0,
                                           std::move(buckets[shard]));
    }

    EXPECT_EQ(inDegreeInRuns(*backend, shard_runs[0], b0), 1)
        << "b0 must fold into shard-0 with in-degree 1";
    EXPECT_EQ(inDegreeInRuns(*backend, shard_runs[1], b1), 1)
        << "b1 must fold into shard-1 with in-degree 1";
    EXPECT_EQ(inDegreeInRuns(*backend, shard_runs[1], b0), 0)
        << "b0 must NOT appear in shard-1's run";
    EXPECT_EQ(inDegreeInRuns(*backend, shard_runs[0], b1), 0)
        << "b1 must NOT appear in shard-0's run";
}

/// ---- Phase 4, Task 7: single-shard equivalence ----
///
/// Prove that the sharded partition+reduce path (gc_shards=2, all blobs routing to shard 0) produces
/// the SAME per-blob in-degrees as the single-shard (gc_shards=1, Phase 1d) fold over an IDENTICAL
/// journal. This is approach (a) from the spec: choose blob hashes whose high64 % 2 == 0 so shard 1's
/// bucket is always empty; the sharded path's shard-0 reducer and the single-shard path both call
/// `foldDeltasIntoGeneration` with the same delta stream (one routing into shard 0 of 2, the other
/// into shard 0 of 1).
///
/// NOTE ON SEAL-BYTE EQUALITY: byte-for-byte equality of the `CasFoldSeal` is NOT asserted here. The
/// fold seal records the `blobTargetRunKey(gen, shard, seq)` path, which embeds the shard number. The
/// single-shard path writes `blobTargetRunKey(g, 0, 0)` for gc_shards=1, while the sharded path writes
/// `blobTargetRunKey(g, 0, 0)` for the shard-0 run AND `blobTargetRunKey(g, 1, 0)` for the (empty)
/// shard-1 run. The per-blob in-degree (the load-bearing property — it drives the spare/delete
/// decision) is identical; the seal's key-set legitimately differs by shard count.
TEST(CASGCShardEquivalence, SingleShardMatchesPhase1dInDegree)
{
    /// Build three blob hashes that ALL route to shard 0 under gc_shards=2 (high64 % 2 == 0).
    /// high64=0 => shard 0, high64=2 => shard 0, high64=4 => shard 0.
    const UInt128 hA = static_cast<UInt128>(0ULL) << 64;   /// high64=0, routes to shard 0
    const UInt128 hB = static_cast<UInt128>(2ULL) << 64;   /// high64=2, routes to shard 0
    const UInt128 hC = static_cast<UInt128>(4ULL) << 64;   /// high64=4, routes to shard 0

    const BlobRef refA{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hA)};
    const BlobRef refB{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hB)};
    const BlobRef refC{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hC)};
    ASSERT_EQ(blobShard(refA, 2), 0u) << "hA must route to shard 0 under gc_shards=2";
    ASSERT_EQ(blobShard(refB, 2), 0u) << "hB must route to shard 0 under gc_shards=2";
    ASSERT_EQ(blobShard(refC, 2), 0u) << "hC must route to shard 0 under gc_shards=2";
    ASSERT_EQ(blobShard(refA, 1), 0u) << "hA must route to shard 0 under gc_shards=1";
    ASSERT_EQ(blobShard(refB, 1), 0u) << "hB must route to shard 0 under gc_shards=1";
    ASSERT_EQ(blobShard(refC, 1), 0u) << "hC must route to shard 0 under gc_shards=1";

    /// Construct the journal: hA gets net +2 (published twice), hB gets net +1, hC gets net 0 (publish
    /// then drop => transitions to zero). This exercises all three outcomes (>1, =1, =0) for the
    /// equivalence proof.
    ///
    /// Note: net +2 is unrealistic for production (two DISTINCT manifests can share a blob, each
    /// contributing +1 independently) but is valid for the fold math test. It directly verifies that
    /// accumulators sum correctly under both paths.
    const RootNamespace ns{"ns-equiv"};
    const ManifestRef rA1{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = static_cast<uint32_t>(0x1)};
    const ManifestRef rA2{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = static_cast<uint32_t>(0x2)};
    const ManifestRef rB{.writer_epoch = 1, .build_sequence = 3, .manifest_ordinal = static_cast<uint32_t>(0x3)};
    const ManifestRef rC{.writer_epoch = 1, .build_sequence = 4, .manifest_ordinal = static_cast<uint32_t>(0x4)};

    /// Helper lambda that sets up a fresh backend + store with the shared scripted journal, runs one GC
    /// round with the given gc_shards, and returns the per-blob in-degrees in the sealed generation.
    /// Returns {indeg_A, indeg_B, indeg_C}.
    auto runJournalAndGetInDegrees = [&](uint64_t gc_shards) -> std::tuple<int64_t, int64_t, int64_t>
    {
        auto backend = std::make_shared<InMemoryBackend>();
        const Layout layout("p");
        /// Raw journal fixtures model an already-created pool and therefore establish both mandatory
        /// controls before writing residual data.
        seedPoolMetaForRestart(*backend);

        /// Write blob bodies so HEAD returns a token (GC retires zero-in-degree blobs only if present).
        writeBlobBody(*backend, layout, hA);
        writeBlobBody(*backend, layout, hB);
        writeBlobBody(*backend, layout, hC);

        /// Write manifests: rA1 references hA once; rA2 also references hA once; rB references hB;
        /// rC references hC. Each publication contributes +1 per referenced blob.
        writeManifestRaw(*backend, layout, ns, rA1, {blobEntryFor("a", hA)});
        writeManifestRaw(*backend, layout, ns, rA2, {blobEntryFor("a", hA)});
        writeManifestRaw(*backend, layout, ns, rB,  {blobEntryFor("b", hB)});
        writeManifestRaw(*backend, layout, ns, rC,  {blobEntryFor("c", hC)});

        /// Publish all four refs (tbl1=rA1, tbl2=rA2, tbl3=rB, tbl4=rC).
        publishCommittedTransition(*backend, layout, ns, "tbl1", std::nullopt, rA1);
        publishCommittedTransition(*backend, layout, ns, "tbl2", std::nullopt, rA2);
        publishCommittedTransition(*backend, layout, ns, "tbl3", std::nullopt, rB);
        publishCommittedTransition(*backend, layout, ns, "tbl4", std::nullopt, rC);
        /// Drop tbl4 (hC net = 0): rC removed from the live set.
        dropRefTransition(*backend, layout, ns, "tbl4", rC);

        /// Open a store with the given `gc_shards` over the pre-seeded restart state.
        auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_shards = gc_shards});
        const UInt128 gc_id = UInt128(0xDEADBEEF42ULL);
        Gc gc(store, gc_id);
        EXPECT_TRUE(gc.runRegularRound().acquired_lease);

        /// The fold seal for new_generation (== snap_generation after fold) holds the in-degree runs.
        /// After runRegularRound the snap_generation points at the COMPLETION generation; the fold
        /// generation is snap_generation - 1 for the first full round. Use inDegreeOf (which reads
        /// currentGenerationOf = completion generation) for the final in-degrees.
        const std::vector<RunRef> shard0 = runsForShard(*backend, layout, /*shard=*/0);
        const int64_t iA = inDegreeInRuns(*backend, shard0, refA);
        const int64_t iB = inDegreeInRuns(*backend, shard0, refB);
        const int64_t iC = inDegreeInRuns(*backend, shard0, refC);
        return {iA, iB, iC};
    };

    const auto [a1, b1_indeg, c1] = runJournalAndGetInDegrees(/*gc_shards=*/1);
    const auto [a2, b2_indeg, c2] = runJournalAndGetInDegrees(/*gc_shards=*/2);

    /// The in-degree values must match exactly between the two runs.
    EXPECT_EQ(a1, a2)
        << "hA in-degree must match: gc_shards=1 gives " << a1 << ", gc_shards=2 gives " << a2;
    EXPECT_EQ(b1_indeg, b2_indeg)
        << "hB in-degree must match: gc_shards=1 gives " << b1_indeg << ", gc_shards=2 gives " << b2_indeg;
    EXPECT_EQ(c1, c2)
        << "hC in-degree must match: gc_shards=1 gives " << c1 << ", gc_shards=2 gives " << c2;

    /// Cross-check the known correct values (derivable from the scripted journal).
    /// hA: +1 (tbl1/rA1) + 1 (tbl2/rA2) = 2.
    EXPECT_EQ(a1, 2) << "hA in-degree must be 2 (two distinct live refs both citing hA)";
    /// hB: +1 (tbl3/rB) = 1.
    EXPECT_EQ(b1_indeg, 1) << "hB in-degree must be 1";
    /// hC: +1 (tbl4/rC publish) - 1 (tbl4 drop) = 0.
    EXPECT_EQ(c1, 0) << "hC in-degree must be 0 (publish then drop; net zero)";
}

/// ---- Phase 4, Task 8: two-replica disjoint-shard concurrency ----
///
/// With gc_shards=2 over a shared `InMemoryBackend`:
///   (a) DISJOINTNESS: a shard-0 reducer's product covers only hashes routing to shard 0; shard-1
///       covers only hashes routing to shard 1 (`owns` check).
///   (b) PER-SHARD RUNS: each reducer writes its own write-once blob-target run; the runs for the two
///       shards are disjoint object keys and durably present after each `ShardReducer::reduce`.
///   (c) MERGED IN-DEGREE: the merged in-degrees across both shards equal the expected edge multiset,
///       and each blob is absent from the other shard's run (cross-shard disjointness).
///
/// Interleaving: driven entirely from the test thread (no threads, no sleeps). The two reducers are
/// constructed and called sequentially from the test thread. This proves the protocol is correct even
/// when reducer work interleaves arbitrarily — the key-space disjointness is static.
TEST(CASGCShardTwoReplica, DisjointShardsConcurrentPerShardRuns)
{
    constexpr uint64_t kGcShards = 2;
    constexpr uint64_t kNewGen = 1;
    constexpr uint64_t kAttempt = 0;

    /// b0 routes to shard 0, b1 routes to shard 1 (from makeTwoShardHashes).
    const auto [b0, b1] = makeTwoShardHashes();
    ASSERT_EQ(blobShard(b0, kGcShards), 0u) << "b0 must route to shard 0";
    ASSERT_EQ(blobShard(b1, kGcShards), 1u) << "b1 must route to shard 1";

    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");

    /// (a) DISJOINTNESS — verify `owns` predicate before any reduce.
    ShardReducer r0(0, kGcShards);
    ShardReducer r1(1, kGcShards);

    EXPECT_TRUE(r0.owns(b0))  << "shard-0 reducer must own b0";
    EXPECT_FALSE(r0.owns(b1)) << "shard-0 reducer must NOT own b1";
    EXPECT_TRUE(r1.owns(b1))  << "shard-1 reducer must own b1";
    EXPECT_FALSE(r1.owns(b1) && r0.owns(b1)) << "no hash may be owned by both reducers";

    /// Construct disjoint delta streams: b0 gets net +2 in shard 0; b1 gets net +1 in shard 1.
    /// In production these buckets are produced by `foldManifestEdges` and partitioned by `blobShard`
    /// (two distinct manifests both referencing b0 contribute two source edges; one manifest
    /// referencing b1 contributes one source edge).
    std::vector<BlobDelta> bucket0 = {
        BlobDelta{.ref = b0, .source_id = UInt128(1), .remove = false},
        BlobDelta{.ref = b0, .source_id = UInt128(2), .remove = false},
    };
    std::vector<BlobDelta> bucket1 = {
        BlobDelta{.ref = b1, .source_id = UInt128(3), .remove = false},
    };

    /// (b) PER-SHARD RUNS — drive both reducers.
    ///
    /// Run shard-0 reducer (simulates the shard-0 replica's work).
    const auto runs0 = r0.reduce(*backend, layout, /*prior_runs=*/{}, kNewGen, kAttempt, std::move(bucket0));
    ASSERT_FALSE(runs0.empty()) << "shard-0 reducer must produce at least one RunRef";

    /// Run shard-1 reducer (simulates the shard-1 replica's work, interleaved from the test thread).
    const auto runs1 = r1.reduce(*backend, layout, /*prior_runs=*/{}, kNewGen, kAttempt, std::move(bucket1));
    ASSERT_FALSE(runs1.empty()) << "shard-1 reducer must produce at least one RunRef";

    /// The blob-target runs for both shards are durably present (the reducer's write-once `putIfAbsent`),
    /// at disjoint object keys.
    EXPECT_TRUE(backend->head(layout.blobTargetRunKey(kNewGen, kAttempt, /*shard=*/0, /*seq=*/0)).exists)
        << "shard-0 blob-target run must be durably written by r0.reduce";
    EXPECT_TRUE(backend->head(layout.blobTargetRunKey(kNewGen, kAttempt, /*shard=*/1, /*seq=*/0)).exists)
        << "shard-1 blob-target run must be durably written by r1.reduce";

    /// (c) MERGED IN-DEGREE — the merged in-degrees across both shards equal the expected edge multiset.
    EXPECT_EQ(inDegreeInRuns(*backend, runs0, b0), 2)
        << "b0 in-degree must be 2 in shard-0 run";
    EXPECT_EQ(inDegreeInRuns(*backend, runs1, b1), 1)
        << "b1 in-degree must be 1 in shard-1 run";
    /// Cross-shard: each blob must be absent from the other shard's run.
    EXPECT_EQ(inDegreeInRuns(*backend, runs0, b1), 0)
        << "b1 must NOT appear in shard-0's run (cross-shard disjointness)";
    EXPECT_EQ(inDegreeInRuns(*backend, runs1, b0), 0)
        << "b0 must NOT appear in shard-1's run (cross-shard disjointness)";
}

/// ---- Phase 4 regression: gc_shards>1 retire-drain (High #1) ----
///
/// A FULL round-protocol regression that drives publish -> drop -> reclaim end-to-end under
/// `gc_shards = 2` with a droppable blob owned by a NON-zero shard. The fold/`ShardReducer` write one
/// in-degree run PER shard, so a zero-in-degree blob owned by shard 1..N is only ever retired (and
/// then exact-token deleted) if `retire`/`previewDeletes` scan EVERY blob-target shard. Before
/// `5f5fa5f7906` both hardcoded shard 0: a shard-1 candidate was never scanned, never retired, and
/// leaked forever. After the fix both shards are drained.
///
/// The test plants TWO droppable blobs in the SAME round — one owned by shard 0, one owned by shard 1
/// (verified via `blobShard(hash, 2)`) — and asserts BOTH are reclaimed. The shard-0 blob proves the
/// round works at all; the shard-1 blob is the regression's teeth (it would leak pre-fix while shard-0
/// still drained, so a single-blob test could pass even with the bug).
///
/// HOW IT WOULD LEAK PRE-FIX: under the old shard-0-only `retire`, the round folds the drop (shard-1
/// blob's in-degree -> 0 in shard 1's run) but `retire` only reads shard 0's in-degree run and only
/// writes shard 0's retired set, so the shard-1 zero-in-degree blob is never proposed for retirement.
/// `previewDeletes` (also shard-0-only pre-fix) never lists it, the recheck never spares-or-deletes it,
/// and `blobExists(b1)` stays true at fixpoint. The shard-0 blob would still be reclaimed — which is
/// exactly why the existing in-degree-equivalence tests (all blobs route to shard 0) did not catch it.
TEST(CASGCShardRetireDrain, ReclaimsDroppableBlobOwnedByNonZeroShard)
{
    constexpr uint64_t kGcShards = 2;

    /// Two blob hashes routing to DIFFERENT shards under gc_shards=2. blobShard = high64 % 2.
    /// high64=0 => shard 0; high64=1 => shard 1.
    const UInt128 blob_shard0 = (static_cast<UInt128>(0ULL) << 64) | static_cast<UInt128>(7ULL); /// high64=0 => shard 0
    const UInt128 blob_shard1 = (static_cast<UInt128>(1ULL) << 64) | static_cast<UInt128>(7ULL); /// high64=1 => shard 1
    ASSERT_EQ(blobShard(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(blob_shard0)}, kGcShards), 0u) << "blob_shard0 must route to shard 0";
    ASSERT_EQ(blobShard(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(blob_shard1)}, kGcShards), 1u) << "blob_shard1 must route to shard 1 (regression teeth)";

    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                 .gc_shards = kGcShards});
    const Layout & layout = store->layout();

    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r0{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = static_cast<uint32_t>(0xA0)};
    const ManifestRef r1{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = static_cast<uint32_t>(0xA1)};
    const ManifestId id0{ns, r0};
    const ManifestId id1{ns, r1};

    /// Local blobExists (the round-level helper is file-local to gtest_cas_gc_round.cpp).
    auto blobExists = [&](const UInt128 & hash)
    {
        return backend->head(layout.blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hash)})).exists;
    };
    auto manifestExists = [&](const ManifestId & id)
    {
        return backend->head(layout.manifestKey(id)).exists;
    };
    /// Whether ANY gc-shard still holds an in-flight condemned entry (the ack-floor deletion pipeline is
    /// in flight while this is true). Retired-in-snapshot (T4): reconstructed from the adopted fold seal's
    /// kCondemned rows across all shards, not a separate retired list.
    auto anyRetiredPending = [&]
    {
        return anyCondemnedInSeal(*backend, layout);
    };
    /// Drive to a fixpoint over the ACK-FLOOR round: advance the store's mount ack each round (so the floor
    /// follows the committed round) and stay alive while any work counter is nonzero OR an in-flight
    /// retired entry remains in ANY shard.
    auto driveToFixpoint = [&](Gc & gc)
    {
        for (size_t r = 0; r < 64; ++r)
        {
            const RoundReport rep = runRegularRoundReclaiming(gc);
            if (!rep.acquired_lease)
                continue;
            store->renewWatermarkOnce();
            const bool no_work = rep.candidates == 0 && rep.deleted == 0 && rep.absent == 0
                && rep.replaced == 0 && rep.spared == 0;
            if (no_work && !anyRetiredPending())
                break;
        }
    };

    /// Publish: ref r0 names the shard-0 blob, ref r1 names the shard-1 blob (distinct refs => distinct
    /// edges, each contributing +1 to its blob's in-degree in its OWNING shard's run).
    writeBlobBody(*backend, layout, blob_shard0);
    writeBlobBody(*backend, layout, blob_shard1);
    writeManifestRaw(*backend, layout, ns, r0, {blobEntryFor("a", blob_shard0)});
    writeManifestRaw(*backend, layout, ns, r1, {blobEntryFor("b", blob_shard1)});
    publishCommittedTransition(*backend, layout, ns, "tbl0", std::nullopt, r0);
    publishCommittedTransition(*backend, layout, ns, "tbl1", std::nullopt, r1);

    const UInt128 gc_id = UInt128(0xDEADBEEF42ULL);
    Gc gc(store, gc_id);
    driveToFixpoint(gc);

    /// While both refs are live: each blob's in-degree is 1 in its OWNING shard's run, and nothing is
    /// collected (no-loss). Derive generation/attempt from gc/state — never hardcode.
    const GcState live = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    ASSERT_GT(live.snap_generation, 0u);
    ASSERT_EQ(live.gc_shards, kGcShards) << "the pool must be running with gc_shards=2";
    EXPECT_EQ(inDegreeInRuns(*backend, runsForShard(*backend, layout, /*shard=*/0), BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(blob_shard0)}), 1)
        << "shard-0 blob in-degree must be 1 while live";
    EXPECT_EQ(inDegreeInRuns(*backend, runsForShard(*backend, layout, /*shard=*/1), BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(blob_shard1)}), 1)
        << "shard-1 blob in-degree must be 1 while live";
    EXPECT_TRUE(blobExists(blob_shard0));
    EXPECT_TRUE(blobExists(blob_shard1));

    /// Drop BOTH refs: each blob's only edge goes away (in-degree -> 0 in its owning shard's run).
    dropRefTransition(*backend, layout, ns, "tbl0", r0);
    dropRefTransition(*backend, layout, ns, "tbl1", r1);
    driveToFixpoint(gc);

    /// After drop + fixpoint: BOTH blobs are retired and exact-token deleted, and BOTH owner-removed
    /// manifest bodies are collected. The shard-1 blob is the regression's teeth — pre-`5f5fa5f` it
    /// would still exist here because retire/previewDeletes never scanned shard 1.
    EXPECT_EQ(inDegreeInRuns(*backend, runsForShard(*backend, layout, /*shard=*/0), BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(blob_shard0)}), 0)
        << "shard-0 blob in-degree must be 0 after drop";
    EXPECT_EQ(inDegreeInRuns(*backend, runsForShard(*backend, layout, /*shard=*/1), BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(blob_shard1)}), 0)
        << "shard-1 blob in-degree must be 0 after drop";
    EXPECT_FALSE(blobExists(blob_shard0)) << "shard-0 droppable blob must be reclaimed";
    EXPECT_FALSE(blobExists(blob_shard1))
        << "shard-1 droppable blob must be reclaimed (High #1: retire must scan ALL shards, not just shard 0)";
    EXPECT_FALSE(manifestExists(id0)) << "shard-0 owner-removed manifest body must be reclaimed";
    EXPECT_FALSE(manifestExists(id1)) << "shard-1 owner-removed manifest body must be reclaimed";

    /// Idempotent: another fixpoint changes nothing and never throws.
    EXPECT_NO_THROW(driveToFixpoint(gc));
    EXPECT_FALSE(blobExists(blob_shard0));
    EXPECT_FALSE(blobExists(blob_shard1));
}
