#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

constexpr uint64_t kCandidateEpoch = 1;
constexpr uint64_t kCandidateBuild = 5;
const UInt128 kGcId = hexToU128("000000000000000000000000000000d8");

ManifestRef candidateRef()
{
    return ManifestRef{.writer_epoch = kCandidateEpoch, .build_sequence = kCandidateBuild, .manifest_ordinal = 1};
}

bool manifestExists(Backend & backend, const Layout & layout, const ManifestId & id)
{
    return backend.head(layout.manifestKey(id)).exists;
}

bool activeSourceExists(Backend & backend, const Layout & layout, const UInt128 & source_id)
{
    const auto state_got = backend.get(layout.gcStateKey());
    if (!state_got)
        return false;
    const GcState state = decodeGcState(state_got->bytes);
    const auto seal_got = backend.get(layout.foldSealKey(state.snap_generation, state.snap_attempt));
    if (!seal_got)
        return false;
    const CasFoldSeal seal = decodeFoldSeal(seal_got->bytes);
    for (const RunRef & run : seal.blob_target_runs)
    {
        SourceEdgeRunView view = openSourceEdgeRun(backend, run.key);
        String key;
        String payload;
        while (view.next(key, payload))
        {
            if (payload.empty() || payload[0] != kEdgeActive)
                continue;
            BlobRef ref;
            UInt128 row_source{};
            SourceEdgeKeyCodec::parse(key, ref, row_source);
            if (row_source == source_id)
                return true;
        }
        view.verifyAgainst(run.checksum);
    }
    return false;
}

size_t condemnedCount(Backend & backend, const Layout & layout)
{
    size_t count = 0;
    const GcState state = decodeGcState(backend.get(layout.gcStateKey())->bytes);
    const CasFoldSeal seal = decodeFoldSeal(
        backend.get(layout.foldSealKey(state.snap_generation, state.snap_attempt))->bytes);
    for (const RunRef & run : seal.blob_target_runs)
    {
        SourceEdgeRunView view = openSourceEdgeRun(backend, run.key);
        String key;
        String payload;
        while (view.next(key, payload))
            count += !payload.empty() && payload[0] == kCondemned;
        view.verifyAgainst(run.checksum);
    }
    return count;
}

class NominationBackend : public InMemoryBackend
{
public:
    using Backend::deleteExact;
    using Backend::get;
    using Backend::putOverwrite;

    DeleteOutcome deleteExact(const String & key, const Token & token) override
    {
        if (key == watched_manifest_key)
        {
            source_absent_when_delete_started = !activeSourceExists(*this, layout, watched_source_id);
            if (replace_manifest_before_delete)
            {
                const auto got = get(key);
                if (got)
                    putOverwrite(key, got->bytes, got->token);
            }
        }
        return InMemoryBackend::deleteExact(key, token);
    }

    Layout layout{"p"};
    String watched_manifest_key;
    UInt128 watched_source_id{};
    bool source_absent_when_delete_started = false;
    bool replace_manifest_before_delete = false;
};

struct ReadyFixture
{
    std::shared_ptr<NominationBackend> backend;
    PoolPtr store;
    std::unique_ptr<Gc> gc;
    RootNamespace ns{"test/aa@cas@"};
    ManifestId candidate{ns, candidateRef()};
    std::vector<BlobRef> blobs;
};

ReadyFixture makeReadyFixture()
{
    ReadyFixture f;
    f.backend = std::make_shared<NominationBackend>();
    PoolConfig config;
    config.pool_prefix = "p";
    config.server_root_id = "gc-runner";
    config.manifest_sweep_list_budget_keys = 100;
    config.manifest_sweep_delete_budget_keys = 100;
    config.gc_fold_max_defer_rounds = 0;
    f.store = Pool::open(f.backend, config);
    f.backend->layout = f.store->layout();
    f.gc = std::make_unique<Gc>(f.store, kGcId);

    /// Establish a real catalog life and fold its cursor across epoch 1 before introducing the orphan.
    publishAt(*f.backend, f.store->layout(), f.ns, RefTxnId{1, 1}, "live-a", /*build_sequence=*/7,
              UInt128(0x7001), /*birth=*/true);
    EXPECT_TRUE(runRegularRoundReclaiming(*f.gc).acquired_lease);
    writeSealAt(*f.backend, f.store->layout(), f.ns, RefTxnId{1, 2});
    publishAt(*f.backend, f.store->layout(), f.ns, RefTxnId{2, 1}, "live-b", /*build_sequence=*/7,
              UInt128(0x7002), /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 2});
    /// Raw log helpers intentionally do not manufacture lifecycle authority. This fixture's durable
    /// frontier includes the predecessor seal and the epoch-2 start, so nomination is exercised rather
    /// than being (correctly) skipped for a missing `_ckpt`.
    writeRecoverableCkptForRawFixture(*f.backend, f.store->layout(), f.ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 2},
    });
    EXPECT_TRUE(runRegularRoundReclaiming(*f.gc).acquired_lease);
    setWatermarkMinActive(*f.backend, f.store->layout(), "test", kCandidateEpoch, /*min_active=*/6);

    std::vector<ManifestEntry> entries;
    std::vector<BlobDelta> seeded_edges;
    for (uint64_t i = 0; i < 6; ++i)
    {
        const UInt128 digest = UInt128(0x8000 + i);
        const BlobRef blob = legacyMetaTestRef(digest);
        f.blobs.push_back(blob);
        writeBlobBody(*f.backend, f.store->layout(), digest);
        const String path = "blob-" + std::to_string(i);
        entries.push_back(blobEntryFor(path, digest));
        seeded_edges.push_back(BlobDelta{
            .ref = blob,
            .source_id = sourceEdgeId(f.candidate, path),
            .remove = false});
        if (i < 4)
            seeded_edges.push_back(BlobDelta{
                .ref = blob,
                .source_id = UInt128(0x9000 + i),
                .remove = false});
    }
    writeManifestRaw(*f.backend, f.store->layout(), f.ns, f.candidate.ref, entries);

    /// Seed the exact S42 precondition: the candidate manifest's `+1` edges are already in the adopted
    /// run, yet the recovered owner view does not name the body. Four blobs also have another source.
    const auto state_got = f.backend->get(f.store->layout().gcStateKey());
    EXPECT_TRUE(state_got.has_value());
    GcState state = decodeGcState(state_got->bytes);
    const auto parent_got = f.backend->get(
        f.store->layout().foldSealKey(state.snap_generation, state.snap_attempt));
    EXPECT_TRUE(parent_got.has_value());
    CasFoldSeal seal = decodeFoldSeal(parent_got->bytes);
    const uint64_t new_generation = state.snap_generation + 1;
    const uint64_t new_attempt = state.snap_attempt + 1000;
    std::vector<RunRef> runs;
    RetiredMergeResult retired;
    foldDeltasIntoGeneration(
        *f.backend, f.store->layout(), seal.blob_target_runs,
        new_generation, new_attempt, /*shard=*/0, std::move(seeded_edges), runs,
        /*current_round=*/state.round, /*condemn_round=*/state.round,
        {}, {}, {}, &retired, /*suppress_destructive=*/false, nullptr);
    seal.parent_generation = state.snap_generation;
    seal.generation = new_generation;
    seal.blob_target_runs = std::move(runs);
    seal.condemned_summary[0] = CondemnedSummary{};
    putDeterministicArtifact(
        *f.backend, f.store->layout().foldSealKey(new_generation, new_attempt), encodeFoldSeal(seal));
    state.snap_generation = new_generation;
    state.snap_attempt = new_attempt;
    f.backend->putOverwrite(f.store->layout().gcStateKey(), encodeGcState(state), state_got->token);

    f.backend->watched_manifest_key = f.store->layout().manifestKey(f.candidate);
    f.backend->watched_source_id = sourceEdgeId(f.candidate, "blob-0");
    return f;
}

}

/// S42: sweeping an aborted precommit must retire that manifest's exact source edges before deleting
/// the body. Other sources stay intact, and only the two uniquely-owned blobs enter retirement.
TEST(CASOrphanNomination, RetiresExactManifestSourcesBeforeDelete)
{
    ReadyFixture f = makeReadyFixture();

    /// The nominating round's own `fold_reduce` phase carries probe B1/B2's per-round verdict; capture
    /// it so the orphan-sourced retirement can be proven accounting-neutral on the real end-to-end path,
    /// not only on the synthetic `foldDeltasIntoGeneration` call `SourceRetirementIsAccountingNeutral`
    /// drives below.
    std::optional<GcPhaseRecord> fold_reduce;
    f.gc->setPhaseSink([&](const GcPhaseRecord & rec) { if (rec.phase == "fold_reduce") fold_reduce = rec; });

    ASSERT_TRUE(runRegularRoundReclaiming(*f.gc).acquired_lease);

    EXPECT_FALSE(manifestExists(*f.backend, f.store->layout(), f.candidate));
    EXPECT_TRUE(f.backend->source_absent_when_delete_started)
        << "the adopted in-degree run must retire the manifest source before exact deletion begins";
    for (size_t i = 0; i < f.blobs.size(); ++i)
    {
        EXPECT_FALSE(activeSourceExists(
            *f.backend, f.store->layout(), sourceEdgeId(f.candidate, "blob-" + std::to_string(i))));
        EXPECT_EQ(inDegreeInRuns(*f.backend, runsForShard(*f.backend, f.store->layout(), 0), f.blobs[i]),
                  i < 4 ? 1 : 0);
    }
    EXPECT_EQ(condemnedCount(*f.backend, f.store->layout()), 2u);

    ASSERT_TRUE(fold_reduce.has_value());
    EXPECT_EQ(fold_reduce->metrics.at("unmatched_removes"), 0u)
        << "the orphan source retirements are exact removes against a present edge, never an unmatched one";
    EXPECT_EQ(fold_reduce->metrics.at("transactions_unapplied"), 0u)
        << "the retirement input rides the reducer alongside ordinary deltas without stranding a "
           "committed+produced ref transaction unapplied";
}

/// A nomination must exact-GET and decode the manifest before it can derive any source-edge identity.
TEST(CASOrphanNomination, CorruptManifestIsRetainedAndSurfaced)
{
    ReadyFixture f = makeReadyFixture();
    const auto got = f.backend->get(f.backend->watched_manifest_key);
    ASSERT_TRUE(got.has_value());
    f.backend->putOverwrite(f.backend->watched_manifest_key, "not a sealed manifest", got->token);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { runRegularRoundReclaiming(*f.gc); });
    EXPECT_TRUE(f.backend->head(f.backend->watched_manifest_key).exists);
}

/// Manifest identities are immutable. A changed token at the same key is illegal ABA, not an ordinary
/// exact-delete race that may be silently treated as spared.
TEST(CASOrphanNomination, TokenAbaIsRetainedAndSurfaced)
{
    ReadyFixture f = makeReadyFixture();
    f.backend->replace_manifest_before_delete = true;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { runRegularRoundReclaiming(*f.gc); });
    EXPECT_TRUE(f.backend->head(f.backend->watched_manifest_key).exists);
}

/// Nomination PLANNING itself is gated on `!suppress_destructive`
/// (`Gc::fold`'s orphan_sweep call site), not merely its eventual delete -- a suppressed pass must
/// never even LIST candidates. The suppressed universe is selected explicitly, because that is the
/// subject: a round on the production default would open the gate and sweep.
TEST(CASOrphanNomination, SuppressedRoundNominatesNothing)
{
    ReadyFixture f = makeReadyFixture();

    std::optional<GcPhaseRecord> orphan_sweep;
    f.gc->setPhaseSink([&](const GcPhaseRecord & rec) { if (rec.phase == "orphan_sweep") orphan_sweep = rec; });

    ASSERT_TRUE(f.gc->runRegularRound({}, /*allow_steal*/true,
                                     UniversePolicy::StageA_Suppressed).acquired_lease);

    ASSERT_TRUE(orphan_sweep.has_value());
    EXPECT_EQ(orphan_sweep->metrics.at("suppressed"), 1u);
    EXPECT_EQ(orphan_sweep->metrics.at("listed"), 0u)
        << "planning is gated on !suppress_destructive; a suppressed pass must not even LIST candidates";
    EXPECT_EQ(orphan_sweep->metrics.at("deleted"), 0u);
    EXPECT_TRUE(manifestExists(*f.backend, f.store->layout(), f.candidate))
        << "the orphan body must survive a suppressed round";
}

/// The retirement input is deliberately outside both ref-transaction accounting mechanisms: a
/// matching edge disappears, an already-absent one stays an idempotent no-op, and neither can alter B2.
TEST(CASOrphanNomination, SourceRetirementIsAccountingNeutral)
{
    InMemoryBackend backend;
    const Layout layout{"p"};
    const BlobRef blob = legacyMetaTestRef(UInt128(0xA001));
    const UInt128 source = UInt128(0xA002);
    std::vector<RunRef> parent_runs;
    foldDeltasIntoGeneration(
        backend, layout, {}, /*new_generation=*/1, /*attempt=*/1, /*shard=*/0,
        {BlobDelta{.ref = blob, .source_id = source, .remove = false}}, parent_runs);

    std::vector<RunRef> next_runs;
    RetiredMergeResult retired;
    std::vector<uint8_t> applied{0x5A};
    foldDeltasIntoGeneration(
        backend, layout, parent_runs, /*new_generation=*/2, /*attempt=*/2, /*shard=*/0,
        {}, next_runs, /*current_round=*/1, /*condemn_round=*/1,
        {}, {}, {}, &retired, /*suppress_destructive=*/false, &applied,
        {BlobSourceRetirement{.ref = blob, .source_id = source},
         BlobSourceRetirement{.ref = blob, .source_id = UInt128(0xA003)}});

    EXPECT_EQ(inDegreeInRuns(backend, next_runs, blob), 0);
    EXPECT_EQ(retired.unmatched_removes, 0u);
    EXPECT_EQ(applied, (std::vector<uint8_t>{0x5A}));
}
