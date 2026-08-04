#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"

namespace DB::ErrorCodes
{
extern const int ABORTED;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;
using DB::Cas::tests::injectRetire;

namespace
{

PoolPtr makePoolWithShards(std::shared_ptr<InMemoryBackend> & out_backend, uint64_t gc_shards = 1)
{
    out_backend = std::make_shared<InMemoryBackend>();
    return Pool::open(out_backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_shards = gc_shards});
}

ManifestRef testRef(uint64_t seq)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = 1};
}

}

/// Review I5: `discoverUniverse` is catalog-authoritative (Task 4-C), and this test used to survive
/// the switch from LIST-based discovery unchanged -- `publishCommittedTransition` admits a catalog
/// entry as its own side effect, so LIST-based and catalog-based discovery were indistinguishable to
/// it. Pins the three shapes that actually distinguish the two sources directly:
///   (a) a `Live` catalog entry with ZERO ref objects IS in the universe -- the catalog alone decides;
///   (b) a `Creating` entry is EXCLUDED -- spec §3, no publication can exist yet;
///   (c) a namespace with ref OBJECTS but NO catalog entry is EXCLUDED -- the C1 shape: the catalog is
///       the authority, so its absence is authoritative too, however much debris LIST would still find.
TEST(CASGCShardIncarnation, DiscoveryEqualsPresentShards)
{
    for (const uint64_t gc_shards : {1u, 4u})
    {
        std::shared_ptr<InMemoryBackend> backend;
        auto store = makePoolWithShards(backend, gc_shards);
        Gc gc(store, hexToU128("0000000000000000000000000000000a"));
        const Layout & layout = store->layout();

        const RootNamespace ns_live_empty{"srv1/tblLiveEmpty"};
        const RootNamespace ns_creating{"srv1/tblCreating"};
        const RootNamespace ns_uncataloged{"srv1/tblUncataloged"};

        /// (a) Admitted Live, nothing else ever written under it.
        fixture::admitLive(*backend, layout, ns_live_empty);

        /// (b) A genuinely Creating entry, admitted directly (step 1 alone -- never completed to Live).
        CasRefCatalog::casAdmitEntry(*backend, layout, store->poolConfig().gc_shards, CatalogEntry{.ns = ns_creating, .state = NsState::Creating,
            .incarnation = UInt128(1), .creator = CreatorFence{.server_root_id = "test", .writer_epoch = 1, .fence_generation = 1}});

        /// (c) Ref objects present, but the catalog was never told (or has since forgotten): write
        /// through the real path, which self-admits, then strip the entry back out to simulate "the
        /// catalog does not name it" without touching the ref objects it left behind.
        writeManifestRaw(*backend, layout, ns_uncataloged, testRef(1), {});
        publishCommittedTransition(*backend, layout, ns_uncataloged, "part_1", std::nullopt, testRef(1), /*shard=*/0);
        {
            CasRefCatalog::Snapshot snap = CasRefCatalog::read(*backend, layout);
            std::erase_if(snap.catalog.entries, [&](const CatalogEntry & e) { return e.ns.string() == ns_uncataloged.string(); });
            const HeadResult h = backend->head(layout.refCatalogKey());
            ASSERT_TRUE(h.exists);
            ASSERT_EQ(backend->putOverwrite(layout.refCatalogKey(), encodeRefCatalog(snap.catalog), h.token).outcome,
                       PutOutcome::Done);
        }

        const auto universe = gc.discoverUniverseForTest();

        /// Stage B (Task 4-C): the universe is catalog-authoritative now, so it is a life per namespace
        /// (there are no numeric shards to destructure -- see `NamespaceLifeId`), never a
        /// `(namespace, shard)` pair.
        bool found_live_empty = false;
        for (const NamespaceLifeId & life : universe)
        {
            if (life.ns.string() == ns_live_empty.string())
                found_live_empty = true;
            EXPECT_NE(life.ns.string(), ns_creating.string()) << "a Creating entry must never be discovered";
            EXPECT_NE(life.ns.string(), ns_uncataloged.string())
                << "ref objects with no catalog entry must not be discovered, however much debris LIST would find";
        }
        EXPECT_TRUE(found_live_empty) << "a Live catalog entry with zero ref objects must still be discovered";

        /// Confirm (b) really is still Creating (not merely absent from a differently-shaped universe).
        const CasRefCatalog::Snapshot final_snap = CasRefCatalog::read(*backend, layout);
        const auto creating_it = std::find_if(final_snap.catalog.entries.begin(), final_snap.catalog.entries.end(),
            [&](const CatalogEntry & e) { return e.ns.string() == ns_creating.string(); });
        ASSERT_NE(creating_it, final_snap.catalog.entries.end());
        EXPECT_EQ(creating_it->state, NsState::Creating);
    }
}

/// Catalog ambiguity stops destructive GC and REBUILD before either can derive authority from a
/// first row. No attempted delete is allowed on the rejected regular round.
TEST(CASGCShardIncarnation, DuplicateLifeIdStopsDestructiveRoundAndRebuild)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_shards = 1});
    const Layout & layout = store->layout();
    RefCatalog catalog;
    catalog.entries = {
        CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Live, .incarnation = UInt128{77}},
        CatalogEntry{
            .ns = RootNamespace{"b"},
            .state = NsState::Removing,
            .incarnation = UInt128{77},
            .removal_started_round = 1},
    };
    const auto empty_catalog = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(empty_catalog);
    ASSERT_EQ(backend->putOverwrite(
        layout.refCatalogKey(), encodeRefCatalog(catalog), empty_catalog->token).outcome, PutOutcome::Done);
    backend->resetCounts();

    Gc gc(store, hexToU128("0000000000000000000000000000000a"));
    EXPECT_THROW(gc.runRegularRound({}, /*allow_steal=*/true, UniversePolicy::Authoritative), DB::Exception);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_THROW(gc.rebuildBaseline(/*force=*/true), DB::Exception);
}

/// A physical life id carries no reversible logical namespace component. Once the catalog moves a
/// logical name to a new life, the former stream is opaque debris: it cannot redirect GC to that name
/// or contribute an edge to the current-life fold. The separately paced janitor may reclaim its
/// unowned physical objects after that fold.
TEST(CASGCShardIncarnation, DeadLifeStreamIsOpaqueInertDebris)
{
    std::shared_ptr<InMemoryBackend> backend;
    auto store = makePoolWithShards(backend, /*gc_shards=*/1);
    Gc gc(store, hexToU128("0000000000000000000000000000000a"));
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/tblIncarnationSwap"};

    CasRefCatalog::casAdmitEntry(*backend, layout, store->poolConfig().gc_shards, CatalogEntry{.ns = ns, .state = NsState::Live,
        .incarnation = UInt128(11), .creator = std::nullopt});   // Live forbids a creator fence
    const ManifestRef dead_ref = testRef(1);
    writeBlobBody(*backend, layout, UInt128(11));
    writeManifestRaw(*backend, layout, ns, dead_ref, {blobEntryFor("dead", UInt128(11))});
    std::vector<RefOp> dead_ops{namespaceBirthOp()};
    const auto dead_committed_ops = publishCommittedOps("part_dead", dead_ref);
    dead_ops.insert(dead_ops.end(), dead_committed_ops.begin(), dead_committed_ops.end());
    appendRefLogSeed(*backend, layout, ns, std::move(dead_ops));   // real but unacknowledged record at incarnation 11
    const NamespaceLifeId dead_life = NamespaceLifeId::fromCatalogEntry(ns, UInt128(11));

    {
        CasRefCatalog::Snapshot snap = CasRefCatalog::read(*backend, layout);
        const auto it = std::find_if(snap.catalog.entries.begin(), snap.catalog.entries.end(),
            [&](const CatalogEntry & e) { return e.ns.string() == ns.string(); });
        ASSERT_NE(it, snap.catalog.entries.end());
        it->incarnation = UInt128(22);   // "recreated" -- same name, different (empty) key space
        const HeadResult h = backend->head(layout.refCatalogKey());
        ASSERT_TRUE(h.exists);
        ASSERT_EQ(backend->putOverwrite(layout.refCatalogKey(), encodeRefCatalog(snap.catalog), h.token).outcome,
                   PutOutcome::Done);
    }

    const NamespaceLifeId current_life = NamespaceLifeId::fromCatalogEntry(ns, UInt128(22));
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(current_life), encodeRefCkpt(RefCkpt{
        .life_epoch = std::optional<uint64_t>{1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);
    const ManifestRef current_ref = testRef(2);
    writeBlobBody(*backend, layout, UInt128(22));
    writeManifestRaw(*backend, layout, ns, current_ref, {blobEntryFor("current", UInt128(22))});
    std::vector<RefOp> current_ops{namespaceBirthOp()};
    const auto committed_ops = publishCommittedOps("part_current", current_ref);
    current_ops.insert(current_ops.end(), committed_ops.begin(), committed_ops.end());
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(), .txn_id = RefTxnId{1, 1}, .ops = std::move(current_ops), .prev_epoch_seal = std::nullopt});
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = std::optional<uint64_t>{1},
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt});

    const RoundReport report = gc.runRegularRound({}, /*allow_steal=*/true, UniversePolicy::Authoritative);
    EXPECT_TRUE(report.anomalies.empty());
    EXPECT_FALSE(report.deferred);
    EXPECT_EQ(inDegreeOf(*backend, layout, UInt128(22)), 1);
    EXPECT_EQ(inDegreeOf(*backend, layout, UInt128(11)), 0)
        << "the unmatched old life must not contribute its unacknowledged edge to the current-life fold";
}

/// Checkpoints live in the state tree and are read by exact key from the catalog cut. They are never
/// discovered through the hot stream LIST, so hiding one from LIST must not affect the round.
TEST(CASGCShardIncarnation, CurrentLifeCheckpointIsReadByExactKeyOutsideHotList)
{
    auto backend = std::make_shared<HintHoleBackendOn<CountingBackend>>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test", .gc_shards = 1,
        .gc_fold_max_defer_rounds = 0});
    Gc gc(store, hexToU128("0000000000000000000000000000000a"));
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/tblOrdinaryRebirth"};

    CasRefCatalog::casAdmitEntry(*backend, layout, store->poolConfig().gc_shards, CatalogEntry{.ns = ns, .state = NsState::Live,
        .incarnation = UInt128(11), .creator = std::nullopt});

    CatalogEntry after_rebirth{.ns = ns, .state = NsState::Live, .incarnation = UInt128(22), .creator = std::nullopt};
    {
        CasRefCatalog::Snapshot snap = CasRefCatalog::read(*backend, layout);
        const auto it = std::find_if(snap.catalog.entries.begin(), snap.catalog.entries.end(),
            [&](const CatalogEntry & e) { return e.ns.string() == ns.string(); });
        ASSERT_NE(it, snap.catalog.entries.end());
        *it = after_rebirth;   // "recreated" -- same name, new (current) incarnation 22
        const HeadResult h = backend->head(layout.refCatalogKey());
        ASSERT_TRUE(h.exists);
        ASSERT_EQ(backend->putOverwrite(layout.refCatalogKey(), encodeRefCatalog(snap.catalog), h.token).outcome,
                   PutOutcome::Done);
    }

    /// The successor's own genesis `_ckpt`, published for the current physical life. Hiding it from
    /// LIST must be irrelevant because the walk obtains state only through exact GETs.
    const NamespaceLifeId current_life = NamespaceLifeId::fromCatalogEntry(ns, UInt128(22));
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(current_life),
        encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .checkpoint_snapshot_id = std::nullopt,
                              .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);
    backend->hide(layout.refCkptKey(current_life));
    backend->resetCounts();
    std::vector<GcPhaseRecord> phases;
    gc.setPhaseSink([&](const GcPhaseRecord & phase) { phases.push_back(phase); });

    const RoundReport report = gc.runRegularRound({}, /*allow_steal=*/true, UniversePolicy::Authoritative);
    EXPECT_FALSE(report.deferred) << "the forced catalog-only fold must reach checkpoint intake";
    EXPECT_GT(backend->getCount(layout.refCkptKey(current_life)), 0u)
        << "the catalog-derived current life must drive an exact checkpoint GET";
    EXPECT_EQ(backend->listCount(layout.namespaceStreamRootPrefix()), 1u)
        << "the round must build exactly one hot stream plan";
    EXPECT_EQ(backend->listCount(layout.namespaceStateRootPrefix()), 0u)
        << "checkpoint state must never receive its own hot LIST";
    EXPECT_EQ(backend->listCount(layout.namespaceRootPrefix()), 1u)
        << "the only broader LIST is the separately paced janitor page";
    EXPECT_EQ(backend->holesServed(), 1u)
        << "the hidden checkpoint is omitted only from the janitor's broad page, never from the hot stream LIST";
    EXPECT_TRUE(backend->head(layout.refCkptKey(current_life)).exists)
        << "the post-page catalog cut retains the current life even when LIST omitted its checkpoint";
    const auto cleanup = std::find_if(phases.begin(), phases.end(), [](const GcPhaseRecord & phase)
    {
        return phase.phase == "namespace_cleanup";
    });
    ASSERT_NE(cleanup, phases.end());
    EXPECT_EQ(cleanup->metrics.at("janitor_pages"), 1u);
    EXPECT_EQ(cleanup->metrics.at("janitor_deleted"), 0u);
    bool saw_anomaly_for_ns = false;
    for (const RoundAnomaly & a : report.anomalies)
        if (a.ns.string() == ns.string())
            saw_anomaly_for_ns = true;
    EXPECT_FALSE(saw_anomaly_for_ns)
        << "the current life's `_ckpt` is real and readable by its exact catalog-derived key";
}

/// A stream life absent from the immutable catalog cut cannot be attributed to any logical namespace.
/// It remains inert debris rather than producing a made-up name or a round anomaly.
TEST(CASGCShardIncarnation, UncatalogedStreamLifeDefersWithoutInventingNamespace)
{
    std::shared_ptr<InMemoryBackend> backend;
    auto store = makePoolWithShards(backend, /*gc_shards=*/1);
    Gc gc(store, hexToU128("0000000000000000000000000000000a"));
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/tblForgotten"};

    writeManifestRaw(*backend, layout, ns, testRef(1), {});
    publishCommittedTransition(*backend, layout, ns, "part_1", std::nullopt, testRef(1), /*shard=*/0);
    const NamespaceLifeId forgotten_life = store->namespaceLife(ns);

    {
        CasRefCatalog::Snapshot snap = CasRefCatalog::read(*backend, layout);
        std::erase_if(snap.catalog.entries, [&](const CatalogEntry & e) { return e.ns.string() == ns.string(); });
        const HeadResult h = backend->head(layout.refCatalogKey());
        ASSERT_TRUE(h.exists);
        ASSERT_EQ(backend->putOverwrite(layout.refCatalogKey(), encodeRefCatalog(snap.catalog), h.token).outcome,
                   PutOutcome::Done);
    }

    const RoundReport report = gc.runRegularRound({}, /*allow_steal=*/true, UniversePolicy::Authoritative);
    EXPECT_TRUE(report.anomalies.empty());
    EXPECT_FALSE(backend->list(layout.namespaceStreamPrefix(forgotten_life), "", 100).keys.empty());
}

/// State-tree objects are point-addressed only. A stalled creator's checkpoint and an unowned opaque
/// checkpoint are both outside the hot stream scan and cannot manufacture logical namespace anomalies.
TEST(CASGCShardIncarnation, StateCheckpointsOutsideCatalogAreInertToHotWalk)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_shards = 1});
    Gc gc(store, hexToU128("0000000000000000000000000000000a"));
    const Layout & layout = store->layout();
    const RootNamespace creating_ns{"srv1/tblStalledBirth"};
    const RootNamespace unrelated_gone_ns{"srv1/tblGenuinelyGone"};

    /// Step 1 of createNamespace: insert the Creating entry with a live creator fence.
    CasRefCatalog::casAdmitEntry(*backend, layout, store->poolConfig().gc_shards, CatalogEntry{.ns = creating_ns, .state = NsState::Creating,
        .incarnation = UInt128(33),
        .creator = CreatorFence{.server_root_id = "test", .writer_epoch = 1, .fence_generation = 1}});
    /// Step 2, without step 3: publish the genesis `_ckpt` directly, at the SAME incarnation the
    /// Creating entry names -- exactly what `completeCreation` durably leaves behind if the creator
    /// crashes between its own steps 2 and 3.
    const NamespaceLifeId creating_life = NamespaceLifeId::fromCatalogEntry(creating_ns, UInt128(33));
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(creating_life),
        encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .checkpoint_snapshot_id = std::nullopt,
                              .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);

    /// Opaque state debris with no corresponding catalog entry.
    const NamespaceLifeId gone_life = NamespaceLifeId::fromCatalogEntry(unrelated_gone_ns, UInt128(44));
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(gone_life),
        encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .checkpoint_snapshot_id = std::nullopt,
                              .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);

    /// Add one fully current stream so the round performs a fold rather than stopping at an empty
    /// walk. Catalog and checkpoint admission keep this traffic out of the janitor's dead-life set,
    /// isolating the one deliberately unowned checkpoint below.
    const RootNamespace ordinary_ns{"srv1/tblOrdinaryTraffic"};
    fixture::admitLive(*backend, layout, ordinary_ns);
    const NamespaceLifeId ordinary_life = fixture::fixtureLife(ordinary_ns);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(ordinary_life),
        encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .checkpoint_snapshot_id = std::nullopt,
                              .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);
    appendRefLogSeed(*backend, layout, ordinary_ns, {});

    backend->resetCounts();
    std::vector<GcPhaseRecord> phases;
    gc.setPhaseSink([&](const GcPhaseRecord & phase) { phases.push_back(phase); });

    const RoundReport report = gc.runRegularRound({}, /*allow_steal=*/true, UniversePolicy::Authoritative);
    bool saw_stalled_birth_anomaly = false;
    bool saw_genuinely_gone_anomaly = false;
    for (const RoundAnomaly & a : report.anomalies)
    {
        if (a.ns.string() == creating_ns.string())
            saw_stalled_birth_anomaly = true;
        if (a.ns.string() == unrelated_gone_ns.string())
            saw_genuinely_gone_anomaly = true;
    }
    EXPECT_FALSE(saw_stalled_birth_anomaly);
    EXPECT_FALSE(saw_genuinely_gone_anomaly);
    EXPECT_EQ(backend->listCount(layout.namespaceStreamRootPrefix()), 1u)
        << "all hot intake must consume one immutable stream listing";
    EXPECT_EQ(backend->listCount(layout.namespaceStateRootPrefix()), 0u)
        << "state checkpoints are never a hot discovery source";
    EXPECT_EQ(backend->listCount(layout.namespaceRootPrefix()), 1u)
        << "the only ownership-tree listing belongs to the independently paced janitor";
    EXPECT_GT(backend->getCount(layout.refCkptKey(ordinary_life)), 0u)
        << "the cataloged Live life is read by its exact checkpoint key";
    EXPECT_EQ(backend->getCount(layout.refCkptKey(creating_life)), 0u)
        << "Creating is retained by the janitor cut but excluded from hot checkpoint intake";
    EXPECT_EQ(backend->getCount(layout.refCkptKey(gone_life)), 0u)
        << "uncataloged state debris is classified by the janitor page, never exact-read by the hot walk";
    EXPECT_TRUE(backend->head(layout.refCkptKey(creating_life)).exists);
    EXPECT_TRUE(backend->head(layout.refCkptKey(ordinary_life)).exists);
    EXPECT_FALSE(backend->head(layout.refCkptKey(gone_life)).exists)
        << "catalog absence is inert to the hot walk but authorizes the later janitor exact-token delete";
    const auto cleanup = std::find_if(phases.begin(), phases.end(), [](const GcPhaseRecord & phase)
    {
        return phase.phase == "namespace_cleanup";
    });
    ASSERT_NE(cleanup, phases.end());
    EXPECT_EQ(cleanup->metrics.at("janitor_pages"), 1u);
    EXPECT_EQ(cleanup->metrics.at("janitor_deleted"), 1u);
}

/// `listNamespaces` projects the authoritative catalog; physical streams never contribute names.
TEST(CASGCShardIncarnation, ListNamespacesFromCatalog)
{
    for (const uint64_t gc_shards : {1u, 4u})
    {
        std::shared_ptr<InMemoryBackend> backend;
        auto store = makePoolWithShards(backend, gc_shards);

        const RootNamespace ns_a{"srv1/tblA"};

        EXPECT_TRUE(store->listNamespaces("").namespaces.empty());

        /// The real writer path admits the catalog row for namespace A.
        writeManifestRaw(*backend, store->layout(), ns_a, testRef(1), {});
        publishCommittedTransition(*backend, store->layout(), ns_a, "part_1", std::nullopt, testRef(1), /*shard=*/0);

        const auto nss = store->listNamespaces("").namespaces;
        ASSERT_EQ(nss.size(), 1u);
        EXPECT_EQ(nss[0], "srv1/tblA");

        /// Prefix filter: no match.
        EXPECT_TRUE(store->listNamespaces("srv2/").namespaces.empty());
        /// Prefix filter: match.
        const auto filtered = store->listNamespaces("srv1/").namespaces;
        ASSERT_EQ(filtered.size(), 1u);
        EXPECT_EQ(filtered[0], "srv1/tblA");
    }
}

/// Task 5: THM-NO-RETURN create-race. A NEWBORN ref-shard is born fenced to the current GC round
/// (self-floor: `fence_round` self-floors to `currentGcRound()` on the create-if-absent branch).
///
/// Scenario (registry-free create-race):
///   1. Open a Pool (gc/state absent).
///   2. Write blob b1's body directly to the backend (present, not yet condemned).
///   3. Inject gc/state at round 1 with b1 condemned (its current token in the retired set).
///      b1's body is still PRESENT — this simulates GC having fenced+retired b1 but not yet
///      deleted it (the retired-but-body-present window).
///   4. A writer for NEWBORN ns B calls `precommitAdd` → reads `currentGcRound() = 1` →
///      the NEWBORN shard is born with `fence_round = 1` (self-floor).
///   5. `promote` binds the condemned-but-present tokenless leaf AS IS (spec
///      2026-07-09-cas-writer-gc-simplification D5: there is no writer-side view refresh at promote
///      any more). This is safe because the precommit closure's edge is journal-durable BEFORE
///      promote returns (EDGE-BEFORE-OBSERVE): the NEXT GC fold sees net in-degree >= 1 for b1 and
///      SPARES the entry, regardless of when it would otherwise graduate — the condemnation is
///      doomed, never the blob. INV-NO-DANGLE holds (dangling=0 in fsck).
///
/// Both gc_shards=1 and gc_shards>1 are exercised. The self-floor and promote gate are independent
/// of the blob-hash-prefix sharding axis (fence_round lives in the ROOT shard).
TEST(CASGCShardIncarnation, NewbornPrecommitProtectsDedupBlobAgainstConcurrentDrop)
{
    for (const uint64_t gc_shards : {1u, 4u})
    {
        std::shared_ptr<InMemoryBackend> backend;
        auto store = makePoolWithShards(backend, gc_shards);
        const RootNamespace ns_b{"srv1/tblB"};

        /// --- Phase 1: Write b1's body directly (before any GC). ---
        /// Mint b1 under the POOL streaming-hash id (via a throwaway build's putBlob) so the in-closure
        /// copy-forward verifier accepts its payload — the plain CityHash test id would be refused.
        const String b1_payload = "shared-blob-b1";
        const String b1_hex = streamingHexOf(b1_payload);
        const BlobRef b1_ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128(b1_hex))};
        {
            auto seed = store->beginPartWrite({});
            seed->putBlob(b1_ref, BlobSource::fromString(b1_payload));
        }
        const String b1_key = store->layout().blobKey(b1_ref);
        ASSERT_TRUE(backend->head(b1_key).exists)
            << "b1 body must be present after the seed putBlob";
        const Token b1_token = backend->head(b1_key).token;

        /// --- Phase 2: Inject gc/state at round 1 with b1 CONDEMNED (body still present). ---
        /// This simulates GC having advanced to round 1 and retired b1 (condemned token recorded
        /// in the retired set) but not yet deleted b1's body object.
        injectRetire(*backend, store->layout(), /*round*/ 1, /*shard*/ 0,
            {RetiredEntry{.kind = ObjectKind::Blob, .ref = b1_ref,
                          .token = b1_token, .size = static_cast<uint64_t>(b1_payload.size())}});

        /// Sanity: currentGcRound() reads gc/state fresh and returns 1.
        ASSERT_EQ(store->currentGcRound(), 1u)
            << "currentGcRound() must return the injected round";

        /// --- Phase 3: Writer for NEWBORN ns B — b1 condemned but body present ---
        PartWriteInfo info_b;
        info_b.intended_ref = ns_b.string() + "/part_b1";
        auto build_b = store->beginPartWrite(info_b);

        /// Adopt b1 by tokenless evidence (simulating the dedup case: the writer observed b1
        /// present BEFORE the GC round — no HEAD here, just evidence).
        ManifestEntry dep_b1;
        dep_b1.path = "data.bin";
        dep_b1.placement = EntryPlacement::Blob;
        dep_b1.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hexToU128(b1_hex))};

        dep_b1.blob_size = b1_payload.size();
        build_b->adoptEvidence(dep_b1);

        const ManifestId id_b = build_b->stageManifest({dep_b1});

        /// precommitAdd: NEWBORN shard does not exist yet. Reads currentGcRound() = 1 → stamps
        /// fence_round = 1 (self-floor). An existing shard would keep its old fence_round.
        build_b->precommitAdd(ns_b, "part_b1", id_b);

        /// --- Phase 4: promote — the safety assertion (Phase-A contract) ---
        /// Spec 2026-07-09-cas-writer-gc-simplification D5: there is NO writer-side view refresh at
        /// promote any more — the K3 gate binds the condemned-but-present token AS IS. This is SAFE
        /// because the precommit closure's edge has been journal-durable since precommitAdd (BEFORE
        /// promote returns), so the NEXT GC fold sees net in-degree >= 1 for b1 and SPARES the entry
        /// (EDGE-BEFORE-OBSERVE) regardless of round-paced graduation timing — the condemnation is
        /// doomed, never the blob. (No GC round runs in this test at all; the argument is what makes
        /// deferring the round safe, not something this test drives to completion.)
        /// The former behavior (self-floor-forced refresh → in-closure copy-forward → fresh incarnation)
        /// was TLA+-Gate-A-verified redundant; the shard's fence_round stamp itself (THM-NO-RETURN birth
        /// floor) remains and is asserted by the sibling shard-incarnation tests.
        EXPECT_NO_THROW(build_b->promote(ns_b, "part_b1", build_b->buildId(), id_b))
            << "gc_shards=" << gc_shards << ": promote must commit — the durable edge protects the "
               "condemned-but-present tokenless leaf without any refresh or copy-forward";
        EXPECT_TRUE(store->resolveRef(ns_b, "part_b1").has_value())
            << "gc_shards=" << gc_shards << ": the ref must commit";
        /// The condemned token is bound UNCHANGED — no displacement happens (and none is needed).
        EXPECT_EQ(backend->head(b1_key).token, b1_token)
            << "gc_shards=" << gc_shards << ": no copy-forward under the Phase-A contract — the token "
               "stays; the folded edge will spare it at the next fold (no round runs here to delete it)";

        /// INV-NO-DANGLE: the body is present and no GC round ever runs in this test to fold the
        /// precommit/committed edge; a real deployment's next fold would see net in-degree >= 1 and
        /// spare the entry. A regression that let the delete pipeline race a live durable edge would
        /// produce dangling=1 here.
        const FsckReport rep = runFsck(*store, /*detail=*/false);
        EXPECT_EQ(rep.dangling, 0u)
            << "gc_shards=" << gc_shards << ": INV-NO-DANGLE violated — a committed ref names a "
               "missing blob (dangling=" << rep.dangling << ", reachable=" << rep.reachable << ")";
    }
}

/// The five shard-OBJECT-reclaim tests that used to follow (`DroppedShardObjectIsReclaimed`,
/// `IdleButLiveShardNotReclaimed`, `RecreateAfterReclaimFoldsFromZero`, `ActivatedPrecommitBlocksShardReclaim`,
/// `ReviveRacesReclaimAborts`) were removed with the snapshot+log ref model. They asserted GC reclaims /
/// token-guards a MUTABLE per-namespace ref-shard object at `rootShardKey(ns, shard)`. There is no such
/// mutable object anymore: a namespace's ref state is its immutable `_log`/`_snap` objects, physical
/// reclamation belongs to the perpetual namespace janitor, and ABA safety is structural -- a recreated
/// namespace uses a different opaque life id. The still-meaningful reincarnation case (a terminal old
/// life followed by a new life folds without inheriting the old cursor) is covered by
/// `gtest_cas_ref_gc.cpp`; lifecycle completion itself requires only folded terminal evidence and the
/// exact catalog-row mutation.
