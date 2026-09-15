#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <limits>
#include <memory>
#include <stdexcept>
#include <vector>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int NETWORK_ERROR;
}

/// Stage B: production birth wiring. `CasRefLedger::resolveNamespaceLife`, called from
/// `ensureRefTableRecovered`, resolves a namespace's real catalog life ONCE per table-open --
/// create-if-absent, adopt an existing `Live`/`Removing` entry, or reconcile a stale `Creating` one via
/// `CasRefCatalog::reconcileStaleCreator` + `isCreatorFenceTerminal` -- so every ref-layer object a
/// mounted writer produces is keyed at a real, catalog-proven incarnation (spec INV-3), never the
/// Stage-A sentinel.
///
/// OBLIGATION 3 (closed here): `CasRefCatalog::checkPublicationAdmittedOrThrow` can only enforce
/// "`Creating` forbids publication" AT THE CATALOG LEVEL, because nothing on the production ref-write
/// path consulted the catalog at all. The refusal this suite pins below rests on CONSTRUCTION, not a
/// check: there is no `if (state == Creating) throw` anywhere in `appendRefOps`'s path.
/// `ensureRefTableRecovered` simply cannot make a table's runtime usable (`rt.recovered` never becomes
/// `true`, `rt.life` never gets set) while the catalog entry is `Creating` under a fence that is not
/// provably dead -- so no append can reach `commitRefChunk` for such a namespace, by construction,
/// stronger than any per-write check could prove. Stated here so nobody later greps for a check and
/// concludes this gap is still open.
///
/// The suite name is prefixed `Cas` so it is covered by the `Cas*` unit-test gate filter.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

/// Fault the mandatory catalog's very first bootstrap write before it reaches durable storage. This
/// models a definite write failure, distinct from the acknowledgement-loss shape below: a retry must
/// still be allowed to prove a new pool, and the failed first attempt must not have published
/// `_pool_meta` without the catalog it makes mandatory.
/// Per-key counts of the WRITE primitive. `CountingBackend` counts reads, heads and lists per key but
/// only totals for writes, and its legacy per-verb counters never see a caller that speaks the
/// primitives -- which every writer below does.
class WriteCountingBackend : public CountingBackend
{
public:
    uint64_t writes(const String & key) const
    {
        std::lock_guard lock(write_count_mutex);
        const auto it = write_counts.find(key);
        return it == write_counts.end() ? 0 : it->second;
    }

    void resetWriteCounts()
    {
        std::lock_guard lock(write_count_mutex);
        write_counts.clear();
    }

    std::expected<String, DB::Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                               const std::optional<String> & expected_value,
                                                               DB::Cas::TransportAccess & access) override
    {
        {
            std::lock_guard lock(write_count_mutex);
            ++write_counts[key];
        }
        return CountingBackend::write(key, bytes, expected_value, access);
    }

private:
    mutable std::mutex write_count_mutex;
    std::map<String, uint64_t> write_counts;
};

/// Faults the mandatory catalog's very first bootstrap write before it reaches durable storage. This
/// models a definite write failure, distinct from the acknowledgement-loss shape below: a retry must
/// still be allowed to prove a new pool, and the failed first attempt must not have published
/// `_pool_meta` without the catalog it makes mandatory.
///
/// A plain `std::runtime_error`, not a `Poco::Exception`, and deliberately so: a `Poco`/transport
/// exception here would exercise the write loop's OWN ambiguity resolution rather than this suite's
/// subject, which is what `FailedCatalogBootstrapDoesNotPublishPoolMetaAndRetryConverges` actually
/// needs -- a fault that propagates out of the FIRST `Pool::open` call so a SEPARATE retry can be the
/// one that converges. The engine's write loop treats any `Poco`/transport exception as an ambiguity it
/// settles itself with one resolve read, and a one-shot fault of that class is retried and silently
/// succeeds within the SAME `Pool::open` call -- it never reaches the caller at all. A non-`Poco`
/// `std::exception` is the engine's own signal for "this could
/// not have landed" and propagates unresolved, which is what "before it reaches durable storage" means.
class CatalogBootstrapWriteFailsOnceBackend final : public WriteCountingBackend
{
public:
    std::expected<String, DB::Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                               const std::optional<String> & expected_value,
                                                               DB::Cas::TransportAccess & access) override
    {
        if (fail_once && key == Layout{"p"}.refCatalogKey())
        {
            fail_once = false;
            throw std::runtime_error("CatalogBootstrapWriteFailsOnceBackend: catalog write did not land");
        }
        return WriteCountingBackend::write(key, bytes, expected_value, access);
    }

private:
    bool fail_once = true;
};

class CatalogCancellationRaceBackend final : public WriteCountingBackend
{
public:
    std::expected<String, DB::Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                               const std::optional<String> & expected_value,
                                                               DB::Cas::TransportAccess & access) override
    {
        if (race_armed && key == Layout{"p"}.refCatalogKey())
        {
            race_armed = false;
            on_catalog_write();
        }
        return WriteCountingBackend::write(key, bytes, expected_value, access);
    }

    bool race_armed = false;
    std::function<void()> on_catalog_write;
};

PoolPtr openPoolForBirthTest(const BackendPtr & backend, const String & server_root_id = "test")
{
    seedPoolMetaForRestart(*backend);
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = server_root_id});
}

const CatalogEntry * findEntry(const RefCatalog & catalog, const RootNamespace & ns)
{
    for (const CatalogEntry & e : catalog.entries)
        if (e.ns.string() == ns.string())
            return &e;
    return nullptr;
}

/// The same one-transaction publish `gtest_cas_ref_ckpt.cpp`'s `publishRef` drives: a namespace's first
/// append through the REAL append lane, which is also what triggers `resolveNamespaceLife`.
RefTxnId publishBirth(const PoolPtr & store, const RootNamespace & ns, const String & ref)
{
    return store->appendRefOps(ns, MutationScope::ref(ref),
        [&ref](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(namespaceBirthOp());
            for (const RefOp & op : publishCommittedOps(ref, ManifestRef{1, 1, 1}))
                ops.push_back(op);
            return ops;
        },
        RootMutationOrigin::Writer, RootMutationKind::Publish);
}

}

/// The happy path: nothing to reconcile, no pre-existing entry. The first append mints a fresh `Live`
/// catalog entry and keys the birth transaction at it -- not at the Stage-A sentinel.
TEST(CASRefCatalogBirthWiring, FirstOpenMintsALiveCatalogEntryAndKeysTheBirthAtIt)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend);
    const RootNamespace ns{"srv1/birth_wiring"};

    const RefTxnId id = publishBirth(store, ns, "a");
    EXPECT_EQ(id, (RefTxnId{store->writerEpoch(), 1}));

    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, store->layout());
    const CatalogEntry * entry = findEntry(snap.catalog, ns);
    ASSERT_NE(entry, nullptr) << "the first open must mint a catalog entry";
    EXPECT_EQ(entry->state, NsState::Live);
    EXPECT_NE(entry->incarnation, UInt128(0));
    EXPECT_EQ(entry->creator, std::nullopt) << "creator is forbidden outside Creating (strict grammar)";

    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry->ns, entry->incarnation);
    EXPECT_TRUE(op.head(store->layout().refLogKey(life, id), Retry::standard()).has_value())
        << "the birth transaction must be keyed at the REAL minted incarnation, not the Stage-A sentinel";
    EXPECT_FALSE(op.head(store->layout().refLogKey(fixture::fixtureLife(ns), id), Retry::standard()).has_value())
        << "and must NOT be keyed at the sentinel any more";
}

TEST(CASRefCatalogBirthWiring, CatalogLossAfterMountCannotRecreateAOneRowAuthority)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();

    publishBirth(store, RootNamespace{"srv1/existing"}, "old");
    const auto catalog = op.read(layout.refCatalogKey(), Retry::standard());
    ASSERT_TRUE(catalog);
    ASSERT_EQ(op.remove(layout.refCatalogKey(), catalog->etag, Retry::standard()), Removal::Removed);
    backend->resetCounts();
    backend->resetWriteCounts();

    EXPECT_THROW(publishBirth(store, RootNamespace{"srv1/new"}, "new"), DB::Exception);
    EXPECT_FALSE(op.head(layout.refCatalogKey(), Retry::standard()).has_value())
        << "runtime loss must not be repaired with a one-row replacement authority";
    EXPECT_EQ(backend->writeTotal(), 0u)
        << "the failed birth must not publish a catalog, a checkpoint or a ref-log body";
}

TEST(CASRefCatalogBirthWiring, FailedCatalogBootstrapDoesNotPublishPoolMetaAndRetryConverges)
{
    auto backend = std::make_shared<CatalogBootstrapWriteFailsOnceBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};

    EXPECT_ANY_THROW(Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));
    EXPECT_FALSE(op.head(layout.poolMetaKey(), Retry::standard()).has_value())
        << "a failed mandatory catalog bootstrap must leave no authoritative pool meta behind";
    EXPECT_FALSE(op.head(layout.refCatalogKey(), Retry::standard()).has_value());

    PoolPtr retry;
    ASSERT_NO_THROW(retry = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));
    EXPECT_TRUE(op.head(layout.poolMetaKey(), Retry::standard()).has_value());
    EXPECT_TRUE(op.head(layout.refCatalogKey(), Retry::standard()).has_value());
}

TEST(CASRefCatalogBirthWiring, LostCatalogBootstrapAcknowledgementResolvesToCommittedWithoutARetryOrADuplicate)
{
    auto backend = std::make_shared<LandedButAckLostOnceBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    backend->key_substr = layout.refCatalogKey();

    /// The catalog create's own write landed; only its response was lost. `LandedButAckLostOnceBackend`
    /// is documented to model exactly that -- a caller that resolves the ambiguity meets its OWN
    /// earlier write as the occupant -- so the engine's one resolve read proves this attempt committed.
    /// The whole bootstrap therefore converges in this SINGLE `Pool::open` call: no throw, no second
    /// catalog write, and no second `Pool::open` needed.
    PoolPtr store;
    ASSERT_NO_THROW(store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));
    EXPECT_TRUE(op.head(layout.poolMetaKey(), Retry::standard()).has_value());
    EXPECT_TRUE(op.head(layout.refCatalogKey(), Retry::standard()).has_value());
    EXPECT_EQ(backend->putCount(layout.refCatalogKey()), 1u)
        << "a landed write whose ack is lost must be proven by a read, never repeated";
}

TEST(CASRefCatalogBirthWiring, BootstrapConflictExactReadsTheCanonicalEmptyCatalog)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    const String canonical_empty = encodeRefCatalog(RefCatalog{});
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(layout.refCatalogKey(), canonical_empty, Retry::standard())));
    backend->resetCounts();
    backend->resetWriteCounts();

    const CasRefCatalog::Snapshot snap = CasRefCatalog::initializeEmptyForNewPool(op, layout);
    EXPECT_TRUE(snap.catalog.entries.empty());
    EXPECT_EQ(backend->writes(layout.refCatalogKey()), 1u);
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 1u)
        << "a concurrent bootstrap winner must be exact-read before acceptance";
}

TEST(CASRefCatalogBirthWiring, BootstrapConflictRefusesANonemptyCatalog)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    const RefCatalog nonempty{.entries = {CatalogEntry{
        .ns = RootNamespace{"test/nonempty"}, .state = NsState::Live, .incarnation = UInt128{1}, .creator = std::nullopt}}};
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(layout.refCatalogKey(), encodeRefCatalog(nonempty), Retry::standard())));
    backend->resetCounts();
    backend->resetWriteCounts();

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { CasRefCatalog::initializeEmptyForNewPool(op, layout); });
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 1u);
}

TEST(CASRefCatalogBirthWiring, ExistingPoolMetaWithMissingCatalogStillFailsClosed)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    PoolPtr first = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const auto catalog = op.read(first->layout().refCatalogKey(), Retry::standard());
    ASSERT_TRUE(catalog);
    ASSERT_EQ(op.remove(first->layout().refCatalogKey(), catalog->etag, Retry::standard()), Removal::Removed);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}); });
}

TEST(CASRefCatalogBirthWiring, RestartFixturePreservesItsExistingNonemptyCatalog)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    seedPoolMetaForRestart(*backend);
    const auto empty = op.read(layout.refCatalogKey(), Retry::standard());
    ASSERT_TRUE(empty);

    const RefCatalog nonempty{.entries = {CatalogEntry{
        .ns = RootNamespace{"test/preserved"}, .state = NsState::Live, .incarnation = UInt128{1}, .creator = std::nullopt}}};
    const String bytes = encodeRefCatalog(nonempty);
    ASSERT_TRUE(std::holds_alternative<Committed>(
        op.replace(layout.refCatalogKey(), bytes, empty->etag, Retry::standard())));
    const auto before = op.read(layout.refCatalogKey(), Retry::standard());
    ASSERT_TRUE(before);

    seedPoolMetaForRestart(*backend);
    const auto after = op.read(layout.refCatalogKey(), Retry::standard());
    ASSERT_TRUE(after);
    EXPECT_EQ(after->bytes, before->bytes);
    EXPECT_EQ(after->etag, before->etag);
}

/// A namespace whose catalog entry is ALREADY `Live` (e.g. admitted by an earlier mount that this
/// runtime never cached) must be ADOPTED, never re-minted: `CasRefCatalog::createNamespace` refuses
/// outright once any entry exists, so `resolveNamespaceLife` has no create branch left to take here --
/// only the adopt branch can succeed.
TEST(CASRefCatalogBirthWiring, AnExistingLiveEntryIsAdoptedRatherThanReminted)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/adopt_live"};

    const CatalogEntry entry{.ns = ns, .state = NsState::Live, .incarnation = UInt128(0xcafe),
                             .creator = std::nullopt};
    CasRefCatalog::casAdmitEntry(op, layout, 1, entry);
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = store->writerEpoch(),
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    const RefTxnId id = publishBirth(store, ns, "a");
    EXPECT_EQ(id, (RefTxnId{store->writerEpoch(), 1}));

    /// The read result must outlive the returned pointer -- findEntry points into its entries.
    const auto after_cut = CasRefCatalog::read(op, layout);
    const CatalogEntry * after = findEntry(after_cut.catalog, ns);
    ASSERT_NE(after, nullptr);
    EXPECT_EQ(after->incarnation, UInt128(0xcafe)) << "adopted, not re-minted";
    EXPECT_EQ(after->state, NsState::Live);

    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, UInt128(0xcafe));
    EXPECT_TRUE(op.head(layout.refLogKey(life, id), Retry::standard()).has_value());
}

/// Regression (CI PR#2073, `tiered_storage_cas`, part `all_1_1_0` of a fresh table): seven concurrent
/// `MergeTreeBackgroundExecutor` movers all reached `resolveNamespaceLife`'s "no entry" read for the
/// SAME namespace before any of them landed a row. The winner's `createNamespace` ran its full three
/// steps to `Live` inside the WINDOW between the loop's own "no entry" read and the loser's own
/// `createNamespace` pre-check read -- so the loser's pre-check itself observed `Live`, not "no entry".
/// That must be adopted through the loop's normal re-read, never abort the server.
TEST(CASRefCatalogBirthWiring, ASiblingsFullCreateInsideCreateNamespacesOwnPreCheckWindowIsAdoptedNotAbort)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation sibling_op = requests.admit();
    auto store = openPoolForBirthTest(backend, "loser-server");
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/precheck_race"};
    const CreatorFence sibling_fence{.server_root_id = "sibling-server", .writer_epoch = 1, .fence_generation = 1};

    /// Fires once, inside the LOSER's own `store->namespaceLife` -> `resolveNamespaceLife` ->
    /// `createNamespace` call, right before that call's pre-check read -- i.e. AFTER
    /// `resolveNamespaceLife`'s own loop already observed no entry. Runs a sibling's entire
    /// `createNamespace` to completion in that window, so the loser's own pre-check read is the one
    /// that observes the sibling's `Live` row.
    CasRefCatalog::setCreateNamespacePreCheckHookForTest([&]
    {
        const auto sibling_outcome = CasRefCatalog::createNamespace(sibling_op, layout, 1, ns, sibling_fence);
        ASSERT_EQ(sibling_outcome, CasRefCatalog::NamespaceCreationOutcome::Live);
    });

    std::optional<NamespaceLifeId> life;
    EXPECT_NO_THROW(life = store->namespaceLife(ns));

    /// The read result must outlive the returned pointer -- findEntry points into its entries.
    const auto snap = CasRefCatalog::read(sibling_op, layout);
    size_t rows_for_ns = 0;
    for (const CatalogEntry & e : snap.catalog.entries)
        if (e.ns.string() == ns.string())
            ++rows_for_ns;
    EXPECT_EQ(rows_for_ns, 1u) << "the loser's refused pre-check left no trace of its own";
    const CatalogEntry * entry = findEntry(snap.catalog, ns);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->state, NsState::Live);
    ASSERT_TRUE(life.has_value());
    EXPECT_EQ(*life, NamespaceLifeId::fromCatalogEntry(ns, entry->incarnation)) << "the sibling's incarnation, adopted";
}

/// OBLIGATION 3, pinned through the PRODUCTION path: a `Creating` entry left by a DIFFERENT, still-live
/// (or at least not provably dead) actor refuses every append -- no test-only seam, no direct call to
/// `resolveNamespaceLife`/`reconcileStaleCreator`, just an ordinary `appendRefOps`.
TEST(CASRefCatalogBirthWiring, ANamespaceStuckCreatingUnderALiveForeignFenceRefusesProductionPublicationByConstruction)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/stuck_creating"};

    /// A DIFFERENT actor's `Creating` entry naming a server root that never mounted at all --
    /// `isCreatorFenceTerminal`'s own doc: an ABSENT mount slot answers nothing about liveness, so it
    /// is treated as NOT terminal (fail closed), never as proof of death.
    const CreatorFence foreign_creator{.server_root_id = "ghost-server", .writer_epoch = 9, .fence_generation = 1};
    const CatalogEntry entry{.ns = ns, .state = NsState::Creating, .incarnation = UInt128(0xdead),
                             .creator = foreign_creator};
    CasRefCatalog::casAdmitEntry(op, layout, 1, entry);
    backend->resetCounts();
    backend->resetWriteCounts();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { publishBirth(store, ns, "a"); });

    /// Nothing was written: the entry is exactly as observed, still Creating, still the foreign fence.
    /// The read result must outlive the returned pointer -- findEntry points into its entries.
    const auto still_cut = CasRefCatalog::read(op, layout);
    const CatalogEntry * still = findEntry(still_cut.catalog, ns);
    ASSERT_NE(still, nullptr);
    EXPECT_EQ(*still, entry) << "a refused resolution must write nothing";
    EXPECT_EQ(backend->writeTotal(), 0u);
}

/// The mirror image, and the deferred obligation to wire `reconcileStaleCreator` and pin it with a
/// test that drives reconciliation through the discovery path rather than by calling the primitive
/// directly: a dead predecessor's `Creating` entry is reconciled onto THIS mount and completed to
/// `Live`, over the SAME incarnation -- resumption, not rebirth.
TEST(CASRefCatalogBirthWiring, AStaleCreatingEntryFromATerminatedForeignFenceIsReconciledThroughTheProductionPath)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend, "this-server");
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/reconciled"};

    /// A dead predecessor's `Creating` entry: its mount lease carries the clean-farewell sentinel
    /// (`min_active_build_sequence == UINT64_MAX`), one of `isCreatorFenceTerminal`'s three certificates of death.
    const CreatorFence dead_creator{.server_root_id = "dead-server", .writer_epoch = 3, .fence_generation = 1};
    const CatalogEntry entry{.ns = ns, .state = NsState::Creating, .incarnation = UInt128(0xbeef),
                             .creator = dead_creator};
    CasRefCatalog::casAdmitEntry(op, layout, 1, entry);
    setWatermarkMinActive(*backend, layout, "dead-server", /*writer_epoch=*/3,
                          /*min_active_build_sequence=*/std::numeric_limits<uint64_t>::max());

    /// The production path resumes creation itself: reconciles the stale entry onto THIS mount's own
    /// fence and completes it to `Live`, over the SAME incarnation the dead creator minted.
    const RefTxnId id = publishBirth(store, ns, "a");
    EXPECT_EQ(id, (RefTxnId{store->writerEpoch(), 1}));

    /// The read result must outlive the returned pointer -- findEntry points into its entries.
    const auto live_cut = CasRefCatalog::read(op, layout);
    const CatalogEntry * live = findEntry(live_cut.catalog, ns);
    ASSERT_NE(live, nullptr);
    EXPECT_EQ(live->state, NsState::Live);
    EXPECT_EQ(live->incarnation, UInt128(0xbeef)) << "the SAME incarnation throughout -- resumption, not rebirth";
    EXPECT_EQ(live->creator, std::nullopt);

    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, UInt128(0xbeef));
    EXPECT_TRUE(op.head(layout.refLogKey(life, id), Retry::standard()).has_value());
}

TEST(CASRefCatalogBirthWiring, DropRefusesLiveCreatingFenceWithZeroCatalogMutation)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"drop_live_creator"};
    const CatalogEntry creating{
        .ns = ns,
        .state = NsState::Creating,
        .incarnation = UInt128{0xd001},
        .creator = CreatorFence{.server_root_id = "unproven-live", .writer_epoch = 7, .fence_generation = 1}};
    CasRefCatalog::casAdmitEntry(op, layout, 1, creating);
    backend->resetCounts();
    backend->resetWriteCounts();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(ns); });
    EXPECT_EQ(backend->writeTotal(), 0u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_EQ(CasRefCatalog::read(op, layout).catalog.entries, std::vector<CatalogEntry>{creating});
}

TEST(CASRefCatalogBirthWiring, DropDeletesTerminalCreatingExactlyAndLeavesCkptForJanitor)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"drop_terminal_creator"};
    const CatalogEntry creating{
        .ns = ns,
        .state = NsState::Creating,
        .incarnation = UInt128{0xd002},
        .creator = CreatorFence{.server_root_id = "dead-creator", .writer_epoch = 8, .fence_generation = 1}};
    CasRefCatalog::casAdmitEntry(op, layout, 1, creating);
    setWatermarkMinActive(*backend, layout, "dead-creator", 8, std::numeric_limits<uint64_t>::max());
    const NamespaceLifeId old_life = NamespaceLifeId::fromCatalogEntry(ns, creating.incarnation);
    const String ckpt_key = layout.refCkptKey(old_life);
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(ckpt_key, "stalled-ckpt", Retry::standard())));
    backend->resetCounts();
    backend->resetWriteCounts();

    store->dropNamespace(ns);
    EXPECT_EQ(backend->writes(layout.refCatalogKey()), 1u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_TRUE(op.head(ckpt_key, Retry::standard()).has_value());
    EXPECT_TRUE(CasRefCatalog::read(op, layout).catalog.entries.empty());

    const NamespaceLifeId reborn = store->namespaceLife(ns);
    EXPECT_NE(reborn.incarnation, old_life.incarnation);
}

TEST(CASRefCatalogBirthWiring, DropLosesExactCreatingRaceToReconciliationWithoutDeletingCkpt)
{
    auto backend = std::make_shared<CatalogCancellationRaceBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"drop_reconcile_race"};
    const CreatorFence old_creator{
        .server_root_id = "dead-racing-creator", .writer_epoch = 9, .fence_generation = 1};
    const CatalogEntry creating{
        .ns = ns, .state = NsState::Creating, .incarnation = UInt128{0xd003}, .creator = old_creator};
    CasRefCatalog::casAdmitEntry(op, layout, 1, creating);
    setWatermarkMinActive(
        *backend, layout, old_creator.server_root_id, old_creator.writer_epoch,
        std::numeric_limits<uint64_t>::max());
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, creating.incarnation);
    const String ckpt_key = layout.refCkptKey(life);
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(ckpt_key, "stalled-ckpt", Retry::standard())));
    backend->on_catalog_write = [&]
    {
        EXPECT_EQ(CasRefCatalog::reconcileStaleCreator(
            op, layout, creating,
            CreatorFence{.server_root_id = "replacement", .writer_epoch = 10, .fence_generation = 1},
            [](const CreatorFence &) { return true; }),
            CasRefCatalog::ReconcileCreatorOutcome::Reconciled);
    };
    backend->race_armed = true;
    backend->resetCounts();
    backend->resetWriteCounts();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(ns); });
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_TRUE(op.head(ckpt_key, Retry::standard()).has_value());
    const CasRefCatalog::Snapshot after = CasRefCatalog::read(op, layout);
    ASSERT_EQ(after.catalog.entries.size(), 1u);
    ASSERT_TRUE(after.catalog.entries.front().creator);
    EXPECT_EQ(after.catalog.entries.front().creator->server_root_id, "replacement");
}

TEST(CASRefCatalogBirthWiring, FencedDropCannotCancelTerminalCreating)
{
    auto backend = std::make_shared<CatalogCancellationRaceBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"fenced_drop_terminal_creator"};
    const CatalogEntry creating{
        .ns = ns,
        .state = NsState::Creating,
        .incarnation = UInt128{0xd004},
        .creator = CreatorFence{.server_root_id = "dead-fenced-creator", .writer_epoch = 11, .fence_generation = 1}};
    CasRefCatalog::casAdmitEntry(op, layout, 1, creating);
    setWatermarkMinActive(*backend, layout, "dead-fenced-creator", 11, std::numeric_limits<uint64_t>::max());
    backend->resetCounts();
    backend->resetWriteCounts();

    /// The first cancellation attempt passes its fence check, then loses its catalog CAS while the
    /// local mount is re-armed at a new fence generation. The retry must re-check the caller fence and
    /// refuse before another catalog mutation attempt.
    backend->on_catalog_write = [&]
    {
        rearmMountFenceAfterAnomalyForTest(store);
        backend->refuseNextWrite(layout.refCatalogKey());
    };
    backend->race_armed = true;

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(ns); });
    EXPECT_EQ(backend->writes(layout.refCatalogKey()), 1u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_EQ(CasRefCatalog::read(op, layout).catalog.entries, std::vector<CatalogEntry>{creating});
}

TEST(CASRefCatalogBirthWiring, ExactOldLifeCannotCancelReplacementTerminalCreating)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"exact_old_life_terminal_creator"};
    const NamespaceLifeId predecessor = NamespaceLifeId::fromCatalogEntry(ns, UInt128{0xd005});
    const CatalogEntry successor{
        .ns = ns,
        .state = NsState::Creating,
        .incarnation = UInt128{0xd006},
        .creator = CreatorFence{.server_root_id = "dead-successor-creator", .writer_epoch = 12, .fence_generation = 1}};
    CasRefCatalog::casAdmitEntry(op, layout, 1, successor);
    setWatermarkMinActive(*backend, layout, "dead-successor-creator", 12, std::numeric_limits<uint64_t>::max());
    const String ckpt_key = layout.refCkptKey(NamespaceLifeId::fromCatalogEntry(ns, successor.incarnation));
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(ckpt_key, "successor-ckpt", Retry::standard())));
    backend->resetCounts();
    backend->resetWriteCounts();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(predecessor); });
    EXPECT_EQ(backend->writeTotal(), 0u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_EQ(CasRefCatalog::read(op, layout).catalog.entries, std::vector<CatalogEntry>{successor});
}

namespace
{

/// Leaves the catalog holding a body no previously observed entry equals: every read first bumps each
/// entry's creator fence generation, so `reconcileStaleCreator`'s token-exactness check refuses on
/// every attempt and `resolveNamespaceLife`'s state machine can never converge.
class ChurningCatalogBackend final : public InMemoryBackend
{
public:
    explicit ChurningCatalogBackend(String catalog_key_)
        : catalog_key(std::move(catalog_key_))
    {
    }

    bool churning = false;

    std::optional<Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        if (churning && key == catalog_key)
            bumpEveryCreatorFence(access);
        return InMemoryBackend::read(key, access);
    }

private:
    /// Qualified calls, never the virtual ones: this must not re-enter its own churn.
    void bumpEveryCreatorFence(DB::Cas::TransportAccess & access)
    {
        const std::optional<Raw> got = InMemoryBackend::read(catalog_key, access);
        if (!got)
            return;
        RefCatalog catalog = decodeRefCatalog(got->bytes);
        for (CatalogEntry & entry : catalog.entries)
            if (entry.creator)
                ++entry.creator->fence_generation;
        (void)InMemoryBackend::write(catalog_key, encodeRefCatalog(catalog), got->value, access);
    }

    const String catalog_key;
};

}

/// A catalog entry that moves under every read drives `resolveNamespaceLife`'s state machine for ever.
/// One `Retry` frozen before the loop bounds the WHOLE resolution to a single standard window, and
/// every re-read a competing actor forces is paced by a jittered sleep -- so a permanently churning
/// catalog costs one window, not one fresh window per verb per iteration hammered with no wait between
/// them.
///
/// What the clock bound below does NOT check: it bounds this call's own wall time, not the number of
/// requests the loop sent, and it says nothing about the paths that converge.
TEST(CASRefCatalogBirthWiring, APerpetuallyChurningCatalogEntryIsPacedAndEndsWithinOneWindow)
{
    auto backend = std::make_shared<ChurningCatalogBackend>(Layout{"p"}.refCatalogKey());
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"churning_creating"};

    /// A FOREIGN creator, so the loop takes the reconciliation branch on every iteration.
    const CatalogEntry creating{
        .ns = ns,
        .state = NsState::Creating,
        .incarnation = UInt128{0xc001},
        .creator = CreatorFence{.server_root_id = "foreign-creator", .writer_epoch = 7, .fence_generation = 1}};
    CasRefCatalog::casAdmitEntry(op, layout, 1, creating);

    auto clock = DB::Cas::tests::VirtualRetryClock::installOn(store);
    backend->churning = true;
    const size_t pauses_before = clock->pauseCount();
    const uint64_t now_before = clock->nowMs();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->namespaceLife(ns); });

    /// Sixteen jittered draws cannot exhaust a 90 s window even at their ceiling, so an unpaced loop
    /// reaches its iteration cap having slept nothing at all.
    EXPECT_GE(clock->pauseCount() - pauses_before, 16u) << "each forced re-read is paced";
    /// The frozen window, plus at most one backoff draw the pace does not consult the deadline for,
    /// plus the virtual clock's one extra millisecond per pause.
    EXPECT_LE(clock->nowMs() - now_before, 95'100u) << "one standard window bounds the whole loop";
}
