#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <limits>
#include <memory>
#include <vector>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int NETWORK_ERROR;
}

/// Stage B Task 4-C: production birth wiring. `CasRefLedger::resolveNamespaceLife`, called from
/// `ensureRefTableRecovered`, resolves a namespace's real catalog life ONCE per table-open --
/// create-if-absent, adopt an existing `Live`/`Removing` entry, or reconcile a stale `Creating` one via
/// `CasRefCatalog::reconcileStaleCreator` + `isCreatorFenceTerminal` -- so every ref-layer object a
/// mounted writer produces is keyed at a real, catalog-proven incarnation (spec INV-3), never the
/// Stage-A sentinel.
///
/// OBLIGATION 3 (carried from Task 3's review, closed here): Task 3 could only enforce "`Creating`
/// forbids publication" (`CasRefCatalog::checkPublicationAdmittedOrThrow`) AT THE CATALOG LEVEL, because
/// nothing on the production ref-write path consulted the catalog at all. The refusal this suite pins
/// below rests on CONSTRUCTION, not a check: there is no `if (state == Creating) throw` anywhere in
/// `appendRefOps`'s path. `ensureRefTableRecovered` simply cannot make a table's runtime usable
/// (`rt.recovered` never becomes `true`, `rt.life` never gets set) while the catalog entry is `Creating`
/// under a fence that is not provably dead -- so no append can reach `commitRefChunk` for such a
/// namespace, by construction, stronger than any per-write check could prove. Stated here so nobody
/// later greps for a check and concludes the gap Task 3's review flagged is still open.
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
class CatalogBootstrapPutFailsOnceBackend final : public CountingBackend
{
public:
    using CountingBackend::putIfAbsent;

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        if (fail_once && key == Layout{"p"}.refCatalogKey())
        {
            fail_once = false;
            throw Poco::TimeoutException("CatalogBootstrapPutFailsOnceBackend: catalog PUT did not land");
        }
        return CountingBackend::putIfAbsent(key, bytes, meta);
    }

private:
    bool fail_once = true;
};

class CatalogCancellationRaceBackend final : public CountingBackend
{
public:
    using CountingBackend::casPut;

    CasResult casPut(
        const String & key, const String & bytes, const std::optional<Token> & expected,
        const ObjectMeta & meta) override
    {
        if (race_armed && key == Layout{"p"}.refCatalogKey())
        {
            race_armed = false;
            on_catalog_cas();
        }
        return CountingBackend::casPut(key, bytes, expected, meta);
    }

    bool race_armed = false;
    std::function<void()> on_catalog_cas;
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
    auto store = openPoolForBirthTest(backend);
    const RootNamespace ns{"srv1/birth_wiring"};

    const RefTxnId id = publishBirth(store, ns, "a");
    EXPECT_EQ(id, (RefTxnId{store->writerEpoch(), 1}));

    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(*backend, store->layout());
    const CatalogEntry * entry = findEntry(snap.catalog, ns);
    ASSERT_NE(entry, nullptr) << "the first open must mint a catalog entry";
    EXPECT_EQ(entry->state, NsState::Live);
    EXPECT_NE(entry->incarnation, UInt128(0));
    EXPECT_EQ(entry->creator, std::nullopt) << "creator is forbidden outside Creating (strict grammar)";

    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry->ns, entry->incarnation);
    EXPECT_TRUE(backend->head(store->layout().refLogKey(life, id)).exists)
        << "the birth transaction must be keyed at the REAL minted incarnation, not the Stage-A sentinel";
    EXPECT_FALSE(backend->head(store->layout().refLogKey(fixture::fixtureLife(ns), id)).exists)
        << "and must NOT be keyed at the sentinel any more";
}

TEST(CASRefCatalogBirthWiring, CatalogLossAfterMountCannotRecreateAOneRowAuthority)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();

    publishBirth(store, RootNamespace{"srv1/existing"}, "old");
    const auto catalog = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog);
    ASSERT_EQ(backend->deleteExact(layout.refCatalogKey(), catalog->token).kind,
              DeleteOutcome::Kind::Deleted);
    backend->resetCounts();

    EXPECT_THROW(publishBirth(store, RootNamespace{"srv1/new"}, "new"), DB::Exception);
    EXPECT_FALSE(backend->head(layout.refCatalogKey()).exists)
        << "runtime loss must not be repaired with a one-row replacement authority";
    EXPECT_EQ(backend->casPutTotal(), 0u);
    EXPECT_EQ(backend->putTotal(), 0u)
        << "the failed birth must not publish a checkpoint or ref-log body";
    EXPECT_EQ(backend->putOverwriteTotal(), 0u);
}

TEST(CASRefCatalogBirthWiring, FailedCatalogBootstrapDoesNotPublishPoolMetaAndRetryConverges)
{
    auto backend = std::make_shared<CatalogBootstrapPutFailsOnceBackend>();
    const Layout layout{"p"};

    EXPECT_ANY_THROW(Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));
    EXPECT_FALSE(backend->head(layout.poolMetaKey()).exists)
        << "a failed mandatory catalog bootstrap must leave no authoritative pool meta behind";
    EXPECT_FALSE(backend->head(layout.refCatalogKey()).exists);

    PoolPtr retry;
    ASSERT_NO_THROW(retry = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));
    EXPECT_TRUE(backend->head(layout.poolMetaKey()).exists);
    EXPECT_TRUE(backend->head(layout.refCatalogKey()).exists);
}

TEST(CASRefCatalogBirthWiring, LostCatalogBootstrapAcknowledgementLeavesOnlyRetryableCatalogResidue)
{
    auto backend = std::make_shared<LandedButAckLostOnceBackend>();
    const Layout layout{"p"};
    backend->key_substr = layout.refCatalogKey();

    EXPECT_ANY_THROW(Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));
    EXPECT_FALSE(backend->head(layout.poolMetaKey()).exists);
    EXPECT_TRUE(backend->head(layout.refCatalogKey()).exists)
        << "the injected write must land before its acknowledgement is lost";

    PoolPtr retry;
    ASSERT_NO_THROW(retry = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));
    EXPECT_TRUE(backend->head(layout.poolMetaKey()).exists);
}

TEST(CASRefCatalogBirthWiring, BootstrapConflictExactReadsTheCanonicalEmptyCatalog)
{
    auto backend = std::make_shared<CountingBackend>();
    const Layout layout{"p"};
    const String canonical_empty = encodeRefCatalog(RefCatalog{});
    ASSERT_EQ(backend->putIfAbsent(layout.refCatalogKey(), canonical_empty).outcome, PutOutcome::Done);
    backend->resetCounts();

    const CasRefCatalog::Snapshot snap = CasRefCatalog::initializeEmptyForNewPool(*backend, layout);
    EXPECT_TRUE(snap.catalog.entries.empty());
    EXPECT_EQ(backend->putCount(layout.refCatalogKey()), 1u);
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 1u)
        << "a concurrent bootstrap winner must be exact-read before acceptance";
}

TEST(CASRefCatalogBirthWiring, BootstrapConflictRefusesANonemptyCatalog)
{
    auto backend = std::make_shared<CountingBackend>();
    const Layout layout{"p"};
    const RefCatalog nonempty{.entries = {CatalogEntry{
        .ns = RootNamespace{"test/nonempty"}, .state = NsState::Live, .incarnation = UInt128{1}, .creator = std::nullopt}}};
    ASSERT_EQ(backend->putIfAbsent(layout.refCatalogKey(), encodeRefCatalog(nonempty)).outcome, PutOutcome::Done);
    backend->resetCounts();

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { CasRefCatalog::initializeEmptyForNewPool(*backend, layout); });
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 1u);
}

TEST(CASRefCatalogBirthWiring, ExistingPoolMetaWithMissingCatalogStillFailsClosed)
{
    auto backend = std::make_shared<CountingBackend>();
    PoolPtr first = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const auto catalog = backend->get(first->layout().refCatalogKey());
    ASSERT_TRUE(catalog);
    ASSERT_EQ(backend->deleteExact(first->layout().refCatalogKey(), catalog->token).kind,
              DeleteOutcome::Kind::Deleted);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}); });
}

TEST(CASRefCatalogBirthWiring, RestartFixturePreservesItsExistingNonemptyCatalog)
{
    auto backend = std::make_shared<CountingBackend>();
    const Layout layout{"p"};
    seedPoolMetaForRestart(*backend);
    const auto empty = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(empty);

    const RefCatalog nonempty{.entries = {CatalogEntry{
        .ns = RootNamespace{"test/preserved"}, .state = NsState::Live, .incarnation = UInt128{1}, .creator = std::nullopt}}};
    const String bytes = encodeRefCatalog(nonempty);
    ASSERT_EQ(backend->putOverwrite(layout.refCatalogKey(), bytes, empty->token).outcome, PutOutcome::Done);
    const auto before = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(before);

    seedPoolMetaForRestart(*backend);
    const auto after = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(after);
    EXPECT_EQ(after->bytes, before->bytes);
    EXPECT_EQ(after->token, before->token);
}

/// A namespace whose catalog entry is ALREADY `Live` (e.g. admitted by an earlier mount that this
/// runtime never cached) must be ADOPTED, never re-minted: `CasRefCatalog::createNamespace` refuses
/// outright once any entry exists, so `resolveNamespaceLife` has no create branch left to take here --
/// only the adopt branch can succeed.
TEST(CASRefCatalogBirthWiring, AnExistingLiveEntryIsAdoptedRatherThanReminted)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/adopt_live"};

    const CatalogEntry entry{.ns = ns, .state = NsState::Live, .incarnation = UInt128(0xcafe),
                             .creator = std::nullopt};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, entry);
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = store->writerEpoch(),
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    const RefTxnId id = publishBirth(store, ns, "a");
    EXPECT_EQ(id, (RefTxnId{store->writerEpoch(), 1}));

    /// The read result must outlive the returned pointer -- findEntry points into its entries.
    const auto after_cut = CasRefCatalog::read(*backend, layout);
    const CatalogEntry * after = findEntry(after_cut.catalog, ns);
    ASSERT_NE(after, nullptr);
    EXPECT_EQ(after->incarnation, UInt128(0xcafe)) << "adopted, not re-minted";
    EXPECT_EQ(after->state, NsState::Live);

    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, UInt128(0xcafe));
    EXPECT_TRUE(backend->head(layout.refLogKey(life, id)).exists);
}

/// OBLIGATION 3, pinned through the PRODUCTION path: a `Creating` entry left by a DIFFERENT, still-live
/// (or at least not provably dead) actor refuses every append -- no test-only seam, no direct call to
/// `resolveNamespaceLife`/`reconcileStaleCreator`, just an ordinary `appendRefOps`.
TEST(CASRefCatalogBirthWiring, ANamespaceStuckCreatingUnderALiveForeignFenceRefusesProductionPublicationByConstruction)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/stuck_creating"};

    /// A DIFFERENT actor's `Creating` entry naming a server root that never mounted at all --
    /// `isCreatorFenceTerminal`'s own doc: an ABSENT mount slot answers nothing about liveness, so it
    /// is treated as NOT terminal (fail closed), never as proof of death.
    const CreatorFence foreign_creator{.server_root_id = "ghost-server", .writer_epoch = 9, .fence_generation = 1};
    const CatalogEntry entry{.ns = ns, .state = NsState::Creating, .incarnation = UInt128(0xdead),
                             .creator = foreign_creator};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, entry);
    backend->resetCounts();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { publishBirth(store, ns, "a"); });

    /// Nothing was written: the entry is exactly as observed, still Creating, still the foreign fence.
    /// The read result must outlive the returned pointer -- findEntry points into its entries.
    const auto still_cut = CasRefCatalog::read(*backend, layout);
    const CatalogEntry * still = findEntry(still_cut.catalog, ns);
    ASSERT_NE(still, nullptr);
    EXPECT_EQ(*still, entry) << "a refused resolution must write nothing";
    EXPECT_EQ(backend->putTotal(), 0u);
    EXPECT_EQ(backend->putOverwriteTotal(), 0u);
    EXPECT_EQ(backend->casPutTotal(), 0u);
}

/// The mirror image, and Task 3's own deferred obligation ("wire `reconcileStaleCreator` and pin it
/// with a test that drives reconciliation through the discovery path rather than by calling the
/// primitive directly"): a dead predecessor's `Creating` entry is reconciled onto THIS mount and
/// completed to `Live`, over the SAME incarnation -- resumption, not rebirth.
TEST(CASRefCatalogBirthWiring, AStaleCreatingEntryFromATerminatedForeignFenceIsReconciledThroughTheProductionPath)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForBirthTest(backend, "this-server");
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/reconciled"};

    /// A dead predecessor's `Creating` entry: its mount lease carries the clean-farewell sentinel
    /// (`min_active == UINT64_MAX`), one of `isCreatorFenceTerminal`'s three certificates of death.
    const CreatorFence dead_creator{.server_root_id = "dead-server", .writer_epoch = 3, .fence_generation = 1};
    const CatalogEntry entry{.ns = ns, .state = NsState::Creating, .incarnation = UInt128(0xbeef),
                             .creator = dead_creator};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, entry);
    setWatermarkMinActive(*backend, layout, "dead-server", /*writer_epoch=*/3,
                          /*min_active=*/std::numeric_limits<uint64_t>::max());

    /// The production path resumes creation itself: reconciles the stale entry onto THIS mount's own
    /// fence and completes it to `Live`, over the SAME incarnation the dead creator minted.
    const RefTxnId id = publishBirth(store, ns, "a");
    EXPECT_EQ(id, (RefTxnId{store->writerEpoch(), 1}));

    /// The read result must outlive the returned pointer -- findEntry points into its entries.
    const auto live_cut = CasRefCatalog::read(*backend, layout);
    const CatalogEntry * live = findEntry(live_cut.catalog, ns);
    ASSERT_NE(live, nullptr);
    EXPECT_EQ(live->state, NsState::Live);
    EXPECT_EQ(live->incarnation, UInt128(0xbeef)) << "the SAME incarnation throughout -- resumption, not rebirth";
    EXPECT_EQ(live->creator, std::nullopt);

    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, UInt128(0xbeef));
    EXPECT_TRUE(backend->head(layout.refLogKey(life, id)).exists);
}

TEST(CASRefCatalogBirthWiring, DropRefusesLiveCreatingFenceWithZeroCatalogMutation)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"drop_live_creator"};
    const CatalogEntry creating{
        .ns = ns,
        .state = NsState::Creating,
        .incarnation = UInt128{0xd001},
        .creator = CreatorFence{.server_root_id = "unproven-live", .writer_epoch = 7, .fence_generation = 1}};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, creating);
    backend->resetCounts();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(ns); });
    EXPECT_EQ(backend->putTotal(), 0u);
    EXPECT_EQ(backend->putOverwriteTotal(), 0u);
    EXPECT_EQ(backend->casPutTotal(), 0u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_EQ(CasRefCatalog::read(*backend, layout).catalog.entries, std::vector<CatalogEntry>{creating});
}

TEST(CASRefCatalogBirthWiring, DropDeletesTerminalCreatingExactlyAndLeavesCkptForJanitor)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"drop_terminal_creator"};
    const CatalogEntry creating{
        .ns = ns,
        .state = NsState::Creating,
        .incarnation = UInt128{0xd002},
        .creator = CreatorFence{.server_root_id = "dead-creator", .writer_epoch = 8, .fence_generation = 1}};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, creating);
    setWatermarkMinActive(*backend, layout, "dead-creator", 8, std::numeric_limits<uint64_t>::max());
    const NamespaceLifeId old_life = NamespaceLifeId::fromCatalogEntry(ns, creating.incarnation);
    const String ckpt_key = layout.refCkptKey(old_life);
    ASSERT_EQ(backend->putIfAbsent(ckpt_key, "stalled-ckpt").outcome, PutOutcome::Done);
    backend->resetCounts();

    store->dropNamespace(ns);
    EXPECT_EQ(backend->casPutCount(layout.refCatalogKey()), 1u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_EQ(backend->deleteCount(ckpt_key), 0u);
    EXPECT_TRUE(backend->head(ckpt_key).exists);
    EXPECT_TRUE(CasRefCatalog::read(*backend, layout).catalog.entries.empty());

    const NamespaceLifeId reborn = store->namespaceLife(ns);
    EXPECT_NE(reborn.incarnation, old_life.incarnation);
}

TEST(CASRefCatalogBirthWiring, DropLosesExactCreatingRaceToReconciliationWithoutDeletingCkpt)
{
    auto backend = std::make_shared<CatalogCancellationRaceBackend>();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"drop_reconcile_race"};
    const CreatorFence old_creator{
        .server_root_id = "dead-racing-creator", .writer_epoch = 9, .fence_generation = 1};
    const CatalogEntry creating{
        .ns = ns, .state = NsState::Creating, .incarnation = UInt128{0xd003}, .creator = old_creator};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, creating);
    setWatermarkMinActive(
        *backend, layout, old_creator.server_root_id, old_creator.writer_epoch,
        std::numeric_limits<uint64_t>::max());
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, creating.incarnation);
    const String ckpt_key = layout.refCkptKey(life);
    ASSERT_EQ(backend->putIfAbsent(ckpt_key, "stalled-ckpt").outcome, PutOutcome::Done);
    backend->on_catalog_cas = [&]
    {
        EXPECT_EQ(CasRefCatalog::reconcileStaleCreator(
            *backend, layout, creating,
            CreatorFence{.server_root_id = "replacement", .writer_epoch = 10, .fence_generation = 1},
            [](const CreatorFence &) { return true; }, store->fenceGeneration(),
            [](uint64_t) {}), CasRefCatalog::ReconcileCreatorOutcome::Reconciled);
    };
    backend->race_armed = true;
    backend->resetCounts();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(ns); });
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_EQ(backend->deleteCount(ckpt_key), 0u);
    EXPECT_TRUE(backend->head(ckpt_key).exists);
    const CasRefCatalog::Snapshot after = CasRefCatalog::read(*backend, layout);
    ASSERT_EQ(after.catalog.entries.size(), 1u);
    ASSERT_TRUE(after.catalog.entries.front().creator);
    EXPECT_EQ(after.catalog.entries.front().creator->server_root_id, "replacement");
}

TEST(CASRefCatalogBirthWiring, FencedDropCannotCancelTerminalCreating)
{
    auto backend = std::make_shared<CatalogCancellationRaceBackend>();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"fenced_drop_terminal_creator"};
    const CatalogEntry creating{
        .ns = ns,
        .state = NsState::Creating,
        .incarnation = UInt128{0xd004},
        .creator = CreatorFence{.server_root_id = "dead-fenced-creator", .writer_epoch = 11, .fence_generation = 1}};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, creating);
    setWatermarkMinActive(*backend, layout, "dead-fenced-creator", 11, std::numeric_limits<uint64_t>::max());
    backend->resetCounts();

    /// The first cancellation attempt passes its fence check, then loses its catalog CAS while the
    /// local mount is re-armed at a new fence generation. The retry must re-check the caller fence and
    /// refuse before another catalog mutation attempt.
    backend->on_catalog_cas = [&]
    {
        rearmMountFenceAfterAnomalyForTest(store);
        backend->failNextCasPut(layout.refCatalogKey());
    };
    backend->race_armed = true;

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(ns); });
    EXPECT_EQ(backend->casPutCount(layout.refCatalogKey()), 1u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_EQ(CasRefCatalog::read(*backend, layout).catalog.entries, std::vector<CatalogEntry>{creating});
}

TEST(CASRefCatalogBirthWiring, ExactOldLifeCannotCancelReplacementTerminalCreating)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForBirthTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"exact_old_life_terminal_creator"};
    const NamespaceLifeId predecessor = NamespaceLifeId::fromCatalogEntry(ns, UInt128{0xd005});
    const CatalogEntry successor{
        .ns = ns,
        .state = NsState::Creating,
        .incarnation = UInt128{0xd006},
        .creator = CreatorFence{.server_root_id = "dead-successor-creator", .writer_epoch = 12, .fence_generation = 1}};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, successor);
    setWatermarkMinActive(*backend, layout, "dead-successor-creator", 12, std::numeric_limits<uint64_t>::max());
    const String ckpt_key = layout.refCkptKey(NamespaceLifeId::fromCatalogEntry(ns, successor.incarnation));
    ASSERT_EQ(backend->putIfAbsent(ckpt_key, "successor-ckpt").outcome, PutOutcome::Done);
    backend->resetCounts();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(predecessor); });
    EXPECT_EQ(backend->casPutTotal(), 0u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    EXPECT_EQ(backend->deleteCount(ckpt_key), 0u);
    EXPECT_EQ(CasRefCatalog::read(*backend, layout).catalog.entries, std::vector<CatalogEntry>{successor});
}
