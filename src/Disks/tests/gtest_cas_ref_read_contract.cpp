#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <algorithm>
#include <memory>
#include <optional>

/// The ref-side read contract (the ten `CasRefCatalog::read` sites in `CasRefLedger.cpp` were
/// classified elsewhere: every site is a mutation/admission authority or a per-key destructive
/// revalidation, and every live table reader reaches `acquireReadableRefTableRuntime`, whose warm path
/// returns the resident runtime before any catalog read). These are COVERAGE PINS for a contract the
/// classification predicted already holds, not a fix: a held reader runtime answers stale-or-absent
/// across a same-name rebirth, a warm read costs no catalog request, and the one held ref-writer seam
/// that exists (`dropNamespace(const NamespaceLifeId &)`) refuses across the same rebirth rather than
/// touching the successor.

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

/// One committed ref, born and published through the REAL production write path (`beginPartWrite` /
/// `stageManifest` / `precommitAdd` / `promote`) -- this is what mints `ns`'s catalog life for real,
/// exactly as an ordinary insert would, rather than a fixture sentinel.
ManifestId publishRefThroughPool(const PoolPtr & store, const RootNamespace & ns, const String & ref_name)
{
    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/" + ref_name;
    auto build = store->beginPartWrite(info);
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, ref_name, id);
    build->promote(ns, ref_name, build->buildId(), id);
    return id;
}

/// Delete the current catalog life through the production exact-removal authority (`casUpdate` to
/// `Removing`, then `deleteCompletedRemoving` under a held fence), retaining every old physical byte
/// and any already-resident runtime. Mirrors `gtest_cas_ns_file_read_contract.cpp`'s
/// `deleteCatalogLife` -- lifecycle-real, not a raw sentinel overwrite.
void deleteCatalogLife(Backend & backend, const Layout & layout, const NamespaceLifeId & life)
{
    CasRefCatalog::casUpdate(backend, layout, [&](const RefCatalog & current)
    {
        RefCatalog next = current;
        const auto it = std::find_if(next.entries.begin(), next.entries.end(), [&](const CatalogEntry & entry)
        {
            return entry.ns == life.ns && entry.incarnation == life.incarnation;
        });
        if (it == next.entries.end())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Missing fixture catalog life '{}'", life.ns.string());
        it->state = NsState::Removing;
        it->removal_started_round = 1;
        return next;
    });

    const CasRefCatalog::Snapshot snapshot = CasRefCatalog::read(backend, layout);
    const auto it = std::find_if(snapshot.catalog.entries.begin(), snapshot.catalog.entries.end(), [&](const CatalogEntry & entry)
    {
        return entry.ns == life.ns && entry.incarnation == life.incarnation;
    });
    if (it == snapshot.catalog.entries.end())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Missing Removing fixture catalog life '{}'", life.ns.string());

    CasFoldSeal parent;
    parent.ref_lives.emplace(life.incarnation, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 1}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 1}}});
    if (CasRefCatalog::deleteCompletedRemoving(
            backend, layout, *it, parent, 1,
            [](uint64_t) { return CasRefCatalog::LeaderFenceStatus::Held; })
        != CasRefCatalog::CompletedRemovingDeleteOutcome::Deleted)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to delete fixture catalog life '{}'", life.ns.string());
}

/// Admit a fresh `Live` catalog row at the SAME logical name as `predecessor` -- the same-name
/// rebirth every test below drives. Mirrors `gtest_cas_ns_file_read_contract.cpp`'s
/// `admitReplacementLife`.
NamespaceLifeId admitReplacementLife(
    Backend & backend, const Layout & layout, uint64_t gc_shards,
    const NamespaceLifeId & predecessor, UInt128 successor_incarnation)
{
    if (predecessor.incarnation == successor_incarnation)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Fixture life ids unexpectedly collide");
    const NamespaceLifeId successor = NamespaceLifeId::fromCatalogEntry(predecessor.ns, successor_incarnation);
    CasRefCatalog::casAdmitEntry(backend, layout, gc_shards, CatalogEntry{
        .ns = successor.ns, .state = NsState::Live, .incarnation = successor.incarnation});
    return successor;
}

}

/// This reader's runtime already holds life 1. Reusing it after a same-name rebirth is a retained
/// life-handle operation, not a fresh logical-name admission: it may still answer life 1's committed
/// value (or absent), but it must never surface life 2's -- the opaque physical life id makes the
/// successor's bytes structurally unreachable through an unrefreshed handle.
TEST(CASRefReadContract, HeldRuntimeAfterSameNameRebirthReadsStaleOrNotFoundNeverSuccessorRefs)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// A 1-byte whole-table cache budget is the production knob (`CASRefTableCacheEviction`) that lets a
    /// single store instance both HOLD a table's runtime and, later, genuinely forget it by touching a
    /// different table -- so the "fresh resolution" positive control below is a real re-recovery, not
    /// a second mount.
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .ref_table_cache_bytes = 1});
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/ref_read_contract_rebirth@cas@"};
    const RootNamespace throwaway_ns{"00/ref_read_contract_rebirth_evictor@cas@"};
    const String ref_name = "part_1";

    const ManifestId life1_manifest = publishRefThroughPool(store, ns, ref_name);

    /// Hold the reader runtime resident: one read.
    const auto held_before = store->resolveRef(ns, ref_name);
    ASSERT_TRUE(held_before.has_value());
    EXPECT_EQ(held_before->manifest_id, life1_manifest);
    ASSERT_TRUE(store->refTableLifeForTest(ns).has_value());
    const NamespaceLifeId life1 = *store->refTableLifeForTest(ns);

    /// Drop and re-admit under the SAME logical name, bypassing this store's own ledger entirely --
    /// exactly as an independent actor's drop/rebirth would look from this reader's point of view.
    deleteCatalogLife(*backend, layout, life1);
    const NamespaceLifeId life2 = admitReplacementLife(*backend, layout, store->poolConfig().gc_shards, life1, UInt128{0xabc123});
    ASSERT_NE(life1.incarnation, life2.incarnation);

    const ManifestRef life2_ref{/*writer_epoch*/ 1, /*build_sequence*/ 777, /*manifest_ordinal*/ 1};
    publishCommittedTransition(*backend, layout, ns, ref_name, std::nullopt, life2_ref);
    const ManifestId life2_manifest{ns, life2_ref};
    ASSERT_NE(life2_manifest, life1_manifest);

    /// The held runtime never re-validates the catalog: it answers from its resident cache -- stale or
    /// not-found -- but never the successor's value.
    const auto held_after = store->resolveRef(ns, ref_name);
    EXPECT_NE(
        held_after.has_value() ? std::optional<ManifestId>(held_after->manifest_id) : std::nullopt,
        std::optional<ManifestId>(life2_manifest));
    if (held_after.has_value())
        EXPECT_EQ(held_after->manifest_id, life1_manifest);

    /// Force the cached runtime out: touch a different namespace under the 1-byte cache budget (the
    /// production whole-table eviction path), so the NEXT access to `ns` re-recovers from scratch.
    (void)publishRefThroughPool(store, throwaway_ns, "evict");
    ASSERT_FALSE(store->refTableCachedForTest(ns));

    /// Positive control: a fresh resolution -- through the SAME Pool, now cold -- sees life 2. Not
    /// vacuous: the value really did move, and an unrefreshed handle really would have missed it.
    const auto fresh = store->resolveRef(ns, ref_name);
    ASSERT_TRUE(fresh.has_value());
    EXPECT_EQ(fresh->manifest_id, life2_manifest);
}

/// The disjoint half of the read-side contract: once a table's runtime is resident, an ordinary read
/// costs no catalog request at all -- the recovered-and-cached `RefTableState` is this process's
/// sole authority for a table it has already opened.
TEST(CASRefReadContract, HotRefReadsThroughHeldRuntimeIssueZeroCatalogRequests)
{
    auto backend = std::make_shared<CountingBackend>();
    PoolPtr store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/ref_read_contract_hot@cas@"};
    const String ref_name = "part_1";

    const ManifestId published = publishRefThroughPool(store, ns, ref_name);
    const auto warm = store->resolveRef(ns, ref_name);
    ASSERT_TRUE(warm.has_value());
    EXPECT_EQ(warm->manifest_id, published);
    ASSERT_TRUE(store->refTableLifeForTest(ns).has_value());
    const NamespaceLifeId life = *store->refTableLifeForTest(ns);

    /// Positive control, captured BEFORE the reset below: the cold admission above really did reach
    /// the catalog and this namespace's own ref stream, so the upcoming zero is an absence and not a
    /// recorder that never saw anything.
    EXPECT_GT(
        backend->headCount(layout.refCatalogKey()) + backend->getCount(layout.refCatalogKey())
            + backend->casPutCount(layout.refCatalogKey()),
        0u) << "the cold admission above must have reached the catalog at least once";
    EXPECT_GT(backend->getCount(layout.refCkptKey(life)), 0u)
        << "the cold recovery above must have read this namespace's own checkpoint at least once";

    backend->resetCounts();

    (void)store->resolveRef(ns, ref_name);
    (void)store->listRefs(ns);
    (void)store->hasAnyRefWithPrefix(ns, "");

    EXPECT_EQ(backend->headCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->casPutCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->putCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->putOverwriteCount(layout.refCatalogKey()), 0u);
    /// Stronger than the catalog-only clauses above: a warm ref read is a pure map lookup over the
    /// recovered state (`ensureRefTableRecovered`'s early return once `rt.recovered`), so it issues no
    /// backend request whatsoever, not merely none against the catalog.
    EXPECT_TRUE(backend->touchedKeys().empty())
        << "a warm ref read must issue no backend requests at all";
}

/// The one held ref-WRITER seam the classification found: `dropNamespace(const NamespaceLifeId &)`'s
/// exact-incarnation guard. A stale holder can only be refused, never allowed to act on the successor
/// -- there is no path by which it could target life 2's row or its ref data.
TEST(CASRefReadContract, StaleLifeDropRefusesAfterRebirthAndNeverTouchesSuccessor)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/ref_read_contract_stale_drop@cas@"};
    const String ref_name = "part_1";

    (void)publishRefThroughPool(store, ns, ref_name);
    ASSERT_TRUE(store->refTableLifeForTest(ns).has_value());
    const NamespaceLifeId life1 = *store->refTableLifeForTest(ns);

    deleteCatalogLife(*backend, layout, life1);
    const NamespaceLifeId life2 = admitReplacementLife(*backend, layout, store->poolConfig().gc_shards, life1, UInt128{0xabc456});

    const ManifestRef life2_ref{/*writer_epoch*/ 1, /*build_sequence*/ 999, /*manifest_ordinal*/ 1};
    publishCommittedTransition(*backend, layout, ns, ref_name, std::nullopt, life2_ref);
    const ManifestId life2_manifest{ns, life2_ref};

    const HeadResult catalog_head_before = backend->head(layout.refCatalogKey());
    ASSERT_TRUE(catalog_head_before.exists);
    const auto catalog_get_before = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_get_before.has_value());

    /// The held life-1 handle names an incarnation the catalog no longer carries: refused, not
    /// resolved against the current (life-2) row.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(life1); });

    const HeadResult catalog_head_after = backend->head(layout.refCatalogKey());
    ASSERT_TRUE(catalog_head_after.exists);
    EXPECT_EQ(catalog_head_after.token, catalog_head_before.token)
        << "a refused stale-life drop must not touch the catalog object at all";
    const auto catalog_get_after = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_get_after.has_value());
    EXPECT_EQ(catalog_get_after->bytes, catalog_get_before->bytes);

    /// Life 2's ref data is untouched: a fresh resolution (a separate mount over the same backend,
    /// exactly like `CASRefWriterRuntimeIdentity.ColdReadRejectsReplacementByExternalPoolActor`'s
    /// `external_store`) still sees exactly the value published above.
    auto verify_store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "verify"});
    const auto resolved = verify_store->resolveRef(ns, ref_name);
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(resolved->manifest_id, life2_manifest);
}
