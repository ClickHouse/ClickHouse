#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include "cas_test_helpers.h"
#include <algorithm>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
}

/// Namespace files are keyed by an opaque LIFE, not by its name: `cas/ns/state/<life_id>/_files/<name>`
/// (Stage B Task 4b, directive design change 2). This file pins the three properties that re-key exists
/// to produce, and the one it must NOT produce.
///
/// THE HOLE IT CLOSES. Before the re-key, a namespace file lived at a name-keyed prefix shared by every
/// life of that name. A file the store's LIST omitted therefore survived namespace removal -- nothing
/// enumerated it, so nothing deleted it -- and then became VISIBLE to the next namespace created under
/// the same name, because that namespace read the same prefix. Deletion was load-bearing for
/// correctness, and deletion depends on enumeration, which is the one thing an object store is allowed
/// to be late about (`HintHoleBackendOn` is that lateness as an interface -- see its doc).
///
/// WHY THE KEY IS THE FIX AND THE DELETE IS NOT. After the re-key the old file is at a prefix the new
/// life cannot name. It is unreachable whether or not it was ever deleted, so a blind LIST costs
/// STORAGE and nothing else -- the directive's "LIST omission may only leak storage, never visibility,
/// rebirth or deletion safety". `ColdReaderUsesCatalogCutWhileOldFileSurvivesRemoval` asserts exactly
/// that split by leaving the old object physically present and byte-intact through the real lifecycle.
///
/// WHAT REBIRTH NO LONGER WAITS FOR. Catalog removal depends on folded terminal evidence for the old
/// opaque life, not on a physical-empty proof. `RebirthDoesNotWaitForFilesToBeEmpty` keeps old `_files`
/// bytes present while that evidence is adopted; their later reclamation belongs to the perpetual
/// janitor and is not a precondition for same-name reuse.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

const String kNsString = "00/aa@cas@";
const String kFile = "format_version.txt";

const UInt128 kGcId = hexToU128("00000000000000000000000000000001");

/// Create a real catalog life and a replay-valid `Live` ref table through the production writer path.
void publishWithProductionBirth(const PoolPtr & store, const RootNamespace & ns, const String & ref)
{
    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = store->beginPartWrite(info);
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, ref, id);
    build->promote(ns, ref, build->buildId(), id);
}

}

/// THE COUPLED HEADLINE. A real removal reaches a catalog-absent cut even when LIST permanently omits
/// an old-life file. A cold reader follows that catalog cut rather than the physical residue, while an
/// already-held exact life remains stale-or-NotFound and can never cross into the successor life.
TEST(CASNsFileIncarnation, ColdReaderUsesCatalogCutWhileOldFileSurvivesRemoval)
{
    auto backend = std::make_shared<HintHoleBackendOn<CountingBackend>>();
    PoolPtr store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{kNsString};
    const String old_bytes = "old-life\n";
    const String successor_bytes = "successor-life\n";

    publishWithProductionBirth(store, ns, "predecessor");
    const std::optional<NamespaceLifeId> old_life = store->namespaceFilesLifeIfReadable(ns);
    ASSERT_TRUE(old_life);
    store->putNamespaceFile(*old_life, kFile, old_bytes);
    const String old_key = layout.namespaceFileKey(*old_life, kFile);
    backend->hide(old_key);

    ASSERT_TRUE(backend->head(old_key).exists) << "the lie must be in LIST only -- the object is durable";
    ASSERT_TRUE(store->listNamespaceFiles(*old_life).empty())
        << "precondition: enumeration omits the file, so no cleanup pass can ever find it";
    const size_t holes_before_gc = backend->holesServed();

    store->dropNamespace(ns);
    ASSERT_TRUE(CasRefCatalog::lifeIfCataloged(*backend, layout, ns));

    Gc gc(store, kGcId);
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred) << "N: the production terminal must fold";
    ASSERT_TRUE(CasRefCatalog::lifeIfCataloged(*backend, layout, ns))
        << "the terminal fold alone must not erase its catalog row";
    (void)runRegularRoundReclaiming(gc);
    ASSERT_FALSE(CasRefCatalog::lifeIfCataloged(*backend, layout, ns))
        << "N+1: the pre-fold drain must erase the exact completed Removing row";
    ASSERT_GT(backend->holesServed(), holes_before_gc)
        << "the GC janitor must observe the injected LIST hole after the explicit precondition LIST";

    const auto old_head = backend->head(old_key);
    ASSERT_TRUE(old_head.exists) << "logical removal must not depend on physical empty";
    const auto old_object = backend->get(old_key);
    ASSERT_TRUE(old_object);
    EXPECT_EQ(old_object->bytes, old_bytes);

    PoolPtr cold = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "cold-reader", .gc_fold_max_defer_rounds = 0});
    EXPECT_FALSE(cold->namespaceFilesLifeIfReadable(ns))
        << "a fresh reader follows the absent catalog row, not discoverable or exact-key old bytes";

    publishWithProductionBirth(store, ns, "successor");
    const std::optional<NamespaceLifeId> successor_life = cold->namespaceFilesLifeIfReadable(ns);
    ASSERT_TRUE(successor_life);
    ASSERT_NE(successor_life->incarnation, old_life->incarnation);
    cold->putNamespaceFile(*successor_life, kFile, successor_bytes);
    EXPECT_EQ(cold->getNamespaceFile(*successor_life, kFile), successor_bytes);

    const std::optional<String> retained_old = store->getNamespaceFile(*old_life, kFile);
    EXPECT_TRUE(!retained_old || *retained_old == old_bytes)
        << "an exact predecessor life may be stale or NotFound, but never aliases successor bytes";
    EXPECT_NE(retained_old, std::optional<String>{successor_bytes});

    for (const String & key : backend->touchedKeys())
        EXPECT_EQ(key.find("/_cleanup/"), String::npos) << key;
}

/// The non-minting reader assignment site accepts exactly a catalog `Live` row. `Creating`,
/// `Removing`, and absence neither install a runtime life nor mutate durable catalog/stream state.
TEST(CASNsFileIncarnation, FreshReaderAssignsOnlyLiveCatalogLifeWithoutMutation)
{
    auto backend = std::make_shared<CountingBackend>();
    PoolPtr store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace creating{"00/creating@cas@"};
    const RootNamespace live{"00/live@cas@"};
    const RootNamespace removing{"00/removing@cas@"};
    const RootNamespace absent{"00/absent@cas@"};

    CasRefCatalog::casAdmitEntry(*backend, layout, 1, CatalogEntry{
        .ns = creating,
        .state = NsState::Creating,
        .incarnation = UInt128{31},
        .creator = CreatorFence{.server_root_id = "foreign", .writer_epoch = 7, .fence_generation = 1}});
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, CatalogEntry{
        .ns = live, .state = NsState::Live, .incarnation = UInt128{32}});
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, CatalogEntry{
        .ns = removing, .state = NsState::Live, .incarnation = UInt128{33}});
    CasRefCatalog::casUpdate(*backend, layout, [&](const RefCatalog & current)
    {
        RefCatalog next = current;
        const auto it = std::find_if(next.entries.begin(), next.entries.end(), [&](const CatalogEntry & entry)
        {
            return entry.ns == removing;
        });
        chassert(it != next.entries.end());
        it->state = NsState::Removing;
        it->removal_started_round = 1;
        return next;
    });

    /// Only a `Live` catalog row is readable. Give that exact life the empty checkpoint authority
    /// that production creation publishes; the other rows deliberately remain raw lifecycle states.
    writeRecoverableCkptForRawFixture(*backend, layout, live, RefCkpt{
        .life_epoch = 1,
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    backend->resetCounts();
    EXPECT_FALSE(store->namespaceFilesLifeIfReadable(creating));
    EXPECT_FALSE(store->namespaceFilesLifeIfReadable(removing));
    EXPECT_FALSE(store->namespaceFilesLifeIfReadable(absent));
    const std::optional<NamespaceLifeId> readable = store->namespaceFilesLifeIfReadable(live);
    ASSERT_TRUE(readable);
    EXPECT_EQ(readable->incarnation, UInt128{32});

    EXPECT_FALSE(store->refTableLifeForTest(creating));
    EXPECT_FALSE(store->refTableLifeForTest(removing));
    EXPECT_FALSE(store->refTableLifeForTest(absent));
    ASSERT_TRUE(store->refTableLifeForTest(live));
    EXPECT_EQ(store->refTableLifeForTest(live)->incarnation, UInt128{32});
    EXPECT_EQ(backend->putTotal(), 0u);
    EXPECT_EQ(backend->putOverwriteTotal(), 0u);
    EXPECT_EQ(backend->casPutTotal(), 0u);
}

/// A real GC fold records terminal evidence for the previous life while its namespace-file debris
/// remains physically present. Lifecycle completion is therefore independent of `_files` enumeration;
/// the perpetual janitor may reclaim the bytes later without participating in the removal proof.
TEST(CASNsFileIncarnation, RebirthDoesNotWaitForFilesToBeEmpty)
{
    auto backend = std::make_shared<CountingBackend>();
    PoolPtr store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{kNsString};

    /// A removed namespace (a bare `remove_namespace` transaction -- no committed refs, so no
    /// owner-removal edge confounds this with an unconditional delete path) whose only surviving
    /// physical objects are namespace files: one flat, one nested in the dedup-log shape.
    {
        RefOp remove_op;
        remove_op.kind = RefOpKind::RemoveNamespace;
        appendRefLogSeed(*backend, layout, ns, {remove_op});
    }
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, layout, ns).value();
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
    const String debris_key = layout.namespaceFileKey(life, kFile);
    backend->putIfAbsent(debris_key, "1\n");
    backend->putIfAbsent(layout.namespaceFileKey(life, "deduplication_logs/deduplication_log_1.txt"), "records");

    Gc gc(store, kGcId);
    gc.runRegularRound();

    /// Folding the terminal records positive evidence on the same life row even though files remain.
    const GcState state = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    ASSERT_GT(state.snap_generation, 0u);
    const CasFoldSeal seal = decodeFoldSeal(
        backend->get(layout.foldSealKey(state.snap_generation, state.snap_attempt))->bytes);
    const auto row_it = seal.ref_lives.find(life.incarnation);
    ASSERT_NE(row_it, seal.ref_lives.end());
    ASSERT_TRUE(row_it->second.cleanup_evidence.has_value());
    EXPECT_EQ(row_it->second.cleanup_evidence->remove_txn_id, (RefTxnId{1, 1}));
    EXPECT_TRUE(backend->head(debris_key).exists) << "cleanup evidence does not gate on physical deletion";
}

/// An old-format pool carrying unqualified `roots/<ns>/_files/x` keys is REFUSED AT OPEN. It is not
/// read, not migrated, and not silently re-keyed: the file layer rides Task 4's format bump B, and the
/// pool-open floor is what makes "there is nothing to migrate" true rather than merely intended.
///
/// Asserted at OPEN rather than at the parser on purpose: `Layout` has no unqualified key constructor
/// at all (a compile-time concept check in `gtest_cas_namespace_life_id.cpp` pins that, and
/// `parseNamespaceFileKey`'s refusal of a legacy key is pinned there too), so the only reachable
/// question left is whether a pool that CONTAINS such keys can be opened. It cannot.
TEST(CASNsFileIncarnation, LegacyUnqualifiedFileKeyIsRefusedAtOpen)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");

    /// A generation-5 `_pool_meta`: the current encoder's output with its header generation moved back
    /// one, so every other byte is exactly what that generation really wrote.
    PoolMeta meta;
    meta.pool_id = hexToU128("0123456789abcdef0123456789abcdef");
    meta.blob_header_len = 256;
    meta.min_reader_generation = kNamespaceLifeKeyedGeneration - 1;
    meta.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128)};
    String encoded = encodePoolMeta(meta);
    const String current_v = "\"v\":" + std::to_string(G_BUILD);
    const String legacy_v = "\"v\":" + std::to_string(kNamespaceLifeKeyedGeneration);
    const size_t at = encoded.find(current_v);
    /// Guard the substitution itself: a silent no-op here would leave a CURRENT-generation pool and the
    /// test would pass by opening a pool it believes it downgraded.
    ASSERT_NE(at, String::npos) << "pool-meta header no longer spells its generation as " << current_v;
    encoded.replace(at, current_v.size(), legacy_v);
    ASSERT_NE(encoded.find(legacy_v), String::npos);
    backend->putIfAbsent(layout.poolMetaKey(), encoded);

    /// The legacy artifact this task removes: a namespace file keyed by NAME ONLY, with no incarnation
    /// segment. Written as raw bytes because no code path in the tree can produce this key any more.
    backend->putIfAbsent("p/roots/" + kNsString + "/_files/" + kFile, "1\n");

    try
    {
        openPoolForTest(backend);
        FAIL() << "an old-format pool must fail closed at open, naming recreation";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNKNOWN_FORMAT_VERSION);
        EXPECT_NE(e.message().find("recreate"), String::npos)
            << "the refusal must tell the operator what to do; got: " << e.message();
    }
}
