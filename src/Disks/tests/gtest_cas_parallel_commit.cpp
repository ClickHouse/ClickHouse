#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/tests/cas_test_helpers.h>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <atomic>
#include <filesystem>

/// Task 2 of the CAS parallel-write-path plan (docs/superpowers/sdd): `promoteBuild`/`repointRef`
/// return an exact, in-lane-derived `Cas::CommitOutcome` instead of `void`/`bool`, and
/// `dropRefIfMatches` gives a future rollback a conditional drop keyed on that exact outcome instead
/// of the unsafe-under-concurrency `dropRef` (which removes whatever manifest currently occupies the
/// ref name). This suite grows across the later parallel-commit tasks; here it only proves the
/// outcome is exact and that the conditional drop is a true guard -- still single-threaded commit, no
/// concurrency yet.
///
/// Task 3 reworks `ContentAddressedTransaction::commit()`'s rollback to be EXACT (per-part
/// `Cas::CommitOutcome` slots + `dropRefIfMatches`) while the commit loop stays single-threaded --
/// correctness-first, before Task 5 adds concurrency. `CasCommitRollback` below drives real
/// `ContentAddressedTransaction`s (not the bare pool primitives `CaWiringFixture` above exercises)
/// through the exact `publishStaging` call path production `commit()` uses, so the fault seams
/// (`armPromoteFailure`/`armAfterPromoteHook`) fire from the real thing.

using namespace DB;
using namespace DB::Cas::tests;

namespace
{

/// Fixture mirroring `gtest_cas_part_folder_access.cpp`'s `publishPart`/`cacheOn` helpers: a fresh
/// in-memory pool + a `CachedPartFolderAccess` facade over it, plus the minimal staging helpers this
/// suite's tests need (stage a simple one-file part without promoting it; stage-and-promote it in one
/// call; repoint an already-committed ref onto a fresh manifest, modeling a later writer).
struct CaWiringFixture
{
    std::shared_ptr<Cas::InMemoryBackend> backend = std::make_shared<Cas::InMemoryBackend>();
    Cas::PoolPtr store = openPoolForTest(backend);
    Cas::CachedPartFolderAccess access{store};
    Cas::RootNamespace namespace_{"srv/t1"};
    int content_counter = 0;

    const Cas::RootNamespace & ns() const { return namespace_; }
    Cas::CachedPartFolderAccess & partAccess() { return access; }

    static Cas::ManifestEntry inlineEntry(const String & path, const String & bytes)
    {
        Cas::ManifestEntry e;
        e.path = path;
        e.placement = Cas::EntryPlacement::Inline;
        e.ref = Cas::BlobRef{Cas::BlobHashAlgo::CityHash128, Cas::BlobDigest::fromU128(u128Of(bytes))};
        e.blob_size = bytes.size();
        e.inline_bytes = bytes;
        return e;
    }

    struct Staged
    {
        Cas::PartWriteTxnPtr build;
        Cas::ManifestId id;
    };

    /// Stages a fresh build (manifest + precommit) for `key` over `blobs` inline entries, WITHOUT
    /// promoting it -- the caller drives `promoteBuild` itself so it can observe the exact
    /// `CommitOutcome` the promote primitive derives.
    Staged stageSimplePart(const Cas::PartRefKey & key, int blobs) const
    {
        std::vector<Cas::ManifestEntry> entries;
        for (int i = 0; i < blobs; ++i)
            entries.push_back(inlineEntry(fmt::format("f{}", i), fmt::format("payload-{}-{}", key.ref, i)));
        auto build = store->beginPartWrite(Cas::PartWriteInfo{
            .intended_ref = key.ns.string() + "/" + key.ref, .intended_namespace = key.ns, .op = Cas::ProvenanceOp::Insert});
        const Cas::ManifestId id = build->stageManifest(entries);
        build->precommitAdd(key.ns, key.ref, id);
        return {std::move(build), id};
    }

    /// Stages and promotes one simple part end-to-end, returning the exact `CommitOutcome`.
    Cas::CommitOutcome commitSimplePart(const Cas::PartRefKey & key, int blobs)
    {
        auto staged = stageSimplePart(key, blobs);
        return access.promoteBuild(*staged.build, key, staged.build->buildId(), staged.id);
    }

    /// Repoints an already-committed `key` onto a fresh manifest (different content), through the
    /// public `repointRef` primitive -- models "another writer" rebinding the ref after this
    /// fixture's own `commitSimplePart`.
    Cas::CommitOutcome repointToFreshManifest(const Cas::PartRefKey & key)
    {
        return access.repointRef(key, {inlineEntry("f0", fmt::format("repoint-{}", ++content_counter))},
            Cas::ProvenanceOp::Other);
    }
};

}

TEST(CASCommitOutcome, PromoteReportsCreatedAndManifest)
{
    CaWiringFixture fx;
    const Cas::PartRefKey key{fx.ns(), "20260101_1_1_0"};
    auto staged = fx.stageSimplePart(key, /*blobs=*/1);

    const Cas::CommitOutcome oc = fx.partAccess().promoteBuild(*staged.build, key, staged.build->buildId(), staged.id);

    EXPECT_TRUE(oc.created);
    EXPECT_EQ(oc.ns.string(), key.ns.string());
    EXPECT_EQ(oc.ref, key.ref);
    EXPECT_EQ(oc.manifest_ref, staged.id.ref);
}

TEST(CASCommitOutcome, DropRefIfMatchesRemovesOnlyExact)
{
    CaWiringFixture fx;
    const Cas::PartRefKey key{fx.ns(), "20260101_2_2_0"};
    const Cas::CommitOutcome oc1 = fx.commitSimplePart(key, /*blobs=*/1);
    EXPECT_TRUE(oc1.created);

    /// Rebind key -> M2 (a legitimate repoint by "another writer").
    const Cas::CommitOutcome oc2 = fx.repointToFreshManifest(key);
    EXPECT_FALSE(oc2.created);
    ASSERT_NE(oc1.manifest_ref, oc2.manifest_ref);

    /// Conditional drop keyed on the STALE M1 must NOT remove the current M2 binding.
    EXPECT_FALSE(fx.partAccess().dropRefIfMatches(key, oc1.manifest_ref));
    EXPECT_TRUE(fx.partAccess().existsRef(key, Cas::Freshness::ForceFresh));

    /// Conditional drop keyed on the CURRENT M2 removes it.
    EXPECT_TRUE(fx.partAccess().dropRefIfMatches(key, oc2.manifest_ref));
    EXPECT_FALSE(fx.partAccess().existsRef(key, Cas::Freshness::ForceFresh));
}

TEST(CASCommitOutcome, DropRefIfMatchesOnAbsentRefIsANoOp)
{
    CaWiringFixture fx;
    const Cas::PartRefKey key{fx.ns(), "20260101_3_3_0"};
    Cas::ManifestRef bogus;
    EXPECT_FALSE(fx.partAccess().dropRefIfMatches(key, bogus)) << "no committed ref at all: nothing to match";
    EXPECT_FALSE(fx.partAccess().existsRef(key, Cas::Freshness::ForceFresh));
}

/// `repointRef`'s byte-equal candidate is a documented ZERO-pool-mutation no-op (it must not mint a
/// fresh manifest just to compare it). The returned `CommitOutcome` must still describe reality: the
/// CURRENTLY committed manifest, unchanged, `created=false`.
TEST(CASCommitOutcome, RepointRefByteEqualNoOpReportsCurrentManifestNotCreated)
{
    CaWiringFixture fx;
    const Cas::PartRefKey key{fx.ns(), "20260101_4_4_0"};
    const Cas::CommitOutcome oc1 = fx.commitSimplePart(key, /*blobs=*/1);

    const Cas::CommitOutcome oc_noop = fx.partAccess().repointRef(
        key, {CaWiringFixture::inlineEntry("f0", fmt::format("payload-{}-0", key.ref))}, Cas::ProvenanceOp::Other);
    EXPECT_FALSE(oc_noop.created);
    EXPECT_EQ(oc_noop.manifest_ref, oc1.manifest_ref);
}

namespace
{

/// Fixture for the `CasCommitRollback` suite: wraps a real `ContentAddressedMetadataStorage` and
/// drives ordinary `ContentAddressedTransaction`s through disk paths, so the fault seams under test
/// (`ContentAddressedMetadataStorage::armPromoteFailureForTest`/`setAfterPromoteHookForTest`, the
/// minimal test-only hooks this task adds) fire from the SAME `publishStaging` call path production
/// `commit()` uses -- unlike `CaWiringFixture` above, which pokes the bare pool primitives directly.
/// Every part in one fixture instance shares ONE fixed table uuid (and therefore one `RootNamespace`),
/// matching every test's single `fx.ns()`.
struct CaTxnRollbackFixture
{
    static constexpr const char * kTableUuid = "c3c3c3c3-0000-4000-8000-c3c3c3c3c3c3";

    std::shared_ptr<DB::ContentAddressedMetadataStorage> storage;
    Cas::RootNamespace namespace_;
    Cas::ManifestRef last_repoint_manifest;
    int content_counter = 0;

    static std::string tablePrefix()
    {
        return std::string(kTableUuid).substr(0, 3) + "/" + kTableUuid;
    }

    const Cas::RootNamespace & ns() const { return namespace_; }
    Cas::CachedPartFolderAccess & partAccess() const { return *storage->partAccess(); }

    DB::MetadataTransactionPtr beginTxn() const { return storage->createTransaction(); }

    /// Stages `blobs` small distinct files for `key` under a tmp build dir and re-keys them to the
    /// final ref name -- the standard MergeTree-insert shape (`gtest_ca_transaction.cpp`'s
    /// `writeFileTx` + `moveDirectory` idiom) this storage's routing expects; `key.ns` must be `ns()`.
    void stageInto(const DB::MetadataTransactionPtr & txn, const Cas::PartRefKey & key, int blobs)
    {
        auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*txn);
        const std::string tmp_dir = tablePrefix() + "/tmp_insert_" + key.ref;
        for (int i = 0; i < blobs; ++i)
        {
            auto buf = ca_tx.writeFile(fmt::format("{}/f{}.bin", tmp_dir, i), 65536, DB::WriteMode::Rewrite, {});
            const std::string bytes = fmt::format("payload-{}-{}", key.ref, i);
            buf->write(bytes.data(), bytes.size());
            buf->finalize();
        }
        txn->moveDirectory(tmp_dir, tablePrefix() + "/" + key.ref);
    }

    /// Stages and commits one part end-to-end in its own transaction -- sets up a pre-existing
    /// committed ref before the transaction under test begins.
    void commitSimplePart(const Cas::PartRefKey & key, int blobs)
    {
        auto txn = beginTxn();
        stageInto(txn, key, blobs);
        txn->commit(DB::NoCommitOptions{});
    }

    /// Repoints an already-committed `key` onto a fresh manifest through the public `repointRef`
    /// primitive directly -- models "another writer" rebinding the ref concurrently with the
    /// transaction under test. Records the manifest for `lastRepointManifest()`.
    void repointToFreshManifest(const Cas::PartRefKey & key)
    {
        const std::string bytes = fmt::format("repoint-{}", ++content_counter);
        Cas::ManifestEntry e;
        e.path = "f0.bin";
        e.placement = Cas::EntryPlacement::Inline;
        e.ref = Cas::BlobRef{Cas::BlobHashAlgo::CityHash128, Cas::BlobDigest::fromU128(u128Of(bytes))};
        e.blob_size = bytes.size();
        e.inline_bytes = bytes;
        const auto oc = partAccess().repointRef(key, {e}, Cas::ProvenanceOp::Other);
        last_repoint_manifest = oc.manifest_ref;
    }

    /// The manifest CURRENTLY bound to `key`, or a default-constructed (zero) `ManifestRef` when `key`
    /// has no committed ref at all.
    Cas::ManifestRef currentManifest(const Cas::PartRefKey & key) const
    {
        auto view = partAccess().getView(key, Cas::Freshness::ForceFresh);
        return view ? view->manifestId().ref : Cas::ManifestRef{};
    }

    Cas::ManifestRef lastRepointManifest() const { return last_repoint_manifest; }

    /// Test-only fault seam (see `ContentAddressedMetadataStorage::armPromoteFailureForTest`): the
    /// NEXT `publishStaging` promote/repoint for `key` (the full `(ns, ref)` routed identity) throws
    /// instead of committing.
    void armPromoteFailure(const Cas::PartRefKey & key) const { storage->armPromoteFailureForTest(key); }
    /// Test-only hook (see `ContentAddressedMetadataStorage::setAfterPromoteHookForTest`): runs once,
    /// synchronously, immediately after `key`'s promote/repoint confirms.
    void armAfterPromoteHook(const Cas::PartRefKey & key, std::function<void()> hook) const
    {
        storage->setAfterPromoteHookForTest(key, std::move(hook));
    }
};

CaTxnRollbackFixture makeCaWiringFixture()
{
    static std::atomic<uint64_t> counter{0};
    const auto scratch = std::filesystem::temp_directory_path()
        / fmt::format("ca_commit_rollback_scratch_{}_{}", ::getpid(), counter.fetch_add(1));
    auto settings = DB::Cas::tests::makeSettingsForTest("test", scratch);
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();

    CaTxnRollbackFixture fx;
    fx.storage = storage;
    fx.namespace_ = storage->liveNamespace(CaTxnRollbackFixture::kTableUuid);
    return fx;
}

}

/// [TXN-ONE-PIPELINE] Task 3: `commit()` publishes `new_a` (created=true) then fails on `new_b`'s
/// promote. The rollback must drop the just-created `new_a` (absent afterward) but never touch the
/// unrelated `pre_existing` ref committed by an EARLIER, already-finished transaction.
TEST(CASCommitRollback, AbsentBeforeDroppedPreExistingUntouched)
{
    auto fx = makeCaWiringFixture();
    const Cas::PartRefKey pre{fx.ns(), "pre_existing_1_1_0"};
    fx.commitSimplePart(pre, 1);                                  // a pre-existing ref, must survive
    // A transaction that commits one NEW part then fails on a second part's promote.
    auto txn = fx.beginTxn();
    fx.stageInto(txn, {fx.ns(), "new_a_1_1_0"}, 1);
    fx.stageInto(txn, {fx.ns(), "new_b_1_1_0"}, 1);
    fx.armPromoteFailure({fx.ns(), "new_b_1_1_0"});               // fault injection in publishStaging's promote
    EXPECT_ANY_THROW(txn->commit({}));
    EXPECT_FALSE(fx.partAccess().existsRef({fx.ns(), "new_a_1_1_0"}, Cas::Freshness::ForceFresh)); // rolled back
    EXPECT_TRUE (fx.partAccess().existsRef(pre, Cas::Freshness::ForceFresh));                       // untouched
}

/// [TXN-ONE-PIPELINE] Task 3: T1 (this transaction) promotes `shared` (M1), then a concurrent writer
/// (modeled by the after-promote hook) repoints it to M2 BEFORE T1's own commit later fails on
/// `poison`'s promote. Rollback must use `dropRefIfMatches(M1)`: M1 != the now-current M2, so the
/// conditional drop must leave `shared` bound to M2 untouched.
///
/// `commit()` publishes `parts` in the map's own (ns, ref) sort order -- so the "shared" part is named
/// `a_shared_...` and the "poison" part `z_poison_...` here purely so `'a' < 'z'` makes "shared"
/// publish (and get repointed by the hook) deterministically BEFORE "poison" fails; this is a test
/// naming choice, not a production ordering guarantee.
TEST(CASCommitRollback, RepointByOtherWriterSurvivesRollback)
{
    auto fx = makeCaWiringFixture();
    const Cas::PartRefKey key{fx.ns(), "a_shared_1_1_0"};
    auto txn = fx.beginTxn();
    fx.stageInto(txn, key, 1);                                   // T1 will create R -> M1
    fx.armAfterPromoteHook(key, [&]{ fx.repointToFreshManifest(key); }); // T2 repoints R -> M2 right after T1's promote
    fx.stageInto(txn, {fx.ns(), "z_poison_1_1_0"}, 1);
    fx.armPromoteFailure({fx.ns(), "z_poison_1_1_0"});
    EXPECT_ANY_THROW(txn->commit({}));
    // T1's rollback used dropRefIfMatches(M1); M2 != M1 so it must survive.
    EXPECT_TRUE(fx.partAccess().existsRef(key, Cas::Freshness::ForceFresh));
    EXPECT_EQ(fx.currentManifest(key), fx.lastRepointManifest());
}
