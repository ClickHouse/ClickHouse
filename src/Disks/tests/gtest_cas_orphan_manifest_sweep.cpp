#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcShardPlan.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Common/Exception.h>
#include "cas_sweep_test_support.h"
#include "cas_test_helpers.h"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{
constexpr uint64_t kWriterEpoch = 7;
const String kServerRoot = "00";
ManifestRef ref(uint64_t seq, uint64_t inst)
{
    return ManifestRef{.writer_epoch = kWriterEpoch, .build_sequence = seq, .manifest_ordinal = static_cast<uint32_t>(inst)};
}

/// The §6 deletion premise (`manifestDeletionPremise`) is a SECOND precondition on every deletion below,
/// alongside the watermark eligibility these tests are about: a manifest of an epoch-`E` build is
/// deletable only once the namespace's sealed fold cursor sits in an epoch strictly above `E`. Tests
/// whose subject is the eligibility or ownership rule therefore have to establish it, or they would
/// assert a deletion the premise (not the rule under test) prevented. Tests whose subject is RETENTION
/// deliberately do NOT call this — see `CASSweepDeletionPremise` for the premise's own coverage.
void seedConsumedSealCursor(InMemoryBackend & backend, const Layout & layout, const RootNamespace & ns)
{
    seedFoldCursorForTest(backend, layout, ns, RefTxnId{kWriterEpoch + 1, 1});
}

/// The catalog cut and `_ckpt` are recovery's sole authority. A fixture that expects a catalog-named
/// life to be swept must establish the same empty, fully readable recovery state a real completed
/// creation would have, rather than relying on the retired sentinel fallback.
void seedEmptyRecoveryAuthority(InMemoryBackend & backend, const Layout & layout, const RootNamespace & ns)
{
    const CasRefCatalog::Snapshot catalog = CasRefCatalog::read(backend, layout);
    const auto entry = std::find_if(catalog.catalog.entries.begin(), catalog.catalog.entries.end(),
        [&](const CatalogEntry & candidate) { return candidate.ns == ns; });
    ASSERT_NE(entry, catalog.catalog.entries.end());
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry->ns, entry->incarnation);
    ASSERT_EQ(backend.putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = std::optional<uint64_t>{kWriterEpoch},
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);
}

/// Replaces the catalog row immediately before its second read after arming. The legacy orphan path
/// reads a catalog cut for coverage, then resolves the name again inside recovery; the second read can
/// splice a successor life into the old coverage decision. An authority-threaded path has no second
/// catalog read, so this seam must remain dormant.
class CatalogChangingOnSecondReadBackend : public InMemoryBackend
{
public:
    using Backend::get;

    void arm(const Layout & layout, CatalogEntry predecessor_, CatalogEntry successor_)
    {
        catalog_key = layout.refCatalogKey();
        predecessor = std::move(predecessor_);
        successor = std::move(successor_);
        catalog_reads = 0;
        armed = true;
    }

    bool didSwitch() const { return did_switch; }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        if (armed && key == catalog_key && ++catalog_reads == 2)
        {
            const auto current = InMemoryBackend::get(key, range);
            if (!current)
                throw std::runtime_error("test catalog disappeared");
            RefCatalog next = decodeRefCatalog(current->bytes);
            const auto it = std::find(next.entries.begin(), next.entries.end(), predecessor);
            if (it == next.entries.end())
                throw std::runtime_error("test predecessor catalog row disappeared");
            *it = successor;
            if (InMemoryBackend::casPut(key, encodeRefCatalog(next), current->token).outcome != CasOutcome::Committed)
                throw std::runtime_error("test catalog replacement conflicted");
            did_switch = true;
        }
        return InMemoryBackend::get(key, range);
    }

private:
    String catalog_key;
    CatalogEntry predecessor;
    CatalogEntry successor;
    uint64_t catalog_reads = 0;
    bool armed = false;
    bool did_switch = false;
};

/// Rewrites a listed manifest after the page captured it but before the page takes its lifecycle cut.
/// The old implementation performed its candidate GET after that cut and would delete this replacement
/// with its new token. The fixed path may nominate the old observation, but exact-token deletion loses.
class ReplacingManifestAfterObservationBackend : public InMemoryBackend
{
public:
    using Backend::get;

    void arm(const Layout & layout, String manifest_key_)
    {
        catalog_key = layout.refCatalogKey();
        manifests_prefix = layout.casManifestsPrefix();
        manifest_key = std::move(manifest_key_);
        listed_page = false;
        replaced_manifest = false;
        armed = true;
    }

    bool didReplace() const { return replaced_manifest; }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        const ListPage page = InMemoryBackend::list(prefix, cursor, limit);
        if (armed && prefix == manifests_prefix)
            listed_page = true;
        return page;
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        const auto result = InMemoryBackend::get(key, range);
        if (armed && listed_page && !replaced_manifest && key == catalog_key)
        {
            const auto current = InMemoryBackend::get(manifest_key);
            if (!current)
                throw std::runtime_error("test manifest disappeared before replacement");
            if (InMemoryBackend::casPut(manifest_key, current->bytes, current->token).outcome != CasOutcome::Committed)
                throw std::runtime_error("test manifest replacement conflicted");
            replaced_manifest = true;
        }
        return result;
    }

private:
    String catalog_key;
    String manifests_prefix;
    String manifest_key;
    bool armed = false;
    bool listed_page = false;
    bool replaced_manifest = false;
};
}

/// A staged-but-unowned body in an ELIGIBLE prefix, absent from the owner view, is deleted (#7).
TEST(CASOrphanManifestSweep, EligibleAndUnownedIsDeleted)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    registerNamespaceRaw(*backend, store->layout(), ns);
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});   // body, no owner
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, /*min_active*/6);   // 6 > 5 => eligible
    seedConsumedSealCursor(*backend, store->layout(), ns);
    seedEmptyRecoveryAuthority(*backend, store->layout(), ns);

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5});
    EXPECT_FALSE(backend->head(store->layout().manifestKey(ManifestId{ns, r})).exists);
}

/// The orphan sweep must not turn a forged same-id snapshot at an OLDER `EpochSeal` into an empty owner
/// view. The base differs from `last_epoch_seal`, so metadata equality cannot catch it; the candidate
/// remains retained until the checkpoint is repaired.
TEST(CASOrphanManifestSweep, CheckpointSnapshotAtOlderEpochSealSkipsDeletion)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/sweep-checkpoint-base-seal@cas@"};
    fixture::admitLive(*backend, layout, ns);
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);

    const RefLogTxn birth{
        .ns = ns.string(), .txn_id = RefTxnId{1, 1}, .ops = {namespaceBirthOp()},
        .prev_epoch_seal = std::nullopt};
    fixture::writeRefLogRaw(*backend, layout, birth);
    RefOp seal;
    seal.kind = RefOpKind::EpochSeal;
    const RefLogTxn seal_txn{
        .ns = ns.string(), .txn_id = RefTxnId{1, 2}, .ops = {seal},
        .prev_epoch_seal = std::nullopt};
    fixture::writeRefLogRaw(*backend, layout, seal_txn);
    RefOp later_seal;
    later_seal.kind = RefOpKind::EpochSeal;
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(), .txn_id = RefTxnId{2, 1}, .ops = {later_seal},
        .prev_epoch_seal = RefTxnId{1, 2}});
    RefTableState through_seal;
    applyRefLogTxn(through_seal, birth);
    applyRefLogTxn(through_seal, seal_txn);
    writeRefSnapshotRaw(*backend, layout, snapshotOf(through_seal, ns.string()));
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 1},
        .checkpoint_snapshot_id = RefTxnId{1, 2},
        .last_epoch_seal = RefTxnId{2, 1}})).outcome, PutOutcome::Done);

    const ManifestRef candidate = ref(5, 0xAC);
    const String candidate_key = layout.manifestKey(ManifestId{ns, candidate});
    writeManifestRaw(*backend, layout, ns, candidate, {blobEntryFor("a", DB::UInt128(1))});
    setWatermarkMinActive(*backend, layout, kServerRoot, kWriterEpoch, /*min_active=*/6);
    seedConsumedSealCursor(*backend, layout, ns);

    std::vector<String> warnings;
    EXPECT_EQ(sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5}, &warnings), 0u);
    EXPECT_TRUE(backend->head(candidate_key).exists);
    ASSERT_FALSE(warnings.empty());
}

/// A body that IS in the owner view (committed) is NEVER swept (#8).
TEST(CASOrphanManifestSweep, OwnedBodyIsSkipped)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);  // now owned
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5});
    EXPECT_TRUE(backend->head(store->layout().manifestKey(ManifestId{ns, r})).exists);
}

/// GC-WEDGE regression (2026-07-10): a COMMITTED ref that has been DROPPED but whose removal `-1` is NOT
/// yet sealed (transition_version above the sealed fold cursor, which is 0 for this fresh pool) must
/// SURVIVE the sweep — the GC fold still needs the body to emit the `-1` (delete-after-sealed-decrements).
/// A promoted build retires its build_seq, so the prefix is watermark-eligible; before the fix the sweep
/// deleted the body in the dropRef→fold window → the removal-fold then clamped FOREVER on the missing
/// committed body → pool-wide GC stop. The pending-removal protection now covers COMMITTED (not only
/// PRECOMMIT) removals.
///
/// SINCE THE §6 PREMISE, this shape is held by TWO independent facts: the tail-removal protection this
/// test is named for, and the premise's rule (1) — the fixture seals no fold cursor, so epoch
/// `kWriterEpoch`'s closing seal is not consumed either. They cannot be separated HERE: the removal log
/// sits in a lower epoch than the build, so any cursor high enough to satisfy rule (1) would also sit
/// above the log and stop the tail scan from reading it at all. The case where the tail-removal
/// protection is the ONLY thing standing — a removal in a LATER epoch, which is the direction removals
/// actually cross — is `CASSweepDeletionPremise.AnUnconsumedTailRemovalRetainsItsTarget`.
TEST(CASOrphanManifestSweep, PendingCommittedRemovalBodyIsSkipped)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);   // committed owner
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);   // dropped: pending committed removal, -1 unsealed
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);   // 6 > 5 => prefix eligible

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5});
    EXPECT_TRUE(backend->head(store->layout().manifestKey(ManifestId{ns, r})).exists)
        << "a dropped-but-unsealed committed manifest body must survive the sweep (delete-after-sealed-"
           "decrements) — else the removal-fold clamps forever on the missing body (GC-WEDGE-2026-07-10)";
}

/// The sweep emits NO blob deltas: the in-degree generation is unchanged.
TEST(CASOrphanManifestSweep, EmitsNoBlobDeltas)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedConsumedSealCursor(*backend, store->layout(), ns);
    seedEmptyRecoveryAuthority(*backend, store->layout(), ns);
    // The sweep must not advance the in-degree generation: capture it AFTER the fixture's own seal.
    const uint64_t gen_before = currentGenerationOf(*backend, store->layout());

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5});
    EXPECT_EQ(currentGenerationOf(*backend, store->layout()), gen_before);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0);
}

TEST(CASOrphanManifestSweep, CursorPageAdvancesAndWrapsWithListBudget)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    registerNamespaceRaw(*backend, store->layout(), ns);
    const ManifestRef r1 = ref(5, 0xE1);
    const ManifestRef r2 = ref(5, 0xE2);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, /*min_active*/6);

    const ManifestSweepResult first = sweepManifestCursorPageForTest(*store, "", /*list_budget*/1, /*delete_budget*/0);
    EXPECT_EQ(first.listed, 1u);
    EXPECT_FALSE(first.wrapped);
    EXPECT_FALSE(first.next_cursor.empty());

    const ManifestSweepResult second = sweepManifestCursorPageForTest(*store, first.next_cursor, /*list_budget*/100, /*delete_budget*/0);
    EXPECT_GE(second.listed, 1u);
    EXPECT_TRUE(second.wrapped);
    EXPECT_TRUE(second.next_cursor.empty());
}

/// A NON-eligible prefix (no watermark fact) deletes NOTHING (#9: frozen-seq is not authority).
TEST(CASOrphanManifestSweep, NoWatermarkIsNotAuthority)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    // No setWatermarkMinActive — no durable fact => not eligible.
    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5});
    EXPECT_TRUE(backend->head(store->layout().manifestKey(ManifestId{ns, r})).exists);
}

TEST(CASOrphanManifestSweep, CursorPageDeletesEligibleUnownedBody)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    registerNamespaceRaw(*backend, store->layout(), ns);
    const ManifestRef r = ref(5, 0xAC);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedConsumedSealCursor(*backend, store->layout(), ns);
    seedEmptyRecoveryAuthority(*backend, store->layout(), ns);

    const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, "", /*list_budget*/100, /*delete_budget*/10);
    EXPECT_GE(result.listed, 1u);
    EXPECT_EQ(result.deleted, 1u);
    EXPECT_FALSE(backend->head(store->layout().manifestKey(ManifestId{ns, r})).exists);
}

TEST(CASOrphanManifestSweep, CursorPageRespectsDeleteBudget)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    registerNamespaceRaw(*backend, store->layout(), ns);
    const ManifestRef r1 = ref(5, 0xAD);
    const ManifestRef r2 = ref(5, 0xAE);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedConsumedSealCursor(*backend, store->layout(), ns);
    seedEmptyRecoveryAuthority(*backend, store->layout(), ns);

    const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, "", /*list_budget*/100, /*delete_budget*/1);
    EXPECT_EQ(result.deleted, 1u);
    const bool first_exists = backend->head(store->layout().manifestKey(ManifestId{ns, r1})).exists;
    const bool second_exists = backend->head(store->layout().manifestKey(ManifestId{ns, r2})).exists;
    EXPECT_NE(first_exists, second_exists);
}

/// A physical manifest captured before a catalog cut which omits its name is dead-life debris: a live
/// creation cannot publish a life-owned object before its catalog row. It therefore has an eventual
/// page-sweep owner without trying to reconstruct a deleted incarnation from the key.
TEST(CASOrphanManifestSweep, CursorPageDeletesObservedBodyWhenCatalogOmitsNamespace)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/catalog-absent-debris@cas@"};
    const ManifestRef r = ref(5, 0xA9);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("debris", DB::UInt128(9))});
    /// No mount lease/watermark exists: after legal catalog-row deletion there may be no server-root
    /// state left to supply one. The post-observation absent row is the complete dead-life proof.

    const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, "", /*list_budget=*/100, /*delete_budget=*/10);

    EXPECT_EQ(result.deleted, 1u);
    EXPECT_FALSE(backend->head(store->layout().manifestKey(ManifestId{ns, r})).exists);
}

/// The candidate body and token must be frozen before the later catalog cut. A concurrent same-key
/// replacement after that observation is a new physical incarnation and must lose the old-token delete.
TEST(CASOrphanManifestSweep, CursorPageCannotDeleteManifestReplacedAfterObservation)
{
    auto backend = std::make_shared<ReplacingManifestAfterObservationBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/replace-after-observation@cas@"};
    registerNamespaceRaw(*backend, store->layout(), ns);
    const ManifestRef r = ref(5, 0xAA);
    const String key = store->layout().manifestKey(ManifestId{ns, r});
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("body", DB::UInt128(10))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedConsumedSealCursor(*backend, store->layout(), ns);
    seedEmptyRecoveryAuthority(*backend, store->layout(), ns);
    backend->arm(store->layout(), key);

    const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, "", /*list_budget=*/100, /*delete_budget=*/10);

    EXPECT_TRUE(backend->didReplace());
    EXPECT_EQ(result.deleted, 0u);
    EXPECT_TRUE(backend->head(key).exists);
}

/// Any duplicate current life id makes the catalog-to-physical join ambiguous. The cursor page is
/// destructive, so the whole cut must be rejected before it can nominate even an unrelated body.
TEST(CASOrphanManifestSweep, CursorPageRefusesAmbiguousCatalogLifeIndex)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/ambiguous-life@cas@"};
    registerNamespaceRaw(*backend, store->layout(), ns);
    const ManifestRef r = ref(5, 0xAB);
    const String key = store->layout().manifestKey(ManifestId{ns, r});
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("body", DB::UInt128(11))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedConsumedSealCursor(*backend, store->layout(), ns);
    seedEmptyRecoveryAuthority(*backend, store->layout(), ns);

    const CasRefCatalog::Snapshot before = CasRefCatalog::read(*backend, store->layout());
    RefCatalog damaged = before.catalog;
    CatalogEntry duplicate = damaged.entries.front();
    duplicate.ns = RootNamespace{"00/ambiguous-life-twin@cas@"};
    damaged.entries.push_back(duplicate);
    std::sort(damaged.entries.begin(), damaged.entries.end(),
        [](const CatalogEntry & lhs, const CatalogEntry & rhs) { return lhs.ns.string() < rhs.ns.string(); });
    ASSERT_EQ(backend->casPut(store->layout().refCatalogKey(), encodeRefCatalog(damaged), before.token).outcome,
              CasOutcome::Committed);

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { sweepManifestCursorPageForTest(*store, "", /*list_budget=*/100, /*delete_budget=*/10); });
    EXPECT_TRUE(backend->head(key).exists);
}

TEST(CASOrphanManifestSweep, CursorPageSkipsOwnedBody)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(5, 0xAF);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);

    const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, "", /*list_budget*/100, /*delete_budget*/10);
    EXPECT_EQ(result.deleted, 0u);
    EXPECT_TRUE(backend->head(store->layout().manifestKey(ManifestId{ns, r})).exists);
}

/// A catalog-named life cannot be treated as an empty table merely because its mandatory recovery
/// checkpoint is missing. The orphan sweep is destructive, so it must retain the body until the
/// caller can recover from the same frozen catalog row and its exact `_ckpt`.
TEST(CASOrphanManifestSweep, MissingRequiredCheckpointSuppressesDestructiveDecision)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/authority-required@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);
    const ManifestRef r = ref(5, 0xB0);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedConsumedSealCursor(*backend, store->layout(), ns);

    const CatalogEntry entry = CasRefCatalog::read(*backend, store->layout()).catalog.entries.front();
    ASSERT_FALSE(readCkpt(*backend, store->layout(), NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation)));

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5});

    EXPECT_TRUE(backend->head(store->layout().manifestKey(ManifestId{ns, r})).exists)
        << "without the exact _ckpt required by a Live catalog row, the sweep must retain rather than "
           "derive an empty owner set";
}

/// A decoded fold cursor at an `EpochSeal` advances to the next GLOBAL writer epoch. Even when this
/// namespace was inactive, every intermediate epoch exists as a chained sequence-1 empty seal, so the
/// exact tail begins at `{E+1, 1}` and must consume each one before reaching a later removal. The
/// removal's target stays protected while an unrelated eligible body remains deletable; retaining both
/// would hide a false missing-log failure at the first intermediate seal.
TEST(CASOrphanManifestSweep, EpochSealFoldCursorCrossesTailByExactDecodedSuccessor)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/seal-cursor-tail@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);
    const CatalogEntry entry = CasRefCatalog::read(*backend, store->layout()).catalog.entries.front();
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation);

    const ManifestRef removed{.writer_epoch = 1, .build_sequence = 5, .manifest_ordinal = 1};
    const ManifestRef unowned{.writer_epoch = 1, .build_sequence = 6, .manifest_ordinal = 1};
    const ManifestRef still_owned{.writer_epoch = 2, .build_sequence = 1, .manifest_ordinal = 1};
    publishAt(*backend, store->layout(), ns, RefTxnId{1, 1}, "dropped", removed.build_sequence, DB::UInt128(0xA1), /*birth=*/true);
    writeSealAt(*backend, store->layout(), ns, RefTxnId{1, 2});
    writeTxnAt(*backend, store->layout(), ns, RefTxnId{2, 1}, publishCommittedOps("still-owned", still_owned), RefTxnId{1, 2});
    writeSealAt(*backend, store->layout(), ns, RefTxnId{2, 2});
    writeSealAt(*backend, store->layout(), ns, RefTxnId{3, 1}, RefTxnId{2, 2});
    writeSealAt(*backend, store->layout(), ns, RefTxnId{4, 1}, RefTxnId{3, 1});
    writeSealAt(*backend, store->layout(), ns, RefTxnId{5, 1}, RefTxnId{4, 1});
    writeSealAt(*backend, store->layout(), ns, RefTxnId{6, 1}, RefTxnId{5, 1});
    for (uint64_t epoch = 3; epoch <= 6; ++epoch)
        ASSERT_TRUE(backend->head(store->layout().refLogKey(life, RefTxnId{epoch, 1})).exists)
            << "fixture must deposit every intermediate exact successor in the catalog life";
    writeTxnAt(*backend, store->layout(), ns, RefTxnId{7, 1},
        {ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, "dropped", removed}, std::nullopt)},
        RefTxnId{6, 1});
    writeSealAt(*backend, store->layout(), ns, RefTxnId{7, 2});
    writeManifestRaw(*backend, store->layout(), ns, unowned, {blobEntryFor("unowned", DB::UInt128(0xA2))});
    ASSERT_EQ(backend->putIfAbsent(store->layout().refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{7, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{7, 2}})).outcome, PutOutcome::Done);
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 7);
    seedFoldCursorForTest(*backend, store->layout(), ns, RefTxnId{2, 2});

    const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, "", /*list_budget=*/100, /*delete_budget=*/10);

    EXPECT_EQ(result.deleted, 1u);
    EXPECT_TRUE(backend->head(store->layout().manifestKey(ManifestId{ns, removed})).exists)
        << "the exact successor of the folded epoch seal contains this body's unconsumed -1";
    EXPECT_FALSE(backend->head(store->layout().manifestKey(ManifestId{ns, unowned})).exists)
        << "an unrelated eligible body must still drain; retaining it would mask a geometry failure";
}

/// A cleaned inherited cursor does not let a later epoch backlink skip the mandatory immediately-next
/// global epoch. `{3,1}` is missing here, so the direct `{7,1} -> {2,2}` link cannot authorize a tail
/// scan; the whole namespace must fail closed and retain even an otherwise unowned eligible body.
TEST(CASOrphanManifestSweep, MissingImmediateEpochAfterCleanedCursorCannotBeSkippedByLaterBacklink)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/missing-next-epoch@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);
    const CatalogEntry entry = CasRefCatalog::read(*backend, store->layout()).catalog.entries.front();
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation);

    const RefTxnId cursor{2, 2};
    writeSealAt(*backend, store->layout(), ns, cursor);
    const HeadResult cursor_head = backend->head(store->layout().refLogKey(life, cursor));
    ASSERT_TRUE(cursor_head.exists);
    ASSERT_EQ(classifyDeleteOutcome(
        backend->deleteExact(store->layout().refLogKey(life, cursor), cursor_head.token)), DeleteClass::Deleted);

    const ManifestRef phantom{.writer_epoch = 7, .build_sequence = 1, .manifest_ordinal = 1};
    /// The codec refuses this skipped predecessor when a writer tries to create it. Inject the malformed
    /// physical lure explicitly: a reader must still not enumerate forward to it when `{3,1}` is absent.
    RefLogTxn direct_later_link{
        .ns = ns.string(),
        .txn_id = RefTxnId{7, 1},
        .ops = publishCommittedOps("phantom", phantom),
        .prev_epoch_seal = RefTxnId{6, 1}};
    String malformed_later_link = encodeRefLogTxn(direct_later_link);
    const String encoded_predecessor{R"("!pse":"6")"};
    const size_t predecessor_pos = malformed_later_link.find(encoded_predecessor);
    ASSERT_NE(predecessor_pos, String::npos);
    malformed_later_link.replace(
        predecessor_pos, encoded_predecessor.size(), R"("!pse":"2")");
    ASSERT_EQ(backend->putIfAbsent(
        store->layout().refLogKey(life, RefTxnId{7, 1}), sealObject(FormatId::RefLog, malformed_later_link)).outcome,
        PutOutcome::Done);
    writeRefSnapshotRaw(*backend, store->layout(), RefTableSnapshot{
        .ns = ns.string(),
        .snapshot_id = RefTxnId{7, 1},
        .committed = {committedRow("phantom", phantom)},
        .precommits = {}});
    ASSERT_EQ(backend->putIfAbsent(store->layout().refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{7, 1},
        .checkpoint_snapshot_id = RefTxnId{7, 1},
        .last_epoch_seal = RefTxnId{6, 1}})).outcome, PutOutcome::Done);

    const ManifestRef victim{.writer_epoch = 1, .build_sequence = 5, .manifest_ordinal = 1};
    writeManifestRaw(*backend, store->layout(), ns, victim, {blobEntryFor("victim", DB::UInt128(0xC1))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedFoldCursorForTest(*backend, store->layout(), ns, cursor);

    const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, "", /*list_budget=*/100, /*delete_budget=*/10);

    EXPECT_EQ(result.deleted, 0u);
    EXPECT_TRUE(backend->head(store->layout().manifestKey(ManifestId{ns, victim})).exists);
}

/// Control for the cleaned-cursor path: the exact immediately-next epoch head exists and names the
/// deleted seal, so the tail is readable. Its `-1` protects the removed body while an unrelated eligible
/// body proves the namespace was scanned rather than retained wholesale. The checkpoint base is the
/// following same-epoch transaction: recovery therefore has a retained exact anchor without turning the
/// deliberately cleaned predecessor seal into part of that anchor's proof.
TEST(CASOrphanManifestSweep, CleanedCursorCrossesOnlyThroughExactImmediateEpochHead)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/exact-next-epoch@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);
    const CatalogEntry entry = CasRefCatalog::read(*backend, store->layout()).catalog.entries.front();
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation);

    const RefTxnId cursor{2, 2};
    writeSealAt(*backend, store->layout(), ns, cursor);
    const HeadResult cursor_head = backend->head(store->layout().refLogKey(life, cursor));
    ASSERT_TRUE(cursor_head.exists);
    ASSERT_EQ(classifyDeleteOutcome(
        backend->deleteExact(store->layout().refLogKey(life, cursor), cursor_head.token)), DeleteClass::Deleted);

    const ManifestRef removed{.writer_epoch = 1, .build_sequence = 5, .manifest_ordinal = 1};
    writeTxnAt(*backend, store->layout(), ns, RefTxnId{3, 1},
        {ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, "removed", removed}, std::nullopt)}, cursor);
    const ManifestRef absent_anchor{.writer_epoch = 1, .build_sequence = 7, .manifest_ordinal = 1};
    writeTxnAt(*backend, store->layout(), ns, RefTxnId{3, 2},
        {ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, "absent-anchor", absent_anchor}, std::nullopt)});
    writeRefSnapshotRaw(*backend, store->layout(), RefTableSnapshot{
        .ns = ns.string(), .snapshot_id = RefTxnId{3, 2}, .committed = {}, .precommits = {}});
    ASSERT_EQ(backend->putIfAbsent(store->layout().refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{3, 2},
        .checkpoint_snapshot_id = RefTxnId{3, 2},
        .last_epoch_seal = cursor})).outcome, PutOutcome::Done);

    const ManifestRef unowned{.writer_epoch = 1, .build_sequence = 6, .manifest_ordinal = 1};
    writeManifestRaw(*backend, store->layout(), ns, removed, {blobEntryFor("removed", DB::UInt128(0xC2))});
    writeManifestRaw(*backend, store->layout(), ns, unowned, {blobEntryFor("unowned", DB::UInt128(0xC3))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 7);
    seedFoldCursorForTest(*backend, store->layout(), ns, cursor);

    const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, "", /*list_budget=*/100, /*delete_budget=*/10);

    EXPECT_EQ(result.deleted, 1u);
    EXPECT_TRUE(backend->head(store->layout().manifestKey(ManifestId{ns, removed})).exists);
    EXPECT_FALSE(backend->head(store->layout().manifestKey(ManifestId{ns, unowned})).exists);
}

/// The catalog row used to obtain coverage and the life used to recover ownership must be ONE frozen
/// authority cut. A later catalog row for the same name may not make the old life's committed manifest
/// look orphaned and therefore deletable.
TEST(CASOrphanManifestSweep, LaterCatalogCutCannotSpliceOwnershipAuthority)
{
    auto backend = std::make_shared<CatalogChangingOnSecondReadBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/frozen-catalog-cut@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);
    const CatalogEntry predecessor = CasRefCatalog::read(*backend, store->layout()).catalog.entries.front();

    const ManifestRef r = ref(5, 0xB1);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    ASSERT_EQ(publishCommittedTransition(*backend, store->layout(), ns, "live", std::nullopt, r), 1u);
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedConsumedSealCursor(*backend, store->layout(), ns);

    CatalogEntry successor = predecessor;
    successor.incarnation = DB::UInt128(0xBEEF);
    backend->arm(store->layout(), predecessor, successor);

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5});

    EXPECT_FALSE(backend->didSwitch())
        << "the sweep must not resolve a second catalog cut after it starts using the frozen entry";
    EXPECT_TRUE(backend->head(store->layout().manifestKey(ManifestId{ns, r})).exists)
        << "the committed predecessor manifest must remain protected by the same frozen authority cut";
}

/// The LIST-based late-log detector that lived here is RETIRED with the sentinel seal, and it is worth
/// recording why rather than leaving a hole in this file's coverage story.
///
/// It existed because the old seal was a SNAPSHOT at a synthetic `{E-1, UINT64_MAX}` id: that object
/// occupied no `_log` key, so a dying predecessor's in-flight PUT could still land in the dead epoch and
/// the only possible response was to notice it afterwards and report it. INV-2's seal is a
/// TRANSACTION at exactly `{E, T+1}` -- the key that ghost would take -- so the store's own write-once
/// create refuses it. There is nothing left to detect at that shape: an id above the seal cannot be
/// minted either, because ids are state-derived and a writer that could derive `{E, T+2}` would have had
/// to observe the seal first.
///
/// `CasEventType::RefLateLogDetected` is retired WITH the detector -- pre-release, so a vocabulary entry
/// nothing can emit is just dead surface. Soak scenario S38 (`s38_late_put_injection.py`) keeps its
/// injection and FLIPS its assertion: from "the detection fired" to "the fence held" -- the late PUT's
/// conditional create must LOSE to the occupied slot, with zero data loss and the namespace folding
/// normally.
