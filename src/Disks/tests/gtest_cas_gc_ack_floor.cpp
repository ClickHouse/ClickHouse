#include <gtest/gtest.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Common/ProfileEvents.h>
#include "cas_test_helpers.h"
#include "config.h"

namespace DB::ErrorCodes
{
extern const int ABORTED;
}

namespace ProfileEvents
{
extern const Event CASMetaDelete;
extern const Event CASGCCondemnMarkerUnconfirmedCarry;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{
const UInt128 kGc = hexToU128("00000000000000000000000000000001");
ManifestRef ref(const String &, uint64_t seq, uint64_t inst)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = static_cast<uint32_t>(inst)};
}
/// A one-shot `create`, asserting it committed (mirrors the retired `backend.putIfAbsent(key, bytes)`).
void createObj(Backend & backend, const String & key, const String & bytes)
{
    OperationForTest op(backend);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(key, bytes, Retry::once())));
}

/// An exact read (mirrors the retired `backend.get(key)`).
std::optional<Object> readObj(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).read(key, Retry::standard());
}

/// A HEAD (mirrors the retired `backend.head(key)`).
std::optional<Meta> headObj(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).head(key, Retry::standard());
}

bool blobExists(InMemoryBackend & b, const Layout & layout, const UInt128 & hash)
{
    return headObj(b, layout.blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hash)})).has_value();
}

/// Publish one physical blob through the production durable-precommit ordering. The committed fixture
/// ref keeps the transaction complete; callers that need an initially unowned body drop that ref.
PutBlobResult publishBlobWithDurablePrecommit(
    const PoolPtr & store, const RootNamespace & ns, const String & ref,
    const BlobRef & blob_ref, const String & payload)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = store->beginPartWrite(info);
    ManifestEntry entry;
    entry.path = "data.bin";
    entry.placement = EntryPlacement::Blob;
    entry.ref = blob_ref;
    entry.blob_size = payload.size();
    const ManifestId id = build->stageManifest({entry});
    build->precommitAdd(ns, ref, id);
    const PutBlobResult result = build->putBlob(blob_ref, BlobSource::fromString(payload));
    build->promote(ns, ref, build->buildId(), id);
    return result;
}

/// The current retired entry for `hash` (dereferenced through gc/state.retired_refs, shard 0), or nullopt.
std::optional<RetiredEntry> currentEntryFor(Backend & backend, const Layout & layout, const UInt128 & hash)
{
    for (const RetiredEntry & e : currentRetiredSet(backend, layout, /*shard*/0))
        if (e.ref == BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hash)})
            return e;
    return std::nullopt;
}

/// Counts every conditional removal sent against `watched_key` while that key is ALREADY absent.
/// A store may answer such a removal with a precondition failure rather than a clean miss (rustfs does,
/// observed 2026-07-11), so a caller that sends one cannot tell "somebody replaced it" from "it is
/// gone". The GC redelete site is not allowed to send one: it observes the blob first and compares the
/// condemned incarnation against what it saw.
class AbsentRemovalWatchBackend : public InMemoryBackend
{
public:
    void watch(const String & key) { watched_key = key; }
    size_t removalsAgainstAbsent() const { return removals_against_absent; }

    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        if (key == watched_key && !InMemoryBackend::head(key, access))
            ++removals_against_absent;
        return InMemoryBackend::remove(key, expected_value, access);
    }

private:
    String watched_key;
    size_t removals_against_absent = 0;
};

class CkptReplacementConflictBackend : public InMemoryBackend
{
public:
    std::expected<String, RawConflict> write(
        const String & key, const String & bytes, const std::optional<String> & expected_value,
        TransportAccess & access) override
    {
        /// Only the CONDITIONAL shape is refused: the fixture's own creation of the object must land.
        if (conflict_once && expected_value && key == watched_key)
        {
            conflict_once = false;
            return std::unexpected(RawConflict{});
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

    String watched_key;
    bool conflict_once = false;
};
}

TEST(CASSemanticRefFixture, WrapperCreatesInitialRecoverableCheckpoint)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/semantic-create@cas@"};
    const ManifestRef manifest = ref("srv-a:1", 1, 0xAB);

    const uint64_t sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, manifest);
    const RefTxnId expected_id{manifest.writer_epoch, sequence};
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    const auto ckpt = readCkpt(op, store->layout(), life);

    ASSERT_TRUE(ckpt.has_value());
    EXPECT_EQ(ckpt->ckpt.life_epoch, 1);
    EXPECT_EQ(ckpt->ckpt.committed_through, expected_id);
    EXPECT_FALSE(ckpt->ckpt.checkpoint_snapshot_id.has_value());
    EXPECT_FALSE(ckpt->ckpt.last_epoch_seal.has_value());
}

TEST(CASSemanticRefFixture, WrapperAdvancesCheckpointWithoutDiscardingSnapshot)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/semantic-advance@cas@"};
    const ManifestRef manifest = ref("srv-a:1", 1, 0xAC);

    const uint64_t publish_sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, manifest);
    const RefTxnId publish_id{manifest.writer_epoch, publish_sequence};
    writeRefSnapshotRaw(*backend, store->layout(), minimalLiveSnapshot(ns.string(), publish_id, {committedRow("tbl", manifest)}));
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    const auto before_drop = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(before_drop.has_value());
    RefCkpt with_snapshot = before_drop->ckpt;
    with_snapshot.checkpoint_snapshot_id = publish_id;
    ASSERT_TRUE(std::holds_alternative<Committed>(op.replace(
        store->layout().refCkptKey(life), encodeRefCkpt(with_snapshot), before_drop->etag,
        Retry::once())));

    const uint64_t drop_sequence = dropRefTransition(*backend, store->layout(), ns, "tbl", manifest);
    const RefTxnId drop_id{manifest.writer_epoch, drop_sequence};
    const auto ckpt = readCkpt(op, store->layout(), life);

    ASSERT_TRUE(ckpt.has_value());
    EXPECT_EQ(ckpt->ckpt.committed_through, drop_id);
    EXPECT_EQ(ckpt->ckpt.checkpoint_snapshot_id, publish_id);
    EXPECT_FALSE(ckpt->ckpt.last_epoch_seal.has_value());
}

TEST(CASSemanticRefFixture, CheckpointAdvanceRejectsNonMonotoneAndInvalidState)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/semantic-refusal@cas@"};
    const ManifestRef manifest = ref("srv-a:1", 1, 0xAD);

    const uint64_t sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, manifest);
    const RefTxnId id{manifest.writer_epoch, sequence};
    EXPECT_THROW(advanceRecoverableCkptForRawFixture(*backend, store->layout(), ns, id), DB::Exception);

    const RootNamespace invalid_ns{"00/semantic-invalid@cas@"};
    fixture::admitLive(*backend, store->layout(), invalid_ns);
    const NamespaceLifeId invalid_life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), invalid_ns);
    const String invalid_key = store->layout().refCkptKey(invalid_life);
    createObj(*backend, invalid_key, "not a checkpoint");
    EXPECT_THROW(advanceRecoverableCkptForRawFixture(*backend, store->layout(), invalid_ns, id), DB::Exception);
    EXPECT_EQ(readObj(*backend, invalid_key)->bytes, "not a checkpoint");
}

TEST(CASRawRefFixture, RawLogWriteDoesNotCreateCheckpoint)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/raw-no-ckpt@cas@"};
    const RefTxnId id{1, 1};

    fixture::writeRefLogRaw(*backend, store->layout(), RefLogTxn{
        .ns = ns.string(),
        .txn_id = id,
        .ops = {namespaceBirthOp()},
        .prev_epoch_seal = std::nullopt,
    });

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    EXPECT_FALSE(readCkpt(op, store->layout(), life).has_value());
}

TEST(CASRawRefFixture, ReplaceRecoverableCheckpointWritesTheSuppliedFullState)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/replace-ckpt@cas@"};
    const ManifestRef manifest = ref("srv-a:1", 1, 0xAE);
    const RefTxnId first_id{manifest.writer_epoch,
        publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, manifest)};
    const RefTxnId seal_id{first_id.writer_epoch, first_id.ref_sequence + 1};
    writeRefSnapshotRaw(*backend, store->layout(), minimalLiveSnapshot(ns.string(), first_id, {committedRow("tbl", manifest)}));
    writeSealAt(*backend, store->layout(), ns, seal_id);

    const RefCkpt next{
        .life_epoch = 1,
        .committed_through = seal_id,
        .checkpoint_snapshot_id = first_id,
        .last_epoch_seal = seal_id,
    };
    replaceRecoverableCkptForRawFixture(*backend, store->layout(), ns, next);

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    const auto replaced = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(replaced.has_value());
    EXPECT_EQ(replaced->ckpt.life_epoch, next.life_epoch);
    EXPECT_EQ(replaced->ckpt.committed_through, next.committed_through);
    EXPECT_EQ(replaced->ckpt.checkpoint_snapshot_id, next.checkpoint_snapshot_id);
    EXPECT_EQ(replaced->ckpt.last_epoch_seal, next.last_epoch_seal);
}

TEST(CASRawRefFixture, ReplaceRecoverableCheckpointRejectsStaleRegressiveAndWrongLife)
{
    auto backend = std::make_shared<CkptReplacementConflictBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/replace-ckpt-refusal@cas@"};
    const ManifestRef manifest = ref("srv-a:1", 1, 0xAF);
    const RefTxnId id{manifest.writer_epoch,
        publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, manifest)};
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    const auto existing = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(existing.has_value());

    RefCkpt wrong_life = existing->ckpt;
    wrong_life.life_epoch = *wrong_life.life_epoch + 1;
    wrong_life.committed_through = RefTxnId{*wrong_life.life_epoch, 1};
    EXPECT_THROW(replaceRecoverableCkptForRawFixture(*backend, store->layout(), ns, wrong_life), DB::Exception);

    RefCkpt regressive = existing->ckpt;
    regressive.committed_through = std::nullopt;
    EXPECT_THROW(replaceRecoverableCkptForRawFixture(*backend, store->layout(), ns, regressive), DB::Exception);

    backend->watched_key = store->layout().refCkptKey(life);
    backend->conflict_once = true;
    EXPECT_THROW(replaceRecoverableCkptForRawFixture(*backend, store->layout(), ns, existing->ckpt), DB::Exception);
    EXPECT_EQ(readCkpt(op, store->layout(), life)->ckpt.committed_through, id);
}

/// The owner-removed manifest body is deleted only after a full round (its decrement is sealed — #11).
TEST(CASGCRetire, ManifestBodyDeletedAfterDecrementsSealed)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    EXPECT_TRUE(headObj(*backend, store->layout().manifestKey(ManifestId{ns, r})).has_value());

    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    runRegularRoundReclaiming(gc);
    EXPECT_FALSE(headObj(*backend, store->layout().manifestKey(ManifestId{ns, r})).has_value());
}

/// A publish racing the pass (in-degree restored) is SPARED, not deleted (#14).
TEST(CASGCRecheck, PublishRacingFenceSparesBlob)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref("srv-a:1", 1, 0xA1);
    const ManifestRef r2 = ref("srv-a:1", 2, 0xA2);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);
    Gc gc(store, kGc);
    gc.runRegularRound();
    // Repoint the ref from r1 to r2 (both reference blob 1) in the same window before the next round
    // folds. ONE repoint event {old=committed(r1), new=committed(r2)} — the -1 (r1's body) and +1
    // (r2's body) net to in-degree 1, so blob 1 is re-pinned and must be SPARED. (Not a separate drop
    // THEN repoint — that would double-count the -1 on r1's body and over-delete.)
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", r1, r2);
    gc.runRegularRound();   // net in-degree 1 => spared
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)));
}

/// A genuinely unreferenced blob is deleted with its exact token (the single content-delete site). The
/// delete is not one-round-after-drop: the entry condemns, graduates the round AFTER the condemning
/// round (round-paced, unconditional), then the NEXT pass executes the exact-token delete.
TEST(CASGCRecheck, UnreferencedBlobDeletedExactToken)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    gc.runRegularRound();
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    // The drop's -1 condemns blob 1; the retired-cursor pipeline (condemn -> graduate -> delete) reclaims it.
    EXPECT_TRUE(runRoundsUntilAbsent(store, gc, *backend, store->layout(), DB::UInt128(1)));
    EXPECT_FALSE(headObj(*backend, store->layout().manifestKey(ManifestId{ns, r})).has_value());
}

/// Task 5 (spec 2026-07-09 §raw-body-refinement, v3): GC writes the writer's freshness meta ALONGSIDE
/// the unchanged ledger retire (RetiredEntry, body token) — the meta is the writer/promote gate's
/// point-read signal (Task 3/4), not a replacement for the ledger or the exact-token body delete.
TEST(CASGCRetire, CondemnWritesMetaCondemned)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    gc.runRegularRound();   /// +1 folds; blob referenced (`writeBlobBody` never wrote a meta itself)
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    gc.runRegularRound();   /// -1 folds => in-degree 0 => condemned THIS round

    const auto lm = loadMetaForTest(*backend, store->layout(), DB::UInt128(1));
    ASSERT_TRUE(lm.has_value()) << "GC must write the freshness meta at condemn time (Task 5)";
    EXPECT_EQ(lm->meta.state, MetaState::Condemned);
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1))) << "condemned, NOT yet deleted";
}

/// Task 5: the round's exact-token body delete drops the meta alongside it (advisory, no tombstone —
/// an absent meta reads exactly like a Clean one for the writer's point-read gate).
TEST(CASGCRetire, DeleteRemovesBodyAndMeta)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    gc.runRegularRound();
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    /// §0 introspection: the meta drop below rides `deleteMetaExact` (`CASMetaDelete` choke point).
    const auto delete_before = ProfileEvents::global_counters[ProfileEvents::CASMetaDelete].load();
    // condemn -> graduate (round-paced) -> delete (the retired-cursor pipeline).
    ASSERT_TRUE(runRoundsUntilAbsent(store, gc, *backend, store->layout(), DB::UInt128(1)));

    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1))) << "body gone via exact-token delete";
    EXPECT_FALSE(loadMetaForTest(*backend, store->layout(), DB::UInt128(1)).has_value())
        << "the meta must be dropped alongside the exact-token body delete (Task 5)";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMetaDelete].load() - delete_before, 1);
}

/// GC freshness meta is ADD-ONLY (spec 2026-07-11 deposed-leader `clearSparedMeta` fix): an entry whose
/// in-degree recovers before graduation is SPARED (unchanged ledger behavior) but GC must NEVER flip its
/// meta `Condemned -> Clean` on the spare. A deposed leader that cleared-then-lost the round would leave a
/// stray-`Clean` over a still-condemned body; a writer reading `Clean` would reuse the exact condemned
/// token, which a stale exact-token redelete then deletes (INV_NO_LOSS live-blob loss).
/// The spare leaves the meta `Condemned`;
/// ONLY a writer that displaces the body with a fresh incarnation token publishes `Clean`.
TEST(CASGCRetire, SpareLeavesMetaCondemned)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};

    /// A content-addressed body + Clean meta via a real fresh upload, so a later writer dedup-attempt
    /// resolves to THIS exact hash (GC condemns it; the writer republishes it). Drop the fixture ref
    /// immediately so only the raw owner transitions below govern its in-degree.
    const String payload = "spare-add-only-payload";
    const UInt128 hash = u128Of(payload);
    const BlobRef id = idOf(payload);
    const RootNamespace seed_ns{"00/spare-seed@cas@"};
    publishBlobWithDurablePrecommit(store, seed_ns, "seed", id, payload);
    store->dropRef(seed_ns, "seed");
    store->renewWatermarkOnce();
    const Etag t_seed = headObj(*backend, store->layout().blobKey(id))->etag;

    const ManifestRef r1 = ref("srv-a:1", 1, 0xA1);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", hash)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);
    Gc gc(store, kGc);
    gc.runRegularRound();   /// +1 folds
    dropRefTransition(*backend, store->layout(), ns, "tbl", r1);
    gc.runRegularRound();   /// -1 folds => in-degree 0 => condemned; meta flipped Condemned
    ASSERT_TRUE(currentEntryFor(*backend, store->layout(), hash).has_value());
    {
        const auto lm = loadMetaForTest(*backend, store->layout(), hash);
        ASSERT_TRUE(lm.has_value());
        ASSERT_EQ(lm->meta.state, MetaState::Condemned);
    }

    /// Re-reference the SAME blob (same body/token — never re-uploaded) via a fresh ref before
    /// graduation. The pass merge nets in-degree back to 1: the prior retired entry is SPARED
    /// (recovery wins, even past the floor) -- not the republication-supersede path (the token never changed).
    const ManifestRef r2 = ref("srv-a:1", 2, 0xA2);
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("a", hash)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl_detached", std::nullopt, r2);
    gc.runRegularRound();   /// +1 folds => spared

    EXPECT_FALSE(currentEntryFor(*backend, store->layout(), hash).has_value())
        << "the spared entry drops from the retired set";
    EXPECT_TRUE(blobExists(*backend, store->layout(), hash));
    EXPECT_EQ(headObj(*backend, store->layout().blobKey(id))->etag, t_seed)
        << "spare does not touch the body — the incarnation token is unchanged";

    /// ADD-ONLY: the spare must NOT clear the meta back to Clean (that is the deposed-leader hole).
    {
        const auto lm = loadMetaForTest(*backend, store->layout(), hash);
        ASSERT_TRUE(lm.has_value());
        EXPECT_EQ(lm->meta.state, MetaState::Condemned)
            << "GC freshness meta is add-only: a spare leaves the meta Condemned (never -> Clean)";
    }

    /// Only a WRITER re-publishes Clean, and only by displacing the body with a fresh incarnation token:
    /// a materialization attempt on the condemned hash republishes the writer's source — the body token CHANGES and
    /// the meta flips to Clean WITH that token change.
    const RootNamespace writer_ns{"00/spare-writer@cas@"};
    auto ref_w = publishBlobWithDurablePrecommit(store, writer_ns, "writer", id, payload);
    EXPECT_EQ(ref_w.ref, id);
    const Etag t_resurrect = headObj(*backend, store->layout().blobKey(id))->etag;
    EXPECT_NE(t_resurrect, t_seed) << "republication displaces the body with a fresh incarnation token";
    const auto lm_after = loadMetaForTest(*backend, store->layout(), hash);
    ASSERT_TRUE(lm_after.has_value());
    EXPECT_EQ(lm_after->meta.state, MetaState::Clean)
        << "the writer's republication path is the sole Condemned -> Clean transition";
}

/// Two-leader stale-redelete regression — the executable form of the deposed-leader spec §2. A stale
/// leader's pre-CAS redelete of the incarnation `t1` must never delete a live reuse. With the buggy
/// clear-on-spare, a spare publishes `Clean`; a writer reads `Clean` and REUSES `t1`; the stale redelete
/// then deletes the LIVE body (INV_NO_LOSS). Add-only meta closes it: the spare leaves `Condemned`, the
/// writer resurrects to `t2`, and the stale redelete finds a different incarnation and sends nothing.
///
/// Interleaving fidelity (APPROXIMATED): the deposed leader's destructive side effect is its pre-CAS
/// redelete of `t1`. We reproduce it deterministically by CAPTURING `t1` at condemn time (exactly the
/// incarnation a paused leader's `delete_pending` snapshot holds) and replaying the round's own
/// observe-compare-remove sequence AFTER the surviving leader's spare and the writer's republication --
/// the faithful destructive step, without a mid-round interrupt seam on the delete path.
///
/// The replay really SENDS its removal when the comparison passes, and the removal counter below is
/// what makes the closing fsck mean something: on the clear-on-spare regression the spare publishes
/// `Clean`, the writer's dedup hit keeps `t1`, the comparison passes, the live body is removed and
/// both the counter and the fsck dangle count move.
TEST(CASGCRetire, StaleRedeleteAfterSpareDoesNotDeleteLiveReuse)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};

    const String payload = "two-leader-stale-redelete-payload";
    const UInt128 hash = u128Of(payload);
    const BlobRef id = idOf(payload);
    const String blob_key = store->layout().blobKey(id);
    const RootNamespace seed_ns{"00/redelete-seed@cas@"};
    publishBlobWithDurablePrecommit(store, seed_ns, "seed", id, payload);
    store->dropRef(seed_ns, "seed");
    store->renewWatermarkOnce();

    const ManifestRef r1 = ref("srv-a:1", 1, 0xA1);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", hash)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);
    Gc gc(store, kGc);
    gc.runRegularRound();   /// +1 folds
    dropRefTransition(*backend, store->layout(), ns, "tbl", r1);
    gc.runRegularRound();   /// -1 => in-degree 0 => condemned at t1

    /// The OLD leader L1's planned pre-CAS delete uses the EXACT token it observed at condemn: capture t1.
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const auto condemned_entry = currentEntryFor(*backend, store->layout(), hash);
    ASSERT_TRUE(condemned_entry.has_value());
    const PersistedEtag t1 = condemned_entry->token;
    const std::optional<Meta> at_condemn = op.head(blob_key, Retry::once());
    ASSERT_TRUE(at_condemn);
    ASSERT_TRUE(t1.matches(at_condemn->etag));

    /// A NEW leader L2 folds a +1 that recovered h's in-degree and adopts a SPARE for h.
    const ManifestRef r2 = ref("srv-a:1", 2, 0xA2);
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("a", hash)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl_live", std::nullopt, r2);
    gc.runRegularRound();   /// +1 => spared

    /// Add-only: the spare left the meta Condemned (the stale-redelete guard depends on it).
    {
        const auto lm = loadMetaForTest(*backend, store->layout(), hash);
        ASSERT_TRUE(lm.has_value());
        EXPECT_EQ(lm->meta.state, MetaState::Condemned)
            << "add-only: a spare must not clear the meta (the writer must still see the hash condemned)";
    }

    /// A writer dedup-hits h. It point-reads Condemned and RESURRECTS to a fresh token t2
    /// from the writer's own source — it never reuses t1.
    const RootNamespace writer_ns{"00/redelete-writer@cas@"};
    publishBlobWithDurablePrecommit(store, writer_ns, "writer", id, payload);
    const std::optional<Meta> t2 = op.head(blob_key, Retry::once());
    ASSERT_TRUE(t2);
    EXPECT_FALSE(t1.matches(t2->etag))
        << "the writer resurrected to a fresh incarnation, not a reuse of t1";

    /// L1 resumes and replays its stale pre-CAS redelete exactly as the round performs one: observe the
    /// key, compare the condemned incarnation against what is there, and remove ONLY on a match. The
    /// removal is genuinely attempted on a match, so this step is destructive whenever the writer
    /// reused `t1`.
    const uint64_t removals_before = backend->deleteCount(blob_key);
    const std::optional<Meta> stale = op.head(blob_key, Retry::once());
    ASSERT_TRUE(stale);
    EXPECT_FALSE(t1.matches(stale->etag))
        << "the stale redelete must miss the live reuse (add-only closes INV_NO_LOSS)";
    if (t1.matches(stale->etag))
        (void)op.remove(blob_key, stale->etag, Retry::once());
    EXPECT_EQ(backend->deleteCount(blob_key), removals_before)
        << "the comparison failed, so the redelete sent no removal at all against the live body";

    /// The live body under t2 survives, stays reachable via the committed r2, and fsck sees no dangle.
    const std::optional<Meta> survivor = op.head(blob_key, Retry::once());
    ASSERT_TRUE(survivor);
    EXPECT_EQ(survivor->etag, t2->etag);
    replaceRecoverableCkptForRawFixture(
        *backend, store->layout(), ns,
        RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 3},
                .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});
    EXPECT_EQ(runFsck(*store, /*detail=*/false).dangling, 0u)
        << "no live reference dangles: the stale redelete did not delete the reused body";
}

/// Copy-forward aftermath, republished arm (spec 2026-07-02-cas-copy-forward-condemned-evidence.md):
/// after a condemned incarnation (hash, t0) is displaced by a verified copy-forward (fresh token t1)
/// and the republished part's +1 lands, the listed (hash, t0) entry settles WITHOUT touching the new
/// incarnation: its exact-token delete is a mismatch no-op and the entry drops; the blob survives at t1.
TEST(CASGCRetire, CopyForwardedBlobSurvivesWhenRepublished)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref("srv-a:1", 1, 0xA1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);
    Gc gc(store, kGc);
    gc.runRegularRound();
    dropRefTransition(*backend, store->layout(), ns, "tbl", r1);
    gc.runRegularRound();   /// -1 folds => in-degree 0 => entry (1, t0) condemned
    ASSERT_TRUE(currentEntryFor(*backend, store->layout(), DB::UInt128(1)).has_value());

    /// The raw equivalent of writer republication: displace exactly t0 with the
    /// same verified bytes under a fresh token t1, then republish a part referencing the blob (the
    /// promoted dst ref of a republishRef move).
    const String blob_key = store->layout().blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(DB::UInt128(1))});
    OperationForTest displace(*backend);
    const Etag t0 = (*displace).head(blob_key, Retry::standard())->etag;
    const WriteResult res = (*displace).replace(blob_key, readObj(*backend, blob_key)->bytes, t0, Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(res));
    const Etag res_etag = std::get<Committed>(res).etag;
    const ManifestRef r2 = ref("srv-a:1", 2, 0xA2);
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl_detached", std::nullopt, r2);

    /// The +1 folds => spared; the (1, t0) entry drops; the t1 incarnation is never deleted.
    for (int i = 0; i < 4; ++i)
        gc.runRegularRound();
    EXPECT_FALSE(currentEntryFor(*backend, store->layout(), DB::UInt128(1)).has_value());
    const auto hr = (*displace).head(blob_key, Retry::standard());
    ASSERT_TRUE(hr.has_value());
    EXPECT_EQ(hr->etag, res_etag);
}

/// Copy-forward aftermath, stale-entry arm: a listed (hash, t0) entry whose incarnation was
/// displaced (token now t1) with NO accompanying owner events. The entry graduates and its
/// exact-token delete MISMATCHES — a no-op, the entry drops, the t1 incarnation is NEVER
/// wrong-token-deleted (no wedge, no unsafe delete). This is a RAW-displacement model, stronger
/// than the real flow: in real `republishRef` the dst precommit + body are durable BEFORE the
/// promote pre-pass runs (reachability-before-content, B188), so an abandoned real copy-forward
/// is fully reclaimed by the pipeline (+1 spare -> reclaim -1 -> transition to zero -> fresh
/// (hash, t1) entry -> exact delete). The raw shape pins the GC-side invariant in isolation.
TEST(CASGCRetire, AbandonedCopyForwardDropsEntryWithoutWrongTokenDelete)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref("srv-a:1", 1, 0xA1);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);
    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    dropRefTransition(*backend, store->layout(), ns, "tbl", r1);
    runRegularRoundReclaiming(gc);
    ASSERT_TRUE(currentEntryFor(*backend, store->layout(), DB::UInt128(1)).has_value());

    const String blob_key = store->layout().blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(DB::UInt128(1))});
    OperationForTest displace(*backend);
    const Etag t0 = (*displace).head(blob_key, Retry::standard())->etag;
    const WriteResult res = (*displace).replace(blob_key, readObj(*backend, blob_key)->bytes, t0, Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(res));
    const Etag res_etag = std::get<Committed>(res).etag;

    /// No events land at all (raw displacement). Drive rounds with the store's ack kept current so
    /// the (1, t0) entry graduates; its exact-token delete mismatches t1 and the entry drops.
    for (int i = 0; i < 6; ++i)
    {
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
    }
    EXPECT_FALSE(currentEntryFor(*backend, store->layout(), DB::UInt128(1)).has_value())
        << "the stale (hash, t0) entry must settle (mismatch redelete drops it), not wedge the list";
    const auto hr = (*displace).head(blob_key, Retry::standard());
    ASSERT_TRUE(hr.has_value()) << "the fresh incarnation must never be deleted under the stale token";
    EXPECT_EQ(hr->etag, res_etag);
}

/// A completed round adopts the SAME attempt its fold minted (the round's single gc/state CAS commits the
/// fold's (snap_generation, snap_attempt) together). Completion seals are a retired concept, so the durable
/// index of the adopted round is the FOLD seal at (snap_generation, snap_attempt). Across rounds each
/// `runRegularRound` re-acquires the lease (bumping `lease.seq`), so a later round mints a FRESH attempt.
TEST(CASGCRecheck, CompletionInheritsFoldAttempt)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);

    gc.runRegularRound();          // round 1: one pass, single CAS commits (snap_generation, snap_attempt)
    const auto after_round1 = decodeGcState(readObj(*backend, store->layout().gcStateKey())->bytes);
    // The round adopted the attempt of THIS round's fold: snap_attempt == the lease.seq that folded it.
    EXPECT_EQ(after_round1.snap_attempt, after_round1.lease.seq);
    EXPECT_GT(after_round1.snap_generation, 0u);
    // The fold seal is durable under the adopted (snap_generation, snap_attempt) pair (no completion seal).
    EXPECT_TRUE(headObj(*backend, store->layout()
        .foldSealKey(after_round1.snap_generation, after_round1.snap_attempt)).has_value());

    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    gc.runRegularRound();          // round 2: re-acquire (bump lease.seq) -> fresh attempt at its fold
    const auto after_round2 = decodeGcState(readObj(*backend, store->layout().gcStateKey())->bytes);
    EXPECT_EQ(after_round2.snap_attempt, after_round2.lease.seq);
    EXPECT_GT(after_round2.snap_attempt, after_round1.snap_attempt);   // per-round monotone attempt
    EXPECT_GT(after_round2.snap_generation, after_round1.snap_generation);
    EXPECT_TRUE(headObj(*backend, store->layout()
        .foldSealKey(after_round2.snap_generation, after_round2.snap_attempt)).has_value());
}

/// ---- round-paced graduation suite (spec 2026-07-02 + Task-9 amendment; re-keyed off acks in v3 Task 6) ----

/// A regular round performs NO writes to the ref objects: ref state is writer-owned (immutable
/// `_log`/`_snap`), and GC only reads it (plus deletes covered objects via ref-object cleanup, which
/// needs a covering snapshot -- none exists here). So a no-op round adds and removes NO ref object.
TEST(CASGCAckFloor, NoOpRoundDoesNotMutateRefShards)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt,
        ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1});
    Gc gc(store, kGc);
    gc.runRegularRound();   // first round folds the publish

    const auto listRefKeys = [&]
    {
        std::set<String> keys;
        String cursor;
        OperationForTest op(*backend);
        for (;;)
        {
            const ListPage page = (*op).list(store->layout().namespaceStreamPrefix(fixture::fixtureLife(ns)), cursor, 1000, Retry::standard());
            for (const ListedKey & lk : page.keys)
                keys.insert(lk.key);
            if (page.next_cursor.empty())
                break;
            cursor = page.next_cursor;
        }
        return keys;
    };

    const std::set<String> before = listRefKeys();
    ASSERT_FALSE(before.empty()) << "the publish must have written at least one ref object";

    gc.runRegularRound();   // a second, no-op round must not add or remove any ref object
    const std::set<String> after = listRefKeys();
    EXPECT_EQ(before, after) << "a no-op GC round must not mutate the table's ref objects";
    // The registry object is gone (Task 4); the fence never existed to write it.
    EXPECT_FALSE(readObj(*backend, "p/gc/registry").has_value());
}

/// The canonical pipeline: a blob condemned at round K stays present after the condemning round; the
/// VERY NEXT round graduates it (round-paced, unconditional — condemn_round < current_round the first
/// round current_round exceeds it) and publishes it delete_pending — the blob still exists; the round
/// AFTER THAT executes the exact-token delete and the blob becomes absent. This pins the critical
/// off-by-one: current_round MUST equal state.round + 1 (the SAME basis condemn_round is stamped at),
/// so an entry graduates exactly one round after it was condemned — never the same round, never never.
TEST(CASGCAckFloor, CondemnThenGraduatesNextRoundThenDeletes)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);

    runRegularRoundReclaiming(gc);                 // round 1: folds the +1; blob referenced
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);

    // The condemning round: the -1 drops in-degree to 0; the blob is condemned into the current retired
    // list but NOT deleted. The entry is present and NOT yet pending. report.condemned counts it.
    {
        const RoundReport rep = runRegularRoundReclaiming(gc);
        EXPECT_EQ(rep.condemned, 1u);        // one blob condemned this round
        EXPECT_EQ(rep.graduated, 0u);        // must NOT graduate the same round it was condemned
        EXPECT_EQ(rep.redeleted, 0u);        // nothing pending to delete yet
        EXPECT_TRUE(blobExists(*backend, store->layout(), blob));
        const auto e = currentEntryFor(*backend, store->layout(), blob);
        ASSERT_TRUE(e.has_value());
        EXPECT_FALSE(e->delete_pending);   // condemned, not yet graduated
    }

    // The VERY NEXT round graduates it deterministically (no ack/heartbeat dependency).
    {
        const RoundReport rep = runRegularRoundReclaiming(gc);
        EXPECT_EQ(rep.graduated, 1u);
        EXPECT_EQ(rep.redeleted, 0u);   // the delete lands on the NEXT pass, not this one
        const auto e = currentEntryFor(*backend, store->layout(), blob);
        ASSERT_TRUE(e.has_value());
        EXPECT_TRUE(e->delete_pending);
        EXPECT_TRUE(blobExists(*backend, store->layout(), blob));   // pending: still present this pass
    }

    // The pass AFTER the pending publish executes the exact-token delete; the blob becomes absent and the
    // entry is dropped from the current retired list. report.redeleted counts the executed pending delete.
    {
        const RoundReport rep = runRegularRoundReclaiming(gc);
        EXPECT_EQ(rep.redeleted, 1u);        // the pending delete executed this round
        EXPECT_FALSE(blobExists(*backend, store->layout(), blob));
        EXPECT_FALSE(currentEntryFor(*backend, store->layout(), blob).has_value());
    }
}

/// End-to-end through the real round driver (`Gc::runRegularRound`) rather than
/// `foldDeltasIntoGeneration` directly: a cohort well past `gc_round_redelete_budget` still drains
/// completely, but no single round's `redeleted` count exceeds the cap — the same convergence the
/// merge-level `CASThreeCursorMerge` budget tests pin, proven through the production entry point.
TEST(CASGCAckFloor, RedeleteBudgetCapsRoundDrainAndConverges)
{
    auto backend = std::make_shared<InMemoryBackend>();
    constexpr uint64_t kCap = 5;
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_round_redelete_budget = kCap});
    const RootNamespace ns{"00/aa@cas@"};

    constexpr uint64_t kCohort = 20;
    std::vector<UInt128> blobs;
    std::vector<ManifestRef> refs;
    for (uint64_t i = 1; i <= kCohort; ++i)
    {
        const UInt128 blob(i);
        const ManifestRef r = ref("srv-a:1", i, static_cast<uint32_t>(i));
        blobs.push_back(blob);
        refs.push_back(r);
        writeBlobBody(*backend, store->layout(), blob);
        writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
        publishCommittedTransition(*backend, store->layout(), ns, "tbl" + std::to_string(i), std::nullopt, r);
    }

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);   // round 1: folds every publish, every blob referenced

    for (uint64_t i = 1; i <= kCohort; ++i)
        dropRefTransition(*backend, store->layout(), ns, "tbl" + std::to_string(i), refs[i - 1]);

    {
        const RoundReport rep = runRegularRoundReclaiming(gc);   // condemning round
        EXPECT_EQ(rep.condemned, kCohort);
        EXPECT_EQ(rep.graduated, 0u);
        EXPECT_EQ(rep.redeleted, 0u);
    }
    {
        // Graduation has no budget configured in this test (default 0 = unbounded) — the whole
        // cohort graduates together, isolating the redelete cap as the only thing under test.
        const RoundReport rep = runRegularRoundReclaiming(gc);
        EXPECT_EQ(rep.graduated, kCohort);
        EXPECT_EQ(rep.redeleted, 0u);
    }

    uint64_t total_redeleted = 0;
    uint64_t rounds = 0;
    while (total_redeleted < kCohort && rounds < 10)
    {
        const RoundReport rep = runRegularRoundReclaiming(gc);
        EXPECT_LE(rep.redeleted, kCap) << "a round must never redelete past gc_round_redelete_budget";
        total_redeleted += rep.redeleted;
        ++rounds;
    }
    EXPECT_EQ(total_redeleted, kCohort) << "no entry lost to the cap across the whole drain";
    EXPECT_EQ(rounds, kCohort / kCap) << "ceil(20 / 5) rounds to fully drain the cohort";
    for (const UInt128 & blob : blobs)
        EXPECT_FALSE(blobExists(*backend, store->layout(), blob));
}

/// A publish re-referencing the condemned blob before graduation is folded and SPARES the entry: the entry
/// is dropped (recovery wins even past graduation) and the blob survives.
TEST(CASGCAckFloor, PublishBeforeGraduationSpares)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref("srv-a:1", 1, 0xA1);
    const ManifestRef r2 = ref("srv-a:1", 2, 0xA2);
    const UInt128 blob = DB::UInt128(1);
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);
    Gc gc(store, kGc);

    gc.runRegularRound();
    store->renewWatermarkOnce();
    dropRefTransition(*backend, store->layout(), ns, "tbl", r1);
    gc.runRegularRound();   // condemns blob 1 (in-degree 0)
    store->renewWatermarkOnce();
    ASSERT_TRUE(currentEntryFor(*backend, store->layout(), blob).has_value());

    // Re-publish a committed ref pointing at the same blob BEFORE it graduates: the next pass folds the +1,
    // the merge sees in-degree 1, and the entry is spared (dropped from the retired list).
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r2);
    gc.runRegularRound();
    store->renewWatermarkOnce();
    EXPECT_FALSE(currentEntryFor(*backend, store->layout(), blob).has_value());   // spared: entry dropped
    EXPECT_TRUE(blobExists(*backend, store->layout(), blob));

    // Keep running: the re-referenced blob must never be deleted.
    for (int i = 0; i < 4; ++i)
    {
        gc.runRegularRound();
        store->renewWatermarkOnce();
    }
    EXPECT_TRUE(blobExists(*backend, store->layout(), blob));
}

namespace
{

/// Shared body for the fence-out timing invariant: opens a pool from `config`, then drives the exact
/// two-round scenario described below. Parameterized only by `config` so the same scenario can be run
/// against the default `PoolConfig` (`ExpiredMountFencedOutAndExcluded`) and against
/// `unsafe_remount_no_delay = true` (`CASGcFenceOut.ThresholdUnchangedByUnsafeKnob`), proving the knob
/// changes nothing about the fence-out threshold or its round count. `events` is heap-owned (not a
/// plain local) because the Pool CAN outlive the function that opened it: a background publish can hold
/// an extra `shared_from_this()` past this function's return, so a stack-local sink target -- even one
/// declared before the Pool (the fix for the 2026-07-09 ASan finding) -- is not enough.
///
/// A dead mount is fenced out by the round's heartbeat step: gc_fenced is set on its body (a
/// token-guarded rewrite that bumps seq). The fence is pure liveness (re-arms the write fence so a
/// resumed sleeper can never mutate again); reclaim itself no longer depends on any mount's heartbeat —
/// graduation paces on GC rounds. The fenced mount's own subsequent renew then fails closed, because the
/// fence invalidated the token it held.
///
/// Rev.6 §token-stability observation (Task 9): the fence-out no longer trusts a bare wall-clock stamp
/// (`expires_at_ms`) against the GC's own clock — it fences ONLY once GC has watched a mount's write
/// token hold unchanged for the full threshold on its OWN monotonic clock. That takes (at least) two
/// `computeHeartbeatFloor` calls spanning the threshold, so this test drives the GC leader's own
/// (persistent) `mono_ms_fn` across three rounds: round 1 seeds the observation for both mounts; the
/// STORE's own mount is then renewed (as a live leader would); round 2, one millisecond short of the
/// threshold, discriminates the threshold's exact value (must NOT fence yet); round 3, exactly at the
/// threshold, is where srid2 — never renewed again after its one-shot claim — gets fenced.
void runExpiredMountFenceOutScenario(const PoolConfig & config)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// Heap-owned, not a plain local: declaring it before the Pool (ASan 2026-07-09) only protects
    /// against an ordinary same-thread unwind, not a detached background completion holding an extra
    /// `shared_from_this()` that can still be running on another thread after this frame returns.
    auto events = std::make_shared<DB::Cas::tests::SharedEventLog>();
    auto store = Pool::open(backend, config);
    const Layout & layout = store->layout();

    // srid2's renewer claims ONE lease via `start` and is never renewed again — tests never enable
    // the runtime-owned renewal worker (`background_watermark` defaults to false), so this alone models a
    // crashed process: a body that is live-shaped (not terminated, not fenced) but whose write token
    // never changes again.
    const String srid2 = "stale-server";
    CasRequests renewer_requests = openRequestsForTest(backend);
    MountLeaseRenewer srid2_renewer(renewer_requests, renewer_requests, layout, srid2, DB::UInt128(0x2222),
        /*writer_epoch=*/1,
        std::chrono::milliseconds(100), [] { return 1000u; }, [] { return 0u; }, {},
        std::chrono::milliseconds(0), [] { return 0u; });
    srid2_renewer.start();
    ASSERT_FALSE(decodeMountLease(readObj(*backend, layout.mountKey(srid2))->bytes).gc_fenced);

    // The fence-out threshold on the GC leader's OWN monotonic clock — mirrors the production formula
    // in `Gc::runRegularRound` (ttl + 5% drift allowance + one round's worth of renewal slack).
    const uint64_t ttl_ms = static_cast<uint64_t>(store->poolConfig().mount_lease_ttl_ms.count());
    const uint64_t threshold_ms = ttl_ms + ttl_ms / 20
        + static_cast<uint64_t>(store->poolConfig().mount_renew_period.count());

    uint64_t gc_now = 1'000'000;   // audit-only wall clock; never gates the fence decision
    uint64_t gc_mono = 0;
    Gc gc(store, kGc, [&] { return gc_now; }, [&] { return gc_mono; });

    // Capture the emitted events so we can assert the round emits exactly one GcFenceOut row for srid2.
    store->setEventSink([events](const CasEvent & e)
    {
        events->push(e);
    });

    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(7);
    writeBlobBody(*backend, layout, blob);
    writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);

    // Round 1 (mono 0): first sight of both mounts — observation starts, nothing fenced yet.
    const RoundReport rep1 = gc.runRegularRound();
    EXPECT_EQ(rep1.fence_outs, 0u);

    // The store's OWN mount renews between rounds (as a live leader would); srid2 never does.
    store->renewWatermarkOnce();
    gc_mono = threshold_ms - 1;

    // Round 2 (mono == threshold - 1): a discriminator for the threshold's EXACT value, not just its
    // existence — one millisecond short of the full threshold, srid2's original token must NOT be fenced
    // yet. Without this round, any knob-shortened positive threshold would also satisfy the fence-out
    // assertion taken only at the full threshold below.
    const RoundReport rep_before_threshold = gc.runRegularRound();
    EXPECT_EQ(rep_before_threshold.fence_outs, 0u);
    EXPECT_FALSE(decodeMountLease(readObj(*backend, layout.mountKey(srid2))->bytes).gc_fenced);

    gc_mono = threshold_ms;

    // Round 3 (mono == threshold): srid2's original token has held stable for the full threshold —
    // fenced. The store's own (just-renewed) mount restarts its observation and stays live.
    const RoundReport rep = gc.runRegularRound();

    EXPECT_EQ(rep.fence_outs, 1u);   // exactly one dead mount fenced-out this round
    const MountLease fenced = decodeMountLease(readObj(*backend, layout.mountKey(srid2))->bytes);
    EXPECT_TRUE(fenced.gc_fenced);

    // Exactly one GcFenceOut audit row was emitted, naming srid2 in its detail.
    size_t fence_out_rows = 0;
    for (const CasEvent & e : events->snapshot())
        if (e.type == CasEventType::GcFenceOut)
        {
            ++fence_out_rows;
            EXPECT_EQ(e.outcome, "fenced");
            EXPECT_FALSE(e.reason.empty());
            const auto it = e.detail.find("server_root_id");
            ASSERT_NE(it, e.detail.end());
            EXPECT_EQ(it->second, srid2);
        }
    EXPECT_EQ(fence_out_rows, 1u);

    // srid2's writer comes back and tries to renew: its held token was invalidated by the fence rewrite,
    // so synchronous renewal returns a terminal failure. (It renews on its own clock; liveness is irrelevant — the token guard
    // trips regardless.)
    const MountRenewResult renewed = srid2_renewer.renew(MountRenewOperationEnvironment{});
    ASSERT_EQ(renewed.outcome, MountRenewOutcome::Terminal);
    ASSERT_NE(renewed.failure, nullptr);
    EXPECT_THROW(std::rethrow_exception(renewed.failure), DB::Exception);

    // The fence-out is pure liveness cleanup: reclaim proceeds through the normal (round-paced) pipeline
    // regardless of srid2's fate — fencing one stale mount must never wedge the reclaim pipeline.
    dropRefTransition(*backend, layout, ns, "tbl", r);
    EXPECT_TRUE(runRoundsUntilAbsent(store, gc, *backend, layout, blob));
}

}

TEST(CASGCAckFloor, ExpiredMountFencedOutAndExcluded)
{
    runExpiredMountFenceOutScenario(PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// GC's fence-out threshold (`ttl + 5% drift allowance + one round's worth of renewal slack`, computed in
/// `Gc::runRegularRound`) never reads `PoolConfig::unsafe_remount_no_delay` -- that knob is consulted only
/// by `Pool::mountWritable`'s own reclaim decision, never by GC's heartbeat-floor observation. Runs the
/// IDENTICAL three-round scenario as `ExpiredMountFencedOutAndExcluded` with the knob turned on, and
/// asserts the SAME round-by-round fence-out counts -- including the one-millisecond-short discriminator
/// round -- proving the threshold's exact value and its timing are unaffected.
TEST(CASGcFenceOut, ThresholdUnchangedByUnsafeKnob)
{
    runExpiredMountFenceOutScenario(
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .unsafe_remount_no_delay = true});
}

/// fix-round F6 (author-review: `Gc`'s own `mono_ms_fn` used to default to the RAW static `Pool::
/// bootMs()`, bypassing the Pool's own injectable `config.boot_ms_fn` -- a time-controlled test can
/// desync the mount side's fake clock from the GC side's real one). This mirrors
/// `ExpiredMountFencedOutAndExcluded` exactly, except: the Pool is opened with an injected
/// `boot_ms_fn` driving a FAKE clock that barely advances in real time, and `Gc` is constructed WITHOUT
/// an explicit `mono_ms_fn` -- exercising the DEFAULT under test. If the default still read the real
/// wall clock, this round would see essentially zero elapsed mono time and never cross the fence-out
/// threshold; the fix makes it default to `store->bootMsNow()`, which tracks the SAME fake clock.
TEST(CASGCAckFloor, DefaultMonoClockTracksPoolsInjectedBootClockNotWallClock)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// Held in a shared atomic, not a plain local: this test mutates the clock below, and the Pool can
    /// outlive this stack frame (a background publish holds `shared_from_this()`), so a by-reference
    /// capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(0);
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        }});
    const Layout & layout = store->layout();

    // A stale mount, exactly as `ExpiredMountFencedOutAndExcluded`: one claim, never renewed again.
    const String srid2 = "stale-server";
    CasRequests renewer_requests = openRequestsForTest(backend);
    MountLeaseRenewer srid2_renewer(renewer_requests, renewer_requests, layout, srid2, DB::UInt128(0x2222),
        /*writer_epoch=*/1,
        std::chrono::milliseconds(100), [] { return 1000u; },
        [fake_boot]
        {
            return fake_boot->load();
        });
    srid2_renewer.start();
    ASSERT_FALSE(decodeMountLease(readObj(*backend, layout.mountKey(srid2))->bytes).gc_fenced);

    const uint64_t ttl_ms = static_cast<uint64_t>(store->poolConfig().mount_lease_ttl_ms.count());
    const uint64_t threshold_ms = ttl_ms + ttl_ms / 20
        + static_cast<uint64_t>(store->poolConfig().mount_renew_period.count());

    // `Gc` constructed with only `now_ms_fn` -- `mono_ms_fn` is left at its DEFAULT (the fix under test).
    Gc gc(store, kGc, [] { return 1'000'000u; });

    const RoundReport rep1 = gc.runRegularRound();
    EXPECT_EQ(rep1.fence_outs, 0u);

    store->renewWatermarkOnce();
    fake_boot->store(threshold_ms);   // advance the FAKE clock only; this test runs in well under a millisecond

    const RoundReport rep2 = gc.runRegularRound();
    EXPECT_EQ(rep2.fence_outs, 1u)
        << "Gc's default mono_ms_fn must track the Pool's injected boot clock, not the real wall clock";
    EXPECT_TRUE(decodeMountLease(readObj(*backend, layout.mountKey(srid2))->bytes).gc_fenced);
}

/// A redelete of a blob the writer RECREATED (fresh incarnation) between the pending publish and the
/// deleting pass finds a different incarnation — a terminal-OK outcome recorded as a replace: the fresh
/// incarnation is a live object and survives. report.replaced counts it.
TEST(CASGCAckFloor, RecreatedBlobDeleteIsTokenMismatchOk)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    const BlobRef blob_id{BlobHashAlgo::CityHash128, BlobDigest::fromU128(blob)};
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);

    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    runRegularRoundReclaiming(gc);   // condemn (captures the ORIGINAL token)
    store->renewWatermarkOnce();

    // Drive rounds until the entry is delete_pending (the token it holds is the original observation).
    bool pending = false;
    for (int i = 0; i < 6 && !pending; ++i)
    {
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
        const auto e = currentEntryFor(*backend, store->layout(), blob);
        pending = e && e->delete_pending;
    }
    ASSERT_TRUE(pending);

    // The writer recreates the blob with a FRESH incarnation before the deleting pass: the current token no
    // longer matches the pending entry's captured token.
    displaceBlobToken(*backend, store->layout(), blob_id);

    // The deleting pass observes the key, finds an incarnation the entry does not name → Replaced. The
    // fresh incarnation survives; the entry is dropped.
    const RoundReport rep = runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    EXPECT_EQ(rep.replaced, 1u);
    EXPECT_TRUE(blobExists(*backend, store->layout(), blob));   // the recreated incarnation is live
    EXPECT_FALSE(currentEntryFor(*backend, store->layout(), blob).has_value());
}

/// Idempotent replay of a crashed round: a fresh Gc instance (new lease seq = new attempt) re-runs a round
/// and completes; a delete that already landed under a prior pass replays onto NotFound (Absent outcome)
/// and the round still completes. We model the crash-after-delete-before-CAS replay by manually deleting
/// the pending blob (its exact token) BEFORE the deleting pass, then asserting the pass reports the delete
/// as absent (report.absent == 1) and completes (round advances).
TEST(CASGCAckFloor, ResumeAfterCrashBetweenRetiredPutAndStateCas)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    const BlobRef blob_id{BlobHashAlgo::CityHash128, BlobDigest::fromU128(blob)};
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    // A fresh Gc per round (each acquires the lease, bumping lease.seq = a fresh attempt) — the replay
    // property: no wedging, each round completes under its own fresh attempt.
    {
        Gc gc(store, kGc);
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
    }
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);

    RetiredEntry pending_entry;
    bool pending = false;
    for (int i = 0; i < 6 && !pending; ++i)
    {
        Gc gc(store, kGc);
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
        const auto e = currentEntryFor(*backend, store->layout(), blob);
        if (e && e->delete_pending)
        {
            pending = true;
            pending_entry = *e;
        }
    }
    ASSERT_TRUE(pending);

    // Simulate a crashed deleting pass that DID land the exact-token delete but crashed before the gc/state
    // CAS. The next (fresh-attempt) pass replays the delete → the object is already gone → NotFound → the
    // pass records Absent and completes.
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String pending_key = store->layout().blobKey(blob_id);
    const std::optional<Meta> doomed = op.head(pending_key, Retry::once());
    ASSERT_TRUE(doomed);
    ASSERT_TRUE(pending_entry.token.matches(doomed->etag));
    ASSERT_EQ(op.remove(pending_key, doomed->etag, Retry::once()), Removal::Removed);

    const uint64_t round_before = decodeGcState(readObj(*backend, store->layout().gcStateKey())->bytes).round;
    Gc gc2(store, kGc);
    const RoundReport rep = runRegularRoundReclaiming(gc2);
    store->renewWatermarkOnce();
    EXPECT_EQ(rep.absent, 1u);   // the replayed delete found the object already gone
    const uint64_t round_after = decodeGcState(readObj(*backend, store->layout().gcStateKey())->bytes).round;
    EXPECT_GT(round_after, round_before);   // the round completed (no wedge)
    EXPECT_FALSE(currentEntryFor(*backend, store->layout(), blob).has_value());
}

/// A blob whose body a crashed pass already deleted must settle as Absent (never Replaced), its `.meta`
/// cleanup must still run, and -- the part a store can punish -- the round must not send a conditional
/// removal against the absent key at all. A store may answer such a removal with a precondition failure
/// instead of a clean miss (rustfs does), which is indistinguishable from "somebody replaced it"; the
/// round observes first, so it never has to tell the two apart.
TEST(CASGCAckFloor, AbsentBlobSettlesAsAbsentWithoutASpeculativeConditionalRemoval)
{
    auto backend = std::make_shared<AbsentRemovalWatchBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    const BlobRef blob_id{BlobHashAlgo::CityHash128, BlobDigest::fromU128(blob)};
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    runRegularRoundReclaiming(gc);   // condemn
    store->renewWatermarkOnce();

    // Drive rounds until the entry is delete_pending, capturing its exact condemn-time token.
    RetiredEntry pending_entry;
    bool pending = false;
    for (int i = 0; i < 6 && !pending; ++i)
    {
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
        const auto e = currentEntryFor(*backend, store->layout(), blob);
        if (e && e->delete_pending)
        {
            pending = true;
            pending_entry = *e;
        }
    }
    ASSERT_TRUE(pending);
    {
        const auto lm = loadMetaForTest(*backend, store->layout(), blob);
        ASSERT_TRUE(lm.has_value()) << "the blob must still carry its Condemned freshness meta pre-delete";
        ASSERT_EQ(lm->meta.state, MetaState::Condemned);
    }

    // The object is genuinely gone already (as if a prior crashed pass landed the delete), and from here
    // every removal the round sends against this key would be sent against an absent object.
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String blob_key = store->layout().blobKey(blob_id);
    const std::optional<Meta> doomed = op.head(blob_key, Retry::once());
    ASSERT_TRUE(doomed);
    ASSERT_TRUE(pending_entry.token.matches(doomed->etag));
    ASSERT_EQ(op.remove(blob_key, doomed->etag, Retry::once()), Removal::Removed);
    ASSERT_FALSE(op.head(blob_key, Retry::once()));
    backend->watch(blob_key);

    // The deleting pass replays the redelete: it observes the absent key and settles Absent without a
    // request the store could answer ambiguously, and the `.meta` cleanup still runs.
    const RoundReport rep = runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    EXPECT_EQ(rep.absent, 1u) << "an already-absent blob settles as Absent, not Replaced";
    EXPECT_EQ(rep.replaced, 0u);
    EXPECT_EQ(backend->removalsAgainstAbsent(), 0u)
        << "the redelete observed the key first, so it sent no conditional removal against an absent object";
    EXPECT_FALSE(currentEntryFor(*backend, store->layout(), blob).has_value());
    EXPECT_FALSE(loadMetaForTest(*backend, store->layout(), blob).has_value())
        << ".meta cleanup (gated on a removal or a proven absence) must still run on the Absent outcome";
}

/// ---- condemn-marker gate suite ----
///
/// The per-hash condemn marker is LOAD-BEARING for the delete edge: the writer's adopt gate point-reads
/// the meta and an ABSENT meta reads as Clean, so a blob whose condemn-marker write was swallowed can be
/// same-token adopted by a writer landing in the [discovery-LIST, redelete] window — invisible to the
/// graduating fold — and the redelete then deletes a body under a live committed edge
/// (dangling manifest). Graduation to `delete_pending` therefore requires CONFIRMED durable `Condemned`
/// evidence for the entry; absent evidence CARRIES the entry to the next round (fail-safe delay, never a
/// fail-open delete) and retries the marker so a healed backend restores liveness.

/// A condemned entry whose marker write was swallowed must be CARRIED round after round — never
/// graduated, never deleted — until durable `Condemned` evidence exists. Once the backend heals, the
/// carry-time marker retry lands and the normal two-phase pipeline reclaims the blob (delay, not a leak).
TEST(CASGCCondemnMarker, SwallowedMarkerWriteCarriesEntryInsteadOfDeleting)
{
    auto backend = std::make_shared<MetaWriteFaultBackend>();
    /// A SWALLOWED write is the premise: the store may have applied it and said nothing, which is the
    /// only shape that leaves the round committing an entry whose marker it cannot confirm. The
    /// propagating kind never reaches the engine's resolve-and-reissue path at all -- the write loop
    /// rethrows a non-`Poco::Exception` on its first attempt.
    backend->armWriteFault(MetaWriteFaultBackend::FaultKind::Ambiguous);
    auto store = openPoolForTest(backend);
    store->setCasRetrySleepForTest([](uint64_t) {});

    /// THE FAULT IS PERMANENT, so every condemn-marker write runs the engine's WHOLE retry window, and
    /// that window has to run on a clock this test advances. A zeroed sleep alone does not do it: the
    /// deadline is still measured against the real clock, so the loop would spin hot for ninety real
    /// seconds per marker write. The sleep therefore moves the clock past its own pause -- plus one
    /// millisecond, because full-jitter backoff may draw zero and a clock that never moves never closes
    /// the window. Scoped to the GC plane, which is where `writeCondemnedMeta` runs, so the mount
    /// plane's lease-bound policies keep their real clock.
    /// Held in shared, heap-owned atomics, not plain locals: `store->openRequests()` is the Pool's own
    /// persistent engine, and the Pool can outlive this stack frame (a background publish holds
    /// `shared_from_this()`), so a by-reference capture of a local -- even an already-atomic one --
    /// would dangle once the frame returns.
    auto engine_now_ms = std::make_shared<std::atomic<uint64_t>>(0);
    auto engine_sleeps = std::make_shared<std::atomic<uint64_t>>(0);
    store->openRequests().setNowFnForTest([engine_now_ms]
    {
        return engine_now_ms->load();
    });
    store->openRequests().setSleepFnForTest([engine_now_ms, engine_sleeps](uint64_t pause_ms)
    {
        engine_sleeps->fetch_add(1);
        engine_now_ms->fetch_add(pause_ms + 1);
    });

    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);

    runRegularRoundReclaiming(gc);   // +1 folds; blob referenced
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    runRegularRoundReclaiming(gc);   // the condemning round; the marker write gives up without committing
    ASSERT_FALSE(loadMetaForTest(*backend, store->layout(), blob).has_value())
        << "precondition: the injected fault must have lost the condemn-marker write";
    /// The marker write was resolved and reissued, then gave up at its own policy window. A give-up
    /// needs the next jittered pause not to fit before the deadline and full jitter draws at most five
    /// seconds, so it cannot happen before the clock has passed `Retry::standard()`'s window minus
    /// that draw. Both assertions pin the REISSUING, which is what the ambiguous kind buys; neither
    /// can tell the injected clock from the real one -- that seam bounds the reissuing in real time.
    EXPECT_GT(engine_sleeps->load(), 1u)
        << "an ambiguous marker write must be resolved and reissued, not surfaced on its first attempt";
    EXPECT_GT(engine_now_ms->load(), 85'000u)
        << "the marker write must have spent its whole retry window before reporting failure";
    ASSERT_TRUE(currentEntryFor(*backend, store->layout(), blob).has_value())
        << "precondition: the retired entry must have been committed despite the lost marker";

    /// Rounds keep coming while the marker stays unwritable: without durable Condemned evidence the
    /// entry must be CARRIED — a writer reading the absent meta may have adopted this exact token.
    const auto carries_before =
        ProfileEvents::global_counters[ProfileEvents::CASGCCondemnMarkerUnconfirmedCarry].load();
    for (int i = 0; i < 4; ++i)
    {
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
        EXPECT_TRUE(blobExists(*backend, store->layout(), blob))
            << "round " << i << " after condemn: deleted without a durable condemn marker";
    }
    const auto e = currentEntryFor(*backend, store->layout(), blob);
    ASSERT_TRUE(e.has_value()) << "the entry must remain retired (carried), not dropped";
    EXPECT_FALSE(e->delete_pending) << "graduation must be refused without a confirmed marker";
    EXPECT_FALSE(e->marker_confirmed);
    EXPECT_GE(ProfileEvents::global_counters[ProfileEvents::CASGCCondemnMarkerUnconfirmedCarry].load()
                  - carries_before, 4u)
        << "every refused graduation must count one unconfirmed carry";

    /// Heal the backend: the carry-time retry publishes the marker, the entry confirms + graduates, and
    /// the pipeline reclaims the blob and drops the meta.
    backend->fail_meta_writes.store(false);
    EXPECT_TRUE(runRoundsUntilAbsent(store, gc, *backend, store->layout(), blob));
    EXPECT_FALSE(currentEntryFor(*backend, store->layout(), blob).has_value());
    EXPECT_FALSE(loadMetaForTest(*backend, store->layout(), blob).has_value());
}

/// The healthy-path counterpart: with the condemn-time marker write landing normally, the gate must not
/// change the canonical schedule — condemned at round K, graduated (delete_pending) at K+1, deleted at
/// K+2 — and the durable Condemned marker exists from the condemning round on.
TEST(CASGCCondemnMarker, DurableMarkerKeepsCanonicalGraduationSchedule)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);

    runRegularRoundReclaiming(gc);
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);

    {
        const RoundReport rep = runRegularRoundReclaiming(gc);   // condemn round K
        EXPECT_EQ(rep.condemned, 1u);
        const auto lm = loadMetaForTest(*backend, store->layout(), blob);
        ASSERT_TRUE(lm.has_value());
        EXPECT_EQ(lm->meta.state, MetaState::Condemned);
    }
    {
        const RoundReport rep = runRegularRoundReclaiming(gc);   // K+1: confirmed marker => graduates on schedule
        EXPECT_EQ(rep.graduated, 1u);
        const auto e = currentEntryFor(*backend, store->layout(), blob);
        ASSERT_TRUE(e.has_value());
        EXPECT_TRUE(e->delete_pending);
        EXPECT_TRUE(e->marker_confirmed) << "a delete_pending row must carry the confirmation bit";
        EXPECT_TRUE(blobExists(*backend, store->layout(), blob));
    }
    {
        const RoundReport rep = runRegularRoundReclaiming(gc);   // K+2: the pending delete executes
        EXPECT_EQ(rep.redeleted, 1u);
        EXPECT_FALSE(blobExists(*backend, store->layout(), blob));
    }
}

/// The leader-restart path: `condemn_markers_confirmed` is a process-local registry on `Gc`, lost on a
/// GC leader restart. Same idiom as the crash-replay tests above (`Gc gc2(store, kGc)` -- a fresh `Gc`
/// object under the SAME identity models a process restart that resumes its own lease, not a steal by a
/// different owner). The fresh instance must still confirm graduation via the ONE synchronous `loadMeta`
/// re-check: the durable `Condemned` meta observed now is sufficient evidence on its own, with no
/// in-process (hash, token) confirmation available at all. This proves the fallback branch -- not just
/// the in-process registry -- authorizes the delete.
TEST(CASGCCondemnMarker, LoadMetaFallbackConfirmsGraduationAfterLeaderRestart)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    {
        /// The first (soon-to-be-gone) leader: seeds the blob, condemns it, and lets the marker write
        /// land on the healthy backend. Its `condemn_markers_confirmed` registry dies with it.
        Gc gc(store, kGc);
        runRegularRoundReclaiming(gc);
        dropRefTransition(*backend, store->layout(), ns, "tbl", r);
        const RoundReport rep = runRegularRoundReclaiming(gc);   // condemn round
        EXPECT_EQ(rep.condemned, 1u);
        store->renewWatermarkOnce();
    }
    const auto lm = loadMetaForTest(*backend, store->layout(), blob);
    ASSERT_TRUE(lm.has_value());
    EXPECT_EQ(lm->meta.state, MetaState::Condemned)
        << "precondition: the durable marker must be on disk before the simulated restart";

    /// A brand-new `Gc` object under the SAME identity -- an empty `condemn_markers_confirmed`, exactly
    /// as after a process restart that resumes its own lease. It never observed the condemn round above,
    /// so the in-process confirmation path (`condemnMarkerConfirmedInProcess`) has nothing to return true
    /// for; only the `loadMeta` fallback can authorize graduation.
    Gc gc2(store, kGc);
    const RoundReport rep = runRegularRoundReclaiming(gc2);
    EXPECT_EQ(rep.graduated, 1u)
        << "the loadMeta fallback (leader-restart path) must authorize graduation from durable evidence "
           "alone";
    const auto e = currentEntryFor(*backend, store->layout(), blob);
    ASSERT_TRUE(e.has_value());
    EXPECT_TRUE(e->delete_pending);
    EXPECT_TRUE(e->marker_confirmed) << "a delete_pending row confirmed via loadMeta still carries the bit";
    EXPECT_TRUE(blobExists(*backend, store->layout(), blob));
}

#if USE_AWS_S3
/// The outcomes-log `create` meets the same shape as the round commit: a refused precondition whose
/// resolve read was itself refused. Nothing observed the key, so the round may not report that the log
/// vanished -- an absent key and an unreadable one are different answers.
///
/// The S3 gate is the fault's, not the site's: the definitive-refusal classification that makes a
/// resolve read settle nothing rather than be reissued exists only for S3 errors.
TEST(CASGCRetire, OutcomeLogUnobservedConflictDoesNotReportItVanished)
{
    /// The fault's state is shared between the round (writes, on the test thread) and the meta
    /// writer's pool (reads), so it lives under its own mutex.
    class UnobservedOutcomesBackend : public InMemoryBackend
    {
    public:
        std::expected<String, RawConflict> write(
            const String & key, const String & bytes, const std::optional<String> & expected_value,
            TransportAccess & access) override
        {
            {
                std::lock_guard lock(fault_mutex);
                if (arm && !expected_value && key.find("/outcomes/") != String::npos)
                {
                    arm = false;
                    refused_key = key;
                    return std::unexpected(RawConflict{});
                }
            }
            return InMemoryBackend::write(key, bytes, expected_value, access);
        }

        std::optional<Raw> read(const String & key, TransportAccess & access) override
        {
            {
                std::lock_guard lock(fault_mutex);
                if (!refused_key.empty() && key == refused_key)
                {
                    refused_key.clear();
                    throw DB::S3Exception("UnobservedOutcomesBackend: the settling read is definitively refused",
                                          Aws::S3::S3Errors::UNKNOWN, "MalformedXML");
                }
            }
            return InMemoryBackend::read(key, access);
        }

        void armOnce()
        {
            std::lock_guard lock(fault_mutex);
            arm = true;
        }

    private:
        std::mutex fault_mutex;
        bool arm TSA_GUARDED_BY(fault_mutex) = false;
        String refused_key TSA_GUARDED_BY(fault_mutex);
    };

    auto backend = std::make_shared<UnobservedOutcomesBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    gc.runRegularRound();
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);

    /// The condemn -> graduate -> delete pipeline needs several rounds before any round has an outcome
    /// to log; the arm fires on the first one that does.
    backend->armOnce();
    bool refusal_reached = false;
    for (int i = 0; i < 8 && !refusal_reached; ++i)
    {
        try
        {
            runRegularRoundReclaiming(gc);
            store->renewWatermarkOnce();
        }
        catch (const DB::Exception & e)
        {
            refusal_reached = true;
            EXPECT_EQ(e.code(), DB::ErrorCodes::ABORTED);
            EXPECT_NE(e.message().find("resolve read observed nothing"), String::npos) << e.message();
            EXPECT_EQ(e.message().find("vanished"), String::npos)
                << "nothing observed the key, so it may not be called vanished: " << e.message();
        }
    }
    EXPECT_TRUE(refusal_reached) << "no round ever wrote an outcome log, so the arm was never reached";
}
#endif
