#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CatalogLifecycleReconciler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Common/ProfileEvents.h>
#include "cas_test_helpers.h"

#include <Poco/StreamChannel.h>

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CORRUPTED_DATA;
}

/// THE DESTRUCTIVE-ROUND FRONTIER PROOF (spec 2026-07-27 "ref chain complete cut" §5).
///
/// Reachability is a property of the WHOLE POOL. A blob is unreferenced only if no namespace anywhere
/// owns an edge to it, so a round that deletes one is asserting something about every namespace at
/// once -- including the ones it never looked at. Task 7 made the per-namespace half of that assertion
/// cheap and exact: one `GET` at the cursor's arithmetic successor, absent means end-of-stream. Task 8
/// made a namespace that could NOT be walked say so durably. What neither can supply is the SET those
/// proofs have to cover, and that is what this task is about.
///
/// So the gate has three terms, and a round destroys only when all three are clear:
///
///     suppress_destructive = any anomaly this round
///                          OR any hold the seal carries
///                          OR the frontier is incomplete
///
/// The second term is STRUCTURAL. Every hold recorded today also records an anomaly, so the first term
/// happens to imply it -- but the invariant is the hold SET, not that coincidence, and the gate reads
/// the seal directly so that a future change to anomaly recording cannot quietly open it.
///
/// The third term is the SET, and only the catalog supplies it. The scenario that makes that so is the
/// one these tests open with: a hidden acked `+1` in a namespace no listing mentions and no sealed cursor
/// names, while a visible `-1` elsewhere drives the shared blob's OBSERVABLE in-degree to zero. Every
/// proof the round holds comes back clean and the blob is still owned. It survives because the round's
/// universe is the catalog's `Live`/`Removing` set, so that namespace is a member the round owes a proof
/// for and cannot supply one -- neither the listing's silence nor the missing cursor can shrink the set.
///
/// The tests here come in two shapes. Ones whose subject is the OPEN gate run on the production path,
/// with no policy argument at all. Ones whose subject is a SUPPRESSOR either pass `StageA_Suppressed`
/// explicitly or arrange the suppressing condition on the pool, and assert every delete family inert PER
/// FAMILY -- an aggregate zero can hide one family running while another did not.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace ProfileEvents
{
extern const Event CASGCRefWalkPlansBuilt;
extern const Event CASGCUnmatchedAdoptedParentLives;
extern const Event CASGCNamespaceCleanupLeaks;
}

namespace
{

const UInt128 kGc = hexToU128("00000000000000000000000000000001");

/// The lying store, shared from `cas_test_helpers.h`: every key is served by exact GET while the
/// selected ones are HIDDEN from every LIST. That is the only way to build the cross-namespace
/// scenario -- the hidden namespace's records stay durable and readable, so a round that KNOWS to
/// look for them finds them, while a round that only enumerates never learns they exist. Composed
/// over `CountingBackend` because these tests also assert request counts.
using CountingHintHoleBackend = DB::Cas::tests::HintHoleBackendOn<DB::Cas::tests::CountingBackend>;

class DrainRaceBackend final : public CountingBackend
{
public:
    using CountingBackend::casPut;
    using CountingBackend::get;
    using CountingBackend::putIfAbsent;

    void blockNextCatalogCas(const String & key)
    {
        std::lock_guard lock(control_mutex);
        catalog_key = key;
        block_next_catalog_cas = true;
    }

    void loseNextCatalogCasResponse(const String & key)
    {
        std::lock_guard lock(control_mutex);
        catalog_key = key;
        lose_next_catalog_cas_response = true;
    }

    void conflictNextCatalogCas(const String & key)
    {
        std::lock_guard lock(control_mutex);
        catalog_key = key;
        conflict_next_catalog_cas = true;
    }

    void waitForBlockedCatalogCas()
    {
        std::unique_lock lock(control_mutex);
        control_cv.wait(lock, [&] { return catalog_cas_blocked; });
    }

    void releaseBlockedCatalogCas()
    {
        std::lock_guard lock(control_mutex);
        release_catalog_cas = true;
        control_cv.notify_all();
    }

    void clearJournal()
    {
        std::lock_guard lock(journal_mutex);
        journal.clear();
    }

    std::vector<String> journalSnapshot() const
    {
        std::lock_guard lock(journal_mutex);
        return journal;
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        record("get " + key);
        return CountingBackend::get(key, range);
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        record("list " + prefix);
        return CountingBackend::list(prefix, cursor, limit);
    }

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        record("put_begin " + key);
        const PutResult result = CountingBackend::putIfAbsent(key, bytes, meta);
        record("put_end " + key);
        return result;
    }

    CasResult casPut(
        const String & key, const String & bytes, const std::optional<Token> & expected,
        const ObjectMeta & meta) override
    {
        record("cas_begin " + key);
        bool lose_response = false;
        bool force_conflict = false;
        {
            std::unique_lock lock(control_mutex);
            if (key == catalog_key && block_next_catalog_cas)
            {
                block_next_catalog_cas = false;
                catalog_cas_blocked = true;
                control_cv.notify_all();
                control_cv.wait(lock, [&] { return release_catalog_cas; });
            }
            if (key == catalog_key && lose_next_catalog_cas_response)
            {
                lose_next_catalog_cas_response = false;
                lose_response = true;
            }
            if (key == catalog_key && conflict_next_catalog_cas)
            {
                conflict_next_catalog_cas = false;
                force_conflict = true;
            }
        }
        if (force_conflict)
        {
            record("cas_forced_conflict " + key);
            return {.outcome = CasOutcome::Conflict, .token = {}};
        }
        const CasResult result = CountingBackend::casPut(key, bytes, expected, meta);
        record("cas_end " + key);
        if (lose_response && result.outcome == CasOutcome::Committed)
        {
            record("cas_response_lost " + key);
            throw std::runtime_error("injected lost catalog CAS response");
        }
        return result;
    }

private:
    void record(String entry) const
    {
        std::lock_guard lock(journal_mutex);
        journal.push_back(std::move(entry));
    }

    mutable std::mutex journal_mutex;
    mutable std::vector<String> journal;
    std::mutex control_mutex;
    std::condition_variable control_cv;
    String catalog_key;
    bool block_next_catalog_cas = false;
    bool catalog_cas_blocked = false;
    bool release_catalog_cas = false;
    bool lose_next_catalog_cas_response = false;
    bool conflict_next_catalog_cas = false;
};

class PostFoldUnreadableTerminalBackend final : public CountingBackend
{
public:
    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ListPage page = CountingBackend::list(prefix, cursor, limit);
        if (prefix.ends_with("/cas/ns/"))
            for (ListedKey & listed : page.keys)
                listed.token.reset();
        return page;
    }

    HeadResult head(const String & key) override
    {
        if (key == unreadable_key)
            throw std::runtime_error("injected post-fold terminal read failure for " + key);
        return CountingBackend::head(key);
    }

    void makeUnreadable(String key)
    {
        unreadable_key = std::move(key);
    }

    bool existsIgnoringFault(const String & key)
    {
        return CountingBackend::head(key).exists;
    }

private:
    String unreadable_key;
};

class ScopedCasGcLogCapture
{
public:
    ScopedCasGcLogCapture()
        : logger(getLogger("CasGc"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel("warning");
    }

    ~ScopedCasGcLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    String captured() const
    {
        return stream.str();
    }

private:
    LoggerPtr logger;
    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::StreamChannel> channel;
    /// A real reference (shared=true), so the parked previous channel cannot die while ours is installed.
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

struct CompletedRemovingFixture
{
    RootNamespace ns;
    UInt128 life_id{};
    String checkpoint_key;
    String checkpoint_bytes;
};

CompletedRemovingFixture seedCompletedRemoving(
    DrainRaceBackend & backend, const PoolPtr & store, const UInt128 & lease_owner)
{
    const Layout & layout = store->layout();
    CompletedRemovingFixture fixture{
        .ns = RootNamespace{"00/drain-race@cas@"},
        .life_id = UInt128{177},
        .checkpoint_key = {},
        .checkpoint_bytes = {}};
    CasRefCatalog::casAdmitEntry(backend, layout, store->poolConfig().gc_shards, CatalogEntry{
        .ns = fixture.ns, .state = NsState::Live, .incarnation = fixture.life_id});
    fixture.checkpoint_key = layout.refCkptKey(
        NamespaceLifeId::fromCatalogEntry(fixture.ns, fixture.life_id));
    fixture.checkpoint_bytes = encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
    backend.putIfAbsent(fixture.checkpoint_key, fixture.checkpoint_bytes);
    EXPECT_TRUE(store->namespaceFilesLifeIfReadable(fixture.ns));
    CasRefCatalog::casUpdate(backend, layout, [](const RefCatalog & current)
    {
        RefCatalog next = current;
        next.entries[0].state = NsState::Removing;
        next.entries[0].removal_started_round = 1;
        return next;
    });

    CasFoldSeal parent;
    parent.generation = 1;
    parent.ref_lives.emplace(fixture.life_id, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 1}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 1}}});
    for (uint64_t shard = 0; shard < store->poolConfig().gc_shards; ++shard)
        parent.condemned_summary.emplace(shard, CondemnedSummary{});
    backend.putIfAbsent(layout.foldSealKey(1, 1), encodeFoldSeal(parent));

    GcState state;
    state.round = 1;
    state.gc_shards = store->poolConfig().gc_shards;
    state.snap_generation = 1;
    state.snap_attempt = 1;
    state.lease = GcLease{.owner = lease_owner, .seq = 1};
    backend.putIfAbsent(layout.gcStateKey(), encodeGcState(state));

    return fixture;
}

void seedCompletedRemovingBatch(
    DrainRaceBackend & backend, const PoolPtr & store, const UInt128 & lease_owner, size_t count)
{
    const Layout & layout = store->layout();
    std::vector<CatalogEntry> entries;
    entries.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        CatalogEntry entry{
            .ns = RootNamespace{fmt::format("00/drain-batch-{}@cas@", i)},
            .state = NsState::Live,
            .incarnation = UInt128{200 + i}};
        CasRefCatalog::casAdmitEntry(backend, layout, store->poolConfig().gc_shards, entry);
        entries.push_back(std::move(entry));
    }
    CasRefCatalog::casUpdate(backend, layout, [](const RefCatalog & current)
    {
        RefCatalog next = current;
        for (CatalogEntry & entry : next.entries)
        {
            entry.state = NsState::Removing;
            entry.removal_started_round = 1;
        }
        return next;
    });

    CasFoldSeal parent;
    parent.generation = 1;
    for (const CatalogEntry & entry : entries)
        parent.ref_lives.emplace(entry.incarnation, RefLifeFoldState{
            .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 1}},
            .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 1}}});
    for (uint64_t shard = 0; shard < store->poolConfig().gc_shards; ++shard)
        parent.condemned_summary.emplace(shard, CondemnedSummary{});
    ASSERT_EQ(backend.putIfAbsent(layout.foldSealKey(1, 1), encodeFoldSeal(parent)).outcome,
        PutOutcome::Done);

    GcState state;
    state.round = 1;
    state.gc_shards = store->poolConfig().gc_shards;
    state.snap_generation = 1;
    state.snap_attempt = 1;
    state.lease = GcLease{.owner = lease_owner, .seq = 1};
    ASSERT_EQ(backend.putIfAbsent(layout.gcStateKey(), encodeGcState(state)).outcome, PutOutcome::Done);
}

enum class CompetingCatalogOutcome : uint8_t
{
    Absent,
    Replacement,
};

class CASGCCompletedRemovalFenceRace : public testing::TestWithParam<CompetingCatalogOutcome>
{
};

void transferGcLease(DrainRaceBackend & backend, const Layout & layout, const UInt128 & new_owner)
{
    const auto got = backend.get(layout.gcStateKey());
    ASSERT_TRUE(got);
    GcState state = decodeGcState(got->bytes);
    state.lease.owner = new_owner;
    ++state.lease.seq;
    ASSERT_EQ(backend.casPut(layout.gcStateKey(), encodeGcState(state), got->token).outcome,
        CasOutcome::Committed);
}

size_t findJournalAfter(const std::vector<String> & journal, const String & entry, size_t after)
{
    const auto it = std::find(journal.begin() + static_cast<ptrdiff_t>(after), journal.end(), entry);
    return it == journal.end() ? journal.size() : static_cast<size_t>(it - journal.begin());
}

/// A pool whose GC frontier-probe budget is set explicitly. Everything else matches `openPoolForTest`.
PoolPtr openPoolWithProbeBudget(std::shared_ptr<InMemoryBackend> backend, uint64_t budget)
{
    return Pool::open(std::move(backend),
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .gc_frontier_probe_budget = budget, .gc_fold_max_defer_rounds = 0});
}

/// Publish `ref_name` in `ns` pinning `blob`, allocating the next ref-log id. Writes the blob body and
/// the manifest body too, so the published edge is one GC can actually fold.
ManifestRef publish(Backend & backend, const Layout & layout, const RootNamespace & ns,
                    const String & ref_name, uint64_t build_sequence, const DB::UInt128 & blob)
{
    const ManifestRef mref{.writer_epoch = 1, .build_sequence = build_sequence, .manifest_ordinal = 1};
    writeBlobBody(backend, layout, blob);
    writeManifestRaw(backend, layout, ns, mref, {blobEntryFor("data.bin", blob)});
    publishCommittedTransition(backend, layout, ns, ref_name, std::nullopt, mref);
    return mref;
}

/// The blob key for a raw hash, as the tests spell it.
String blobKeyOf(const Layout & layout, const DB::UInt128 & hash)
{
    return layout.blobKey(legacyMetaTestRef(hash));
}

/// The sealed fold cursor for `ns` as a full `RefTxnId`. Every seed here allocates `writer_epoch = 1`,
/// which is what `foldCursorOf` (returning the sequence alone) assumes too.
RefTxnId sealedCursorOf(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    return RefTxnId{1, foldCursorOf(backend, layout, ns, /*shard*/ 0)};
}

/// Drive `rounds` GC rounds under the given policy, renewing the store's watermark between them the way
/// the production scheduler does.
void drive(const PoolPtr & store, Gc & gc, int rounds, UniversePolicy policy)
{
    for (int i = 0; i < rounds; ++i)
    {
        gc.runRegularRound({}, /*allow_steal*/true, policy);
        store->renewWatermarkOnce();
    }
}

/// Every key the backend was asked to delete, rendered for a failing assertion's message.
String deletedKeysMessage(const CountingBackend & backend)
{
    String out;
    for (const String & key : backend.deletedKeys())
        out += "\n    " + key;
    return out.empty() ? String{" (none)"} : out;
}

/// The gate's own verdict for one round, READ OFF THE PHASE ROWS rather than recomputed in the test. A
/// test that re-derived `frontier_complete` from the tally would agree with a wrong formula just as
/// readily as with the right one.
struct GateVerdict
{
    bool saw_fold = false;
    bool frontier_complete = false;
    bool suppress_destructive = false;
    uint64_t frontier_namespaces = 0;
    uint64_t frontier_proven = 0;
    uint64_t frontier_unprobed_budget = 0;
    uint64_t catalog_entries = 0;
    bool catalog_proved_empty = false;
};

GateVerdict runRoundCapturingGate(const PoolPtr & store, Gc & gc, UniversePolicy policy)
{
    GateVerdict verdict;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        const auto value = [&](const char * name) -> std::optional<UInt64>
        {
            const auto it = rec.metrics.find(name);
            return it == rec.metrics.end() ? std::nullopt : std::optional<UInt64>{it->second};
        };
        if (rec.phase == "fold_reduce")
        {
            if (const auto complete = value("frontier_complete"))
            {
                verdict.saw_fold = true;
                verdict.frontier_complete = *complete != 0;
            }
            if (const auto suppress = value("suppress_destructive"))
                verdict.suppress_destructive = *suppress != 0;
        }
        else if (rec.phase == "fold_ref_intake")
        {
            if (const auto total = value("frontier_namespaces"))
                verdict.frontier_namespaces = *total;
            if (const auto proven = value("frontier_proven"))
                verdict.frontier_proven = *proven;
            if (const auto unprobed = value("frontier_unprobed_budget"))
                verdict.frontier_unprobed_budget = *unprobed;
            if (const auto entries = value("catalog_entries"))
                verdict.catalog_entries = *entries;
            if (const auto proved_empty = value("catalog_proved_empty"))
                verdict.catalog_proved_empty = *proved_empty != 0;
        }
    });
    gc.runRegularRound({}, /*allow_steal*/true, policy);
    gc.setPhaseSink({});
    store->renewWatermarkOnce();
    return verdict;
}

/// Every delete family a round can reach, asserted PER FAMILY: an aggregate zero can hide one family
/// running while another did not.
void expectEveryDeleteFamilyInert(const CountingBackend & backend, const char * where)
{
    EXPECT_EQ(backend.deleteCountForKeysContaining("/blobs/"), 0u) << where << ": blob delete";
    EXPECT_EQ(backend.deleteCountForKeysContaining("/cas/manifests/"), 0u)
        << where << ": manifest-body delete";
    EXPECT_EQ(backend.deleteCountForKeysContaining("/gc/gen/"), 0u)
        << where << ": generation prune and hand-off reclaim";
    EXPECT_EQ(backend.deleteCountForKeysContaining("/cas/ns/stream/"), 0u)
        << where << ": covered-log / superseded-snapshot cleanup";
    EXPECT_EQ(backend.deleteTotal(), 0u)
        << where << ": a family not named above also ran. Deleted:" << deletedKeysMessage(backend);
}

}

/// ===================== A HIDDEN `+1` IN AN UNKNOWN NAMESPACE =====================
///
/// Two namespaces share one blob. `visible` publishes it and then drops it, so the round observes
/// `+1` then `-1` and reads the blob's in-degree as zero. `hidden` also owns it -- durably, acked,
/// readable by exact key -- but is absent from the round's LIST hint. Its own publish still carries a
/// real checkpoint, so the arithmetic walk finds and folds its `+1` by exact key regardless of what the
/// LIST omits.
///
/// The three arms below are the whole argument: the exact-key probe finds `hidden`'s edge and saves the
/// blob on a complete frontier; the blob still drains once `hidden` also honestly folds its own removal;
/// and a namespace inside the universe with a sealed cursor has its hidden `+1` found by the exact-key
/// probe the same way.

namespace
{
/// Build the shared-blob scenario. `hidden` owns `blob` and is hidden from every LIST; `visible`
/// publishes and drops it. Returns the pool.
PoolPtr buildCrossNamespaceScenario(const std::shared_ptr<CountingHintHoleBackend> & backend,
                                    const RootNamespace & hidden, const RootNamespace & visible,
                                    const DB::UInt128 & blob, bool fold_hidden_first)
{
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();

    if (fold_hidden_first)
    {
        /// Give the hidden namespace a sealed cursor, WITHOUT folding the edge under test. It publishes
        /// an unrelated blob and one round folds that; from then on the namespace is in the universe via
        /// its cursor even after the hint stops naming it.
        ///
        /// The unrelated blob is what makes this arm mean anything: if the shared blob's `+1` had
        /// already been folded by the seeding round, the blob would survive on the DURABLE in-degree and
        /// the test would pass whether or not the round probes anything. Publishing it only AFTER the
        /// seal puts it strictly above the cursor, so the probe is the one and only thing that can find
        /// it.
        publish(*backend, layout, hidden, "seed_ref", 7, DB::UInt128(0x5eed));
        Gc seed(store, kGc);
        seed.runRegularRound();
        store->renewWatermarkOnce();
    }

    publish(*backend, layout, hidden, "kept_ref", 1, blob);
    backend->hidePrefix(layout.namespaceStreamPrefix(fixture::fixtureLife(hidden)));

    const ManifestRef dropped = publish(*backend, layout, visible, "dropped_ref", 2, blob);
    dropRefTransition(*backend, layout, visible, "dropped_ref", dropped);
    return store;
}
}

/// Rounds on the PRODUCTION path -- no policy argument -- because that is what the claim is about.
/// `hidden`'s own publish left it a real checkpoint, so the arithmetic walk's exact-key probe finds and
/// folds its `+1` no matter what the LIST hides: the blob survives on its own complete, proven frontier,
/// not on the round declining to touch anything.
TEST(CASGCFrontierGate, AHiddenEdgeIsFoundByTheExactKeyProbeAndSavesTheBlobOnACompleteFrontier)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    const RootNamespace hidden{"00/hidden@cas@"};
    const RootNamespace visible{"00/visible@cas@"};
    const DB::UInt128 blob(0x5ade);

    auto store = buildCrossNamespaceScenario(backend, hidden, visible, blob, /*fold_hidden_first=*/false);
    const Layout & layout = store->layout();

    Gc gc(store, kGc);
    backend->resetCounts();
    GateVerdict verdict;
    for (int i = 0; i < 5; ++i)
    {
        const GateVerdict round = runRoundCapturingGate(store, gc, UniversePolicy::kDefault);
        if (round.saw_fold)
            verdict = round;
        store->renewWatermarkOnce();
    }

    ASSERT_TRUE(verdict.saw_fold) << "no round folded, so none published a gate verdict";
    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists)
        << "the blob a hidden namespace still owns must survive";
    EXPECT_TRUE(verdict.frontier_complete)
        << "the exact-key probe reads at `cursor + 1` and a LIST hole cannot hide an exact key, so the "
           "catalog-named hidden namespace IS provable — if this is false the blob above survived on "
           "suppression instead of on its own in-degree, which proves nothing about the edge";
    EXPECT_FALSE(verdict.suppress_destructive);
}

/// The arm above asserts "nothing was deleted", which does not on its own distinguish the gate correctly
/// refusing from the round simply never deleting anything at all. This
/// is the positive control: `hidden` is genuinely folded through its OWN drop of the same
/// blob (an honest exact-key read of a record the LIST still hides -- the arithmetic-intake mechanism
/// this whole file is about), so its frontier is REALLY proven, not merely declared so, and the blob is
/// REALLY unreferenced by both namespaces. The round drains it -- the zero-deletion arm above would pass
/// identically if the round were simply incapable of ever deleting anything.
TEST(CASGCFrontierGate, TheSameBlobDrainsOnceHiddenGenuinelyProvesItsOwnFrontier)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    const RootNamespace hidden{"00/hidden@cas@"};
    const RootNamespace visible{"00/visible@cas@"};
    const DB::UInt128 blob(0x5ade);

    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    Gc gc(store, kGc);

    /// `hidden`'s BIRTH is folded (and its cursor SEALED) with everything still listed -- a namespace
    /// with no `_ckpt` (the raw-fixture admission this file's helper uses never publishes one) has no
    /// genesis signal EXCEPT a sealed cursor or a visible LIST, so a real fold first is what makes an
    /// arithmetic (cursor-relative) genesis available at all for what follows.
    const ManifestRef kept = publish(*backend, layout, hidden, "kept_ref", 1, blob);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    store->renewWatermarkOnce();

    /// NOW `hidden` drops its own reference (written while still fully listed, so the raw fixture's own
    /// LIST -- finding the greatest existing log id, to derive the next one -- sees the truth), and
    /// ONLY THEN does its whole prefix vanish from every subsequent LIST. With a sealed cursor already
    /// in hand the walk's genesis is arithmetic (`cursor + 1`), so this drop is found and folded by
    /// exact key alone -- the arithmetic-intake mechanism this whole file is about, exercised honestly
    /// rather than declared past by fiat.
    dropRefTransition(*backend, layout, hidden, "kept_ref", kept);
    backend->hidePrefix(layout.namespaceStreamPrefix(fixture::fixtureLife(hidden)));

    const ManifestRef dropped = publish(*backend, layout, visible, "dropped_ref", 2, blob);
    dropRefTransition(*backend, layout, visible, "dropped_ref", dropped);

    drive(store, gc, /*rounds*/ 5, UniversePolicy::Authoritative);

    EXPECT_FALSE(backend->head(blobKeyOf(layout, blob)).exists)
        << "both namespaces genuinely proved their frontier and the blob is genuinely unreferenced -- "
           "the round must still be able to reclaim it";
}

/// AND THE PER-NAMESPACE LOGIC IS WHAT SAVES IT. Identical to the arm above except that the hidden
/// namespace was folded once first, so it carries a sealed cursor and is therefore IN the universe even
/// though the hint has gone silent about it. The round probes its expected-next by exact key, finds the
/// record the listing hid, folds the `+1`, and the blob is never condemned.
TEST(CASGCFrontierGate, AKnownNamespaceIsProbedByExactKeyAndItsHiddenEdgeSavesTheBlob)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    const RootNamespace hidden{"00/hidden@cas@"};
    const RootNamespace visible{"00/visible@cas@"};
    const DB::UInt128 blob(0x5ade);

    auto store = buildCrossNamespaceScenario(backend, hidden, visible, blob, /*fold_hidden_first=*/true);
    const Layout & layout = store->layout();

    Gc gc(store, kGc);
    drive(store, gc, /*rounds*/ 5, UniversePolicy::Authoritative);

    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists)
        << "the cursor kept the namespace in the universe, so its frontier was probed and its edge folded";
}

/// ===================== THE GATE FORMULA, TERM BY TERM =====================
///
/// The healthy case first, because every suppressor arm below is only meaningful against it: a pool with
/// nothing hidden, nothing held, no anomaly, and every namespace walked to an honest end-of-stream OPENS
/// the gate and reclaims. The two booleans are read off the fold's own rows, so a formula that computed
/// them differently would fail here rather than agree with the test.
TEST(CASGCFrontierGate, AHealthyCatalogRoundOpensTheGateAndReclaims)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    const DB::UInt128 blob(0xdead);

    const ManifestRef mref = publish(*backend, layout, ns, "ref_1", 1, blob);
    dropRefTransition(*backend, layout, ns, "ref_1", mref);

    Gc gc(store, kGc);
    backend->resetCounts();
    const GateVerdict verdict = runRoundCapturingGate(store, gc, UniversePolicy::kDefault);

    ASSERT_TRUE(verdict.saw_fold) << "the round did not fold, so it published no gate verdict";
    EXPECT_TRUE(verdict.frontier_complete)
        << "every namespace in a healthy catalog universe reached a proven frontier";
    EXPECT_FALSE(verdict.suppress_destructive)
        << "no anomaly, no hold, a complete frontier -- the gate has nothing left to refuse on";
    EXPECT_GT(verdict.frontier_namespaces, 0u)
        << "a zero-namespace universe would satisfy the equality vacuously; this pool must not be one";
    EXPECT_EQ(verdict.frontier_proven, verdict.frontier_namespaces);

    /// And the gate being open is worth something: the condemned blob actually drains.
    EXPECT_TRUE(runRoundsUntilAbsent(store, gc, *backend, layout, blob))
        << "an unsuppressed round must reclaim a blob no ref owns any more";
}

/// ===================== EVERY DESTRUCTIVE SITE, INDIVIDUALLY =====================
///
/// The inventory as an assertion. The pool below has real work waiting at every gated site: a
/// graduated blob to delete, an owner-removed manifest body to delete, aged generations to prune and
/// hand off, ref logs and snapshots covered by a durable snapshot, and a removed namespace with a
/// Pending cleanup item. A suppressed round issues ZERO deletes against ALL of them, and the per-site
/// assertions name which one leaked if any does.

namespace
{
/// A pool with destructive work pending at every site, plus a few completed rounds so generations have
/// aged past the retention floor. Returns the hash of a blob whose in-degree has dropped to zero.
DB::UInt128 buildPoolWithWorkAtEverySite(const std::shared_ptr<CountingBackend> & backend,
                                         const PoolPtr & store, Gc & gc)
{
    const Layout & layout = store->layout();
    const RootNamespace live{"00/live@cas@"};
    const RootNamespace doomed{"00/doomed@cas@"};
    const DB::UInt128 blob(0xfeed);

    /// A long-lived namespace that keeps publishing, so snapshots and covered logs accumulate and
    /// generations keep advancing past the retention floor.
    for (uint64_t i = 1; i <= 4; ++i)
    {
        publish(*backend, layout, live, "ref_" + std::to_string(i), i, DB::UInt128(0x1000 + i));
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
    }

    /// The condemnable blob: published in `doomed`, then dropped. Its manifest body becomes
    /// owner-removed cleanup work at the same time.
    const ManifestRef mref = publish(*backend, layout, doomed, "doomed_ref", 9, blob);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    dropRefTransition(*backend, layout, doomed, "doomed_ref", mref);
    return blob;
}
}

/// (3a) THE NEGATIVE-POLICY SEAM, and the per-site inventory at the same time. A caller that supplies no
/// universe suppresses on that term ALONE: this pool has no anomaly, no hold, and a frontier every
/// per-namespace probe proves -- the control at the end of the test is what says so, since the identical
/// pool drains on the production path.
TEST(CASGCFrontierGate, EveryInventoriedDestructiveSiteIsInertUnderSuppression)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();

    Gc gc(store, kGc);
    const DB::UInt128 blob = buildPoolWithWorkAtEverySite(backend, store, gc);

    /// From here on the rounds supply NO universe: every site has work queued and every site must
    /// decline it.
    backend->resetCounts();
    const GateVerdict verdict = runRoundCapturingGate(store, gc, UniversePolicy::StageA_Suppressed);
    for (int i = 0; i < 5; ++i)
        runRoundCapturingGate(store, gc, UniversePolicy::StageA_Suppressed);

    ASSERT_TRUE(verdict.saw_fold) << "the round did not fold, so it published no gate verdict";
    EXPECT_FALSE(verdict.frontier_complete)
        << "with no universe supplied the frontier can never be complete, whatever the probes proved";
    EXPECT_TRUE(verdict.suppress_destructive);
    expectEveryDeleteFamilyInert(*backend, "no universe supplied");
    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists);

    /// The control: the identical pool DOES reclaim at those sites on the production path, so the zeros
    /// above are the gate at work and not an empty work queue -- and it is also what makes the "on that
    /// term alone" claim above true rather than assumed.
    drive(store, gc, /*rounds*/ 4, UniversePolicy::kDefault);
    EXPECT_GT(backend->deleteTotal(), 0u)
        << "the work queue was real -- a round with a universe drains it";
    EXPECT_FALSE(backend->head(blobKeyOf(layout, blob)).exists);
}

/// (1) ONE ANOMALY. A namespace whose `_ckpt` is present but undecodable records the "no usable
/// checkpoint" anomaly, and the round declines every site on that. It leaves the frontier incomplete too,
/// so what this arm pins is "an anomaly suppresses", not "only the anomaly does" -- which is why every
/// assertion below is about inertness and not about which term fired.
TEST(CASGCFrontierGate, AnUndecodableCheckpointAnomalySuppressesEveryDeleteFamily)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();

    Gc gc(store, kGc);
    const DB::UInt128 blob = buildPoolWithWorkAtEverySite(backend, store, gc);

    const RootNamespace damaged{"00/damaged@cas@"};
    publish(*backend, layout, damaged, "damaged_ref", 42, DB::UInt128(0xda43));
    /// Resolved through the catalog, not minted from the namespace name: the corruption has to land on
    /// the very object the round's own life resolution will read, or the round folds normally and this
    /// test measures nothing.
    const std::optional<NamespaceLifeId> damaged_life =
        CasRefCatalog::lifeIfCataloged(*backend, layout, damaged);
    ASSERT_TRUE(damaged_life.has_value()) << "the publish must have left a catalog entry to resolve";
    const std::optional<CkptSample> damaged_ckpt = readCkpt(*backend, layout, *damaged_life);
    ASSERT_TRUE(damaged_ckpt.has_value()) << "the publish must have left a `_ckpt` to damage";
    ASSERT_EQ(backend->casPut(layout.refCkptKey(*damaged_life), "not a checkpoint",
                              damaged_ckpt->token).outcome, CasOutcome::Committed);

    backend->resetCounts();
    std::vector<size_t> anomaly_counts;
    for (int i = 0; i < 6; ++i)
    {
        anomaly_counts.push_back(gc.runRegularRound().anomalies.size());
        store->renewWatermarkOnce();
    }

    EXPECT_GT(anomaly_counts.front(), 0u)
        << "the undecodable `_ckpt` must be RECORDED, not silently absorbed -- a silent exit would make "
           "this test pass for the wrong reason";
    expectEveryDeleteFamilyInert(*backend, "one anomaly");
    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists);
}

/// (2) ONE CARRIED HOLD. The gate's second term reads the SEAL, not this round's anomaly list, so the
/// round that matters here is a LATER one: the hold was detected earlier, rides forward because its
/// offending position is still unresolved, and must suppress on its own.
TEST(CASGCFrontierGate, ACarriedHoldSuppressesEveryDeleteFamily)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();

    Gc gc(store, kGc);
    const DB::UInt128 blob = buildPoolWithWorkAtEverySite(backend, store, gc);

    /// A committed gap: the checkpoint says `{1,2}` is committed while only `{1,1}` was ever written, so
    /// the walk reads `{1,2}` absent BELOW its authority ceiling and holds there. Nothing repairs it, so
    /// every later round re-detects the same position and carries the same hold.
    const RootNamespace gapped{"00/gapped@cas@"};
    publish(*backend, layout, gapped, "gapped_ref", 44, DB::UInt128(0x6a9));
    replaceRecoverableCkptForRawFixture(*backend, layout, gapped, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    std::map<String, UInt64> intake;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    gc.runRegularRound();
    gc.setPhaseSink({});
    store->renewWatermarkOnce();
    ASSERT_FALSE(intake.empty());
    ASSERT_GT(intake.at("tables_held"), 0u)
        << "the gap must be HELD, or the later rounds carry nothing and this test proves nothing";

    backend->resetCounts();
    for (int i = 0; i < 5; ++i)
    {
        gc.runRegularRound();
        store->renewWatermarkOnce();
    }
    expectEveryDeleteFamilyInert(*backend, "one carried hold");
    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists);
}

/// (3c) THE PROBE BUDGET. A namespace with a sealed cursor, no `_ckpt` and no listing left can be proven
/// only by a successor probe; a zero budget denies it one, so it counts toward the universe and not toward
/// the proofs, and the equality fails.
TEST(CASGCFrontierGate, AnExhaustedProbeBudgetSuppressesEveryDeleteFamily)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    auto store = openPoolWithProbeBudget(backend, /*budget*/ 0);
    const Layout & layout = store->layout();

    Gc gc(store, kGc);
    const DB::UInt128 blob = buildPoolWithWorkAtEverySite(backend, store, gc);

    /// The budget is spent only on a namespace the round knows about and can reach NO other way. Three
    /// conditions, and all three are load-bearing: a SEALED cursor (an unhinted namespace with no cursor
    /// is a no-genesis shape the budget never reaches), NO listing (a hinted target is walked for free),
    /// and NO readable `_ckpt` (a recoverable checkpoint proves the frontier without spending a probe --
    /// which is why publishing and hiding alone leaves the namespace provable and measures nothing).
    const RootNamespace quiet{"00/quiet@cas@"};
    publish(*backend, layout, quiet, "quiet_ref", 43, DB::UInt128(0x9a1e));
    runRoundCapturingGate(store, gc, UniversePolicy::StageA_Suppressed);
    ASSERT_NE(sealedCursorOf(*backend, layout, quiet), (RefTxnId{}))
        << "without a sealed cursor the namespace never becomes a budget-spending probe target";

    const std::optional<NamespaceLifeId> quiet_life =
        CasRefCatalog::lifeIfCataloged(*backend, layout, quiet);
    ASSERT_TRUE(quiet_life.has_value());
    const std::optional<CkptSample> quiet_ckpt = readCkpt(*backend, layout, *quiet_life);
    ASSERT_TRUE(quiet_ckpt.has_value()) << "there must be a `_ckpt` to remove";
    ASSERT_EQ(backend->deleteExact(layout.refCkptKey(*quiet_life), quiet_ckpt->token).kind,
              DeleteOutcome::Kind::Deleted);
    backend->hidePrefix(layout.namespaceStreamPrefix(*quiet_life));

    backend->resetCounts();
    const GateVerdict verdict = runRoundCapturingGate(store, gc, UniversePolicy::kDefault);
    for (int i = 0; i < 5; ++i)
        runRoundCapturingGate(store, gc, UniversePolicy::kDefault);

    ASSERT_TRUE(verdict.saw_fold);
    EXPECT_GT(verdict.frontier_unprobed_budget, 0u)
        << "this arm must suppress on the BUDGET term; a zero here means some other suppressor fired and "
           "the test would pass without ever exhausting a budget";
    EXPECT_LT(verdict.frontier_proven, verdict.frontier_namespaces)
        << "the unprobed namespace must count toward the universe and not toward the proofs";
    EXPECT_FALSE(verdict.frontier_complete);
    EXPECT_TRUE(verdict.suppress_destructive);
    expectEveryDeleteFamilyInert(*backend, "exhausted probe budget");
    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists);
}

/// `frontier_proven == frontier_namespaces` is `0 == 0` -- vacuously TRUE -- on an empty universe, which
/// is not by itself a proof of anything: a fresh pool, a damaged catalog, and a genuinely emptied pool
/// all produce the same zeros. The gate's non-vacuity term therefore has TWO ways to be satisfied:
/// `frontier_namespaces > 0` (an ordinary nonempty pool, everything proven), or the round's own hot-scan
/// catalog cut positively proving the universe empty (present, token-bearing, decoded, zero rows of
/// every lifecycle state). The next two tests are that positive/negative pair.
TEST(CASGCFrontierGate, ADecodedTokenBearingEmptyCatalogCompletesTheFrontierAndDrainsRetiredWork)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const DB::UInt128 blob(0xbead);

    /// Built to make `frontier_namespaces` GENUINELY zero by every source that feeds it -- no catalog
    /// entry (this pool never admitted a namespace), no sealed cursor, no ref-log hint -- while a
    /// condemned blob with a real, present body and in-degree 0 sits queued exactly the way a real
    /// round leaves one (`injectRetire`).
    writeBlobBody(*backend, layout, blob);
    const BlobRef blob_ref = legacyMetaTestRef(blob);
    const Token blob_token = backend->head(layout.blobKey(blob_ref)).token;
    injectRetire(*backend, layout, /*round*/ 1, /*shard*/ 0,
        {RetiredEntry{.kind = ObjectKind::Blob, .ref = blob_ref, .token = blob_token, .size = 0}});
    store->renewWatermarkOnce();

    ASSERT_TRUE(CasRefCatalog::read(*backend, layout).catalog.entries.empty())
        << "the scenario needs a genuinely, provably empty catalog, or this test measures nothing";

    Gc gc(store, kGc);
    backend->resetCounts();

    /// `injectRetire` seeds the condemned entry WITHOUT the durable per-hash `Condemned` meta a real
    /// condemn round writes (`fold`'s own `scheduleCondemnMarkerWrite` side effect) -- so this fixture's
    /// round cadence is longer than the textbook condemn->graduate->delete: the lease is UNCLAIMED
    /// (`injectRetire` writes `gc/state` directly, never through a real acquire), so the first round
    /// only arms `acquireOrRenewLease`'s two-tick steal-safety window; the graduation gate's own
    /// `confirm_condemned_marker` then fails its first sighting, schedules the meta write, and CARRIES
    /// the entry (not yet delete_pending) while `meta_pool_wait` lands it durably by that round's end;
    /// only the round after that sees the durable meta and actually graduates; and the physical delete
    /// is the round after THAT. MEASURED (phase-sink instrumentation, not a guess): round 1 arms
    /// (`saw_fold == false`), round 2 is the first to fold with the gate open while the blob is still
    /// present (the carry round), round 3 graduates, round 4 executes the delete -- four rounds exactly.
    /// Bound the drive at that plus one (5): enough slack for the fixture's own cadence to be measured
    /// without hand-counting rounds against this gate, but tight enough that a real regression in the
    /// confirm/retry cadence still fails loudly instead of silently absorbing into a generous loop.
    ASSERT_TRUE(backend->head(layout.blobKey(blob_ref)).exists)
        << "the scenario starts with the condemned blob present, or the loop below measures nothing";

    constexpr int kMaxRounds = 5;   /// measured cadence (4) + 1; see the comment above
    /// Observe the TWO-PHASE PIPELINE explicitly: an open, unsuppressed gate while the blob was still
    /// present (the graduate side), and only a STRICTLY LATER round removing it (the delete side).
    /// Asserting only the final state and the last verdict would pass just as readily if the blob
    /// vanished by some other path entirely.
    int round_gate_opened_while_present = -1;   /// the graduate side: FIRST round that folded, unsuppressed,
                                                 /// with the blob still present
    int round_blob_vanished = -1;               /// the delete side
    GateVerdict last;
    int rounds_run = 0;
    for (int i = 0; i < kMaxRounds && backend->head(layout.blobKey(blob_ref)).exists; ++i)
    {
        last = runRoundCapturingGate(store, gc, UniversePolicy::Authoritative);
        ++rounds_run;
        const bool still_present = backend->head(layout.blobKey(blob_ref)).exists;
        if (round_gate_opened_while_present < 0 && last.saw_fold && !last.suppress_destructive && still_present)
            round_gate_opened_while_present = rounds_run;
        if (round_blob_vanished < 0 && !still_present)
            round_blob_vanished = rounds_run;
    }

    ASSERT_GT(rounds_run, 0) << "the loop must actually run, or every assertion below is vacuous";
    ASSERT_LE(rounds_run, kMaxRounds)
        << "the drain took more than the measured cadence -- this is a real regression in the "
           "confirm/retry pacing, not something to hide by bumping the bound; re-derive the cadence";
    ASSERT_TRUE(last.saw_fold);
    EXPECT_EQ(last.frontier_namespaces, 0u);
    EXPECT_EQ(last.frontier_proven, 0u);
    EXPECT_TRUE(last.catalog_proved_empty)
        << "the catalog cut itself must be the proof, not the bare 0==0 equality";
    EXPECT_TRUE(last.frontier_complete);
    EXPECT_FALSE(last.suppress_destructive);
    ASSERT_GT(round_gate_opened_while_present, 0)
        << "the gate must have opened (folded, unsuppressed) at least one round BEFORE the blob was "
           "gone -- the graduate side of the two-phase pipeline -- not just at the round that deleted it";
    ASSERT_GT(round_blob_vanished, round_gate_opened_while_present)
        << "the delete must be a round STRICTLY LATER than the one that opened the gate, never the same "
           "round -- a round that both graduates and deletes in one step would hide the two-phase split";
    EXPECT_FALSE(backend->head(layout.blobKey(blob_ref)).exists)
        << "a proved-empty universe is a COMPLETE frontier, not a suppressed one -- the condemned blob "
           "must drain through the ordinary two-phase pipeline instead of leaking forever";
}

/// The negative half of the pair. A `Creating` row is a birth in progress (spec §3: no publication can
/// exist under it, so `live_incarnation`/`walk_plan.lives()` exclude it -- see the R10 comment above the
/// intake loop), not an empty universe -- but it produces the SAME `frontier_namespaces ==
/// frontier_proven == 0` a genuinely empty catalog does. Only `catalog_cut_proved_empty` tells them
/// apart, because it reads `entries` (every lifecycle state), not the frontier counters.
TEST(CASGCFrontierGate, AZeroWalkableFrontierWithACreatingCatalogRowIsNotProvedEmpty)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const DB::UInt128 blob(0xbead);

    writeBlobBody(*backend, layout, blob);
    const BlobRef blob_ref = legacyMetaTestRef(blob);
    const Token blob_token = backend->head(layout.blobKey(blob_ref)).token;
    injectRetire(*backend, layout, /*round*/ 1, /*shard*/ 0,
        {RetiredEntry{.kind = ObjectKind::Blob, .ref = blob_ref, .token = blob_token, .size = 0}});
    store->renewWatermarkOnce();

    const RootNamespace stalled{"00/stalled@cas@"};
    CatalogEntry entry;
    entry.ns = stalled;
    entry.state = NsState::Creating;
    entry.incarnation = hexToU128("00000000000000000000000000000042");
    entry.creator = CreatorFence{
        .server_root_id = "test-stalled-creator", .writer_epoch = 1, .fence_generation = 1};
    CasRefCatalog::casAdmitEntry(*backend, layout, /*gc_shards*/ 1, entry);

    Gc gc(store, kGc);
    backend->resetCounts();
    /// `injectRetire` leaves `gc/state`'s lease unclaimed (owner 0); the first round only arms the
    /// steal-safety window (see `ADecodedTokenBearingEmptyCatalogCompletesTheFrontierAndDrainsRetiredWork`)
    /// and folds nothing, so it is spent here rather than counted among the assertions below.
    const GateVerdict warm_up = runRoundCapturingGate(store, gc, UniversePolicy::Authoritative);
    EXPECT_FALSE(warm_up.saw_fold);
    for (int i = 0; i < 6; ++i)
    {
        const GateVerdict v = runRoundCapturingGate(store, gc, UniversePolicy::Authoritative);
        ASSERT_TRUE(v.saw_fold);
        EXPECT_EQ(v.frontier_namespaces, 0u);
        EXPECT_EQ(v.frontier_proven, 0u);
        EXPECT_FALSE(v.catalog_proved_empty)
            << "a Creating-only catalog is a birth in progress, not proof of an empty universe";
        EXPECT_FALSE(v.frontier_complete);
        EXPECT_TRUE(v.suppress_destructive);
    }
    expectEveryDeleteFamilyInert(*backend, "Creating-only catalog");
    EXPECT_TRUE(backend->head(layout.blobKey(blob_ref)).exists);
}

/// The bootstrap-only absent-as-empty representation (`initializeEmptyForNewPool`) must never leak into
/// the operational round: an absent mandatory catalog is corruption, never an empty authority set.
TEST(CASGCFrontierGate, AnAbsentCatalogNeverReadsAsAnEmptyUniverse)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();

    const Token catalog_token = backend->head(layout.refCatalogKey()).token;
    ASSERT_EQ(backend->deleteExact(layout.refCatalogKey(), catalog_token).kind, DeleteOutcome::Kind::Deleted);

    Gc gc(store, kGc);
    backend->resetCounts();
    bool saw_fold = false;
    gc.setPhaseSink([&](const GcPhaseRecord & rec) { if (rec.phase == "fold_reduce") saw_fold = true; });
    try
    {
        gc.runRegularRound({}, /*allow_steal*/true, UniversePolicy::Authoritative);
        FAIL() << "expected the missing mandatory catalog to throw before any fold gate verdict";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
    }
    gc.setPhaseSink({});
    EXPECT_FALSE(saw_fold) << "an absent catalog must abort before the round computes any gate verdict";
    EXPECT_EQ(backend->deleteTotal(), 0u) << "no destructive work may run on an unauthorized round";
}

/// A malformed, truncated, wrong-typed, count-mismatched, or future-versioned catalog must not decode
/// into a `Snapshot` at all -- these are the replacement guards for R11's original "damaged catalog"
/// concern, now that the empty case has a positive proof to keep separate from a broken one.
/// One table-driven test: each row installs a different broken body at the mandatory key and expects
/// decode failure before any destructive work.
///
/// NOT covered here: a header version BELOW `RefCatalog`'s own birth generation. `checkCompatibility`
/// today only rejects a version ABOVE `G_BUILD`; a version below a type's birth floor decodes as if it
/// were legal, and `decodeRefCatalog` discards the parsed header entirely, so "decoded successfully"
/// does not yet imply "legal version for this type". That gap is not load-bearing for THIS proof: the
/// proof is token-present + a full structural decode (type, complete records, matching count trailer,
/// no trailing bytes) + zero entries, and a well-formed-but-out-of-protocol empty catalog is already an
/// accepted residual under the trusted-store model (the token proves byte identity, not history) --
/// closing the version floor would only shrink that residual, not remove it. Tracked separately as
/// `[cas-format-version-floor]` in `BACKLOG.md`; deliberately out of scope for this gate.
TEST(CASGCFrontierGate, AMalformedCatalogNeverDecodesIntoAnEmptyProof)
{
    /// A one-entry catalog's canonical bytes, the base every mutation below starts from.
    RefCatalog one_entry;
    CatalogEntry entry;
    entry.ns = RootNamespace{"00/malformed-base@cas@"};
    entry.state = NsState::Live;
    entry.incarnation = hexToU128("00000000000000000000000000000099");
    one_entry.entries.push_back(entry);
    const String base = encodeRefCatalog(one_entry);
    const String empty_base = encodeRefCatalog(RefCatalog{});

    const String type_needle = fmt::format("\"type\":\"{}\"", traitsFor(FormatId::RefCatalog).type);
    ASSERT_NE(empty_base.find(type_needle), String::npos);
    const String version_needle = fmt::format("\"v\":{}", currentCompatibilityVersion());
    ASSERT_NE(empty_base.find(version_needle), String::npos);
    ASSERT_NE(base.find("\"n\":1"), String::npos);

    const auto replaceOnce = [](const String & haystack, const String & needle, const String & replacement) -> String
    {
        const auto pos = haystack.find(needle);
        EXPECT_NE(pos, String::npos) << "expected to find '" << needle << "'";
        String out = haystack;
        out.replace(pos, needle.size(), replacement);
        return out;
    };

    struct Case { const char * name; String bytes; };
    const std::vector<Case> cases = {
        {"wrong-type", replaceOnce(empty_base, type_needle, "\"type\":\"cas_ref_ckpt\"")},
        /// The one version case the CURRENT (unmodified) gate actually enforces: a version ABOVE
        /// `G_BUILD` is refused by `checkCompatibility` before decode proceeds.
        {"future-version", replaceOnce(empty_base, version_needle, "\"v\":999999")},
        {"trailer-count-mismatch", replaceOnce(base, "\"n\":1", "\"n\":2")},
        /// The trailer line entirely gone: decode's post-entry loop expects another line and hits EOF.
        {"missing-trailer", base.substr(0, base.rfind("{\"n\":1}\n"))},
        /// The trailer present but its own line has no terminator: EOF strictly inside a line.
        {"truncated-mid-line", base.substr(0, base.size() - 2)},
    };

    for (const Case & c : cases)
    {
        auto backend = std::make_shared<CountingBackend>();
        auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
        const Layout & layout = store->layout();
        const Token bootstrap_token = backend->head(layout.refCatalogKey()).token;
        ASSERT_EQ(backend->casPut(layout.refCatalogKey(), c.bytes, bootstrap_token).outcome,
            CasOutcome::Committed) << c.name;

        Gc gc(store, kGc);
        backend->resetCounts();
        bool saw_fold = false;
        gc.setPhaseSink([&](const GcPhaseRecord & rec) { if (rec.phase == "fold_reduce") saw_fold = true; });
        EXPECT_THROW(
            gc.runRegularRound({}, /*allow_steal*/true, UniversePolicy::Authoritative), DB::Exception)
            << c.name;
        gc.setPhaseSink({});
        EXPECT_FALSE(saw_fold) << c.name << ": a broken catalog must abort before any fold gate verdict";
        EXPECT_EQ(backend->deleteTotal(), 0u) << c.name << ": no destructive work may run on it";
    }
}

/// `StageA_Suppressed` refuses outright regardless of what the catalog proves -- a proved-empty cut
/// satisfies the frontier term but is not the only term the gate reads.
TEST(CASGCFrontierGate, AProvedEmptyCatalogUnderStageASuppressedStaysSuppressed)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const DB::UInt128 blob(0xbead);

    writeBlobBody(*backend, layout, blob);
    const BlobRef blob_ref = legacyMetaTestRef(blob);
    const Token blob_token = backend->head(layout.blobKey(blob_ref)).token;
    injectRetire(*backend, layout, /*round*/ 1, /*shard*/ 0,
        {RetiredEntry{.kind = ObjectKind::Blob, .ref = blob_ref, .token = blob_token, .size = 0}});
    store->renewWatermarkOnce();

    Gc gc(store, kGc);
    backend->resetCounts();
    /// `injectRetire` leaves `gc/state`'s lease unclaimed (owner 0); the first round only arms the
    /// steal-safety window (see `ADecodedTokenBearingEmptyCatalogCompletesTheFrontierAndDrainsRetiredWork`)
    /// and folds nothing, independent of policy -- lease acquisition precedes the destructive gate.
    const GateVerdict warm_up = runRoundCapturingGate(store, gc, UniversePolicy::StageA_Suppressed);
    EXPECT_FALSE(warm_up.saw_fold);
    for (int i = 0; i < 6; ++i)
    {
        const GateVerdict v = runRoundCapturingGate(store, gc, UniversePolicy::StageA_Suppressed);
        ASSERT_TRUE(v.saw_fold);
        EXPECT_TRUE(v.catalog_proved_empty)
            << "the catalog cut is still genuinely empty -- the fact does not depend on policy";
        EXPECT_FALSE(v.frontier_complete)
            << "StageA_Suppressed refuses outright no matter what the catalog cut proves";
        EXPECT_TRUE(v.suppress_destructive);
    }
    expectEveryDeleteFamilyInert(*backend, "StageA_Suppressed over a proved-empty catalog");
    EXPECT_TRUE(backend->head(layout.blobKey(blob_ref)).exists);
}

/// THE BIRTH-AFTER-EMPTY-CUT BLOB RACE. The proved-empty exception's soundness rests on one hard fact:
/// under this pool's protocol every live or live-precommit edge requires an exact `Live` catalog row
/// (INV-3), so a catalog cut with zero rows proves no namespace ANYWHERE holds one -- AT THAT INSTANT.
/// A namespace born strictly after the cut is invisible to the round that took it; safety for blob
/// CONTENT then rests entirely on the condemned-marker/resurrection protocol (EDGE-BEFORE-OBSERVE:
/// `ContentAddressedTransaction.cpp` persists the precommit edge before observing/uploading the pool
/// blob), never on the frontier proof, which by construction cannot see a birth postdating its own cut.
/// This test pins that: a real writer, through the production `createNamespace` lifecycle
/// (`precommitAdd` on a namespace that has never existed), lands a precommit edge to an
/// ALREADY-CONDEMNED blob strictly after round R's catalog cut but strictly before round R executes the
/// pending delete that cut licensed.
TEST(CASGCFrontierGate, ANamespaceBornAfterTheEmptyCutResurrectsTheCondemnedBlobInstead)
{
    ensureBlobUploadPoolForTest();

    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace doomed{"00/doomed@cas@"};

    const String payload = "empty-cut-birth-race-payload";
    const DB::UInt128 hash = u128Of(payload);
    const BlobRef id = idOf(payload);
    const String key = layout.blobKey(id);
    String raw_body(store->poolMeta().blob_header_len, '\0');
    raw_body += payload;
    writeRawBlobBody(*backend, layout, hash, raw_body);

    const ManifestRef mref{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1};
    writeManifestRaw(*backend, layout, doomed, mref, {blobEntryFor("data.bin", hash)});
    publishCommittedTransition(*backend, layout, doomed, "ref_1", std::nullopt, mref);

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);                                 /// folds the +1 edge
    store->renewWatermarkOnce();
    dropRefTransition(*backend, layout, doomed, "ref_1", mref);
    runRegularRoundReclaiming(gc);                                 /// condemns: durable Condemned meta
    store->renewWatermarkOnce();
    const Token condemned_token = backend->head(key).token;
    const auto condemned_meta = loadMetaForTest(*backend, layout, hash);
    ASSERT_TRUE(condemned_meta.has_value());
    ASSERT_EQ(condemned_meta->meta.state, MetaState::Condemned)
        << "the delete round R is about to execute must be backed by durable Condemned evidence";
    runRegularRoundReclaiming(gc);                                 /// graduates: publishes delete_pending
    store->renewWatermarkOnce();

    /// Remove `doomed` entirely -- the ONLY way the catalog can become genuinely, provably empty. A raw
    /// `RemoveNamespace` op plus the Removing-state CAS mirrors
    /// `CleanupEvidenceLeavesRemovedNamespaceCheckpointForJanitor`'s own recipe exactly.
    RefOp remove_op;
    remove_op.kind = RefOpKind::RemoveNamespace;
    const uint64_t remove_seq = appendRefLogSeed(*backend, layout, doomed, {remove_op});
    publishRecoverableCkptForSemanticWrapper(*backend, layout, doomed, RefTxnId{1, remove_seq});
    CasRefCatalog::casUpdate(*backend, layout, [&](const RefCatalog & current) -> RefCatalog
    {
        RefCatalog next = current;
        const auto it = std::find_if(next.entries.begin(), next.entries.end(),
            [&](const CatalogEntry & e) { return e.ns == doomed; });
        EXPECT_NE(it, next.entries.end());
        it->state = NsState::Removing;
        it->removal_started_round = 1;
        return next;
    });

    /// A SUPPRESSED round folds `doomed` through its removal terminal and records `cleanup_evidence` in
    /// the seal, WITHOUT executing the delete_pending the graduate round above published -- suppressed
    /// rounds carry pending deletes forward untouched. `StageA_Suppressed` here is the test's OWN
    /// control over timing, not the scenario under test: it exists only to keep blob X's delete pending
    /// until round R below, rather than letting it drain the ordinary way while `doomed` is still Live.
    gc.runRegularRound({}, /*allow_steal*/true, UniversePolicy::StageA_Suppressed);
    store->renewWatermarkOnce();
    EXPECT_TRUE(backend->head(key).exists) << "the pending delete must still be carried, not yet run";

    /// Round R: its pre-fold drain (`drainCompletedRemoving`) reads the round just above's
    /// `cleanup_evidence` and drops `doomed`'s catalog row BEFORE this round's own hot-scan `GET` --
    /// so round R's catalog cut is the first one that is genuinely, provably empty. The hook fires the
    /// instant that cut is taken and races a real namespace birth into the window before round R's own
    /// pre-CAS delete phase runs.
    bool hook_fired = false;
    Token fresh_token{};
    gc.setPostHotScanCatalogReadHookForTest([&]()
    {
        hook_fired = true;
        ASSERT_TRUE(CasRefCatalog::read(*backend, layout).catalog.entries.empty())
            << "the race must land inside the window where the cut itself is already empty";

        const RootNamespace newborn{"00/newborn@cas@"};
        auto build = store->beginPartWrite(
            PartWriteInfo{.intended_ref = newborn.string() + "/ref_1", .intended_namespace = newborn});
        const ManifestId new_id = build->stageManifest({blobEntryFor("data.bin", hash)});
        build->precommitAdd(newborn, "ref_1", new_id);            /// mints `newborn` via real createNamespace
        const PutBlobResult uploaded = build->putBlob(id, BlobSource::fromString(payload));
        EXPECT_EQ(uploaded.ref, id);
        fresh_token = backend->head(key).token;
        EXPECT_NE(fresh_token, condemned_token)
            << "the writer must have observed Condemned and resurrected -- a fresh token, not an adopt "
               "of the dying incarnation";
    });

    const GateVerdict verdict = runRoundCapturingGate(store, gc, UniversePolicy::Authoritative);
    ASSERT_TRUE(hook_fired) << "the race hook never fired -- this test proves nothing about the race";
    ASSERT_TRUE(verdict.saw_fold);
    EXPECT_TRUE(verdict.catalog_proved_empty);
    EXPECT_TRUE(verdict.frontier_complete);
    EXPECT_FALSE(verdict.suppress_destructive);

    EXPECT_TRUE(backend->head(key).exists)
        << "the resurrected incarnation must survive round R's delete";
    EXPECT_EQ(backend->head(key).token, fresh_token) << "and it is still the writer's incarnation";
    EXPECT_EQ(backend->deleteExact(key, condemned_token).kind, DeleteOutcome::Kind::TokenMismatch)
        << "the condemned token can never remove the fresh object (INV_NO_LOSS)";

    /// A later round's own fresh catalog cut names `newborn`, folds its `+1`, and the blob's frontier is
    /// intact going forward.
    const GateVerdict later = runRoundCapturingGate(store, gc, UniversePolicy::Authoritative);
    ASSERT_TRUE(later.saw_fold);
    EXPECT_EQ(later.frontier_namespaces, 1u);
    EXPECT_EQ(later.frontier_proven, 1u);
    EXPECT_TRUE(backend->head(key).exists) << "the newly folded owner keeps the blob alive";
}

/// The generation prune's cursor must not move on a suppressed round either. It is a monotone
/// high-water mark that the wholesale prune never revisits, so a cursor that advanced past a generation
/// this round declined to delete would strand that generation's whole prefix with no reclaimer left.
TEST(CASGCFrontierGate, ASuppressedRoundDoesNotAdvanceTheGenerationPruneCursor)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    Gc gc(store, kGc);
    for (uint64_t i = 1; i <= 6; ++i)
    {
        publish(*backend, layout, ns, "ref_" + std::to_string(i), i, DB::UInt128(0x2000 + i));
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
    }
    const uint64_t pruned_through_before =
        decodeGcState(backend->get(layout.gcStateKey())->bytes).snap_pruned_through;

    for (uint64_t i = 7; i <= 10; ++i)
    {
        publish(*backend, layout, ns, "ref_" + std::to_string(i), i, DB::UInt128(0x2000 + i));
        gc.runRegularRound({}, /*allow_steal*/true, UniversePolicy::StageA_Suppressed);
        store->renewWatermarkOnce();
    }

    EXPECT_EQ(decodeGcState(backend->get(layout.gcStateKey())->bytes).snap_pruned_through,
              pruned_through_before)
        << "the retention cursor is a high-water mark; it may not pass a generation nothing deleted";
}

/// THE HAND-OFF RECLAIM, WHICH THE INVENTORY TEST ABOVE CANNOT REACH. This site only fires for a
/// generation the wholesale prune SKIPPED while a live ref still pinned it (so the retention cursor
/// moved past it and will never revisit it) and which a later round's ref then moves off. Building that
/// takes a deliberately idle shard and a retention cursor driven past it, which is why it gets its own
/// test rather than riding on the inventory pool.
///
/// It is reachable under suppression precisely because FOLDING still happens on a suppressed round: the
/// ref moves off the old generation exactly as it would otherwise, and only the reclaim is withheld.
TEST(CASGCFrontierGate, TheHandOffReclaimIsInertUnderSuppression)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .gc_snapshot_generations_to_keep = 1, .gc_fold_max_defer_rounds = 0});
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef r1 = publish(*backend, layout, ns, "tbl", 1, DB::UInt128(0xa1));

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    const uint64_t old_gen = decodeGcState(backend->get(layout.gcStateKey())->bytes).snap_generation;
    const String old_prefix = layout.gcGenPrefix(old_gen);
    ASSERT_FALSE(backend->list(old_prefix, "", 1000).keys.empty());

    /// Idle-carry the ref until the retention cursor is strictly PAST its generation. Until then an
    /// ordinary prune could still reclaim it and the hand-off would not be the load-bearing path.
    for (int i = 0; i < 6; ++i)
    {
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
    }
    ASSERT_GT(decodeGcState(backend->get(layout.gcStateKey())->bytes).snap_pruned_through, old_gen)
        << "the generation must be behind the retention cursor before the hand-off is exercised";
    ASSERT_FALSE(backend->list(old_prefix, "", 1000).keys.empty())
        << "and still retained, because a live ref pins it";

    /// A real delta moves the shard's run off the old generation. This is the round the hand-off would
    /// reclaim it on -- and it supplies no universe.
    const ManifestRef r2{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 1};
    writeBlobBody(*backend, layout, DB::UInt128(0xb2));
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("data.bin", DB::UInt128(0xb2))});
    publishCommittedTransition(*backend, layout, ns, "tbl", r1, r2);

    backend->resetCounts();
    gc.runRegularRound({}, /*allow_steal*/true, UniversePolicy::StageA_Suppressed);

    EXPECT_EQ(backend->deleteCountForKeysContaining("/gc/gen/"), 0u)
        << "a suppressed round hands nothing off. Deleted:" << deletedKeysMessage(*backend);
    EXPECT_FALSE(backend->list(old_prefix, "", 1000).keys.empty())
        << "the superseded generation's prefix survives a suppressed round intact";

    /// AND THE OPPORTUNITY IS CONSUMED, NOT DEFERRED -- the one place in this task where the gate
    /// costs something permanent, so it is asserted here rather than left to be discovered later.
    ///
    /// The hand-off is a one-shot DIFFERENCE: it compares the PARENT seal's runs against the new
    /// seal's, and the suppressed round above already folded the delta, so the next round's parent
    /// seal no longer mentions the old generation. Nothing revisits it -- the retention cursor is
    /// already past it and the prune never goes back. The prefix is left to `fsck`, which is exactly
    /// the outcome the site's own doc comment already records for a crash in the same window ("the
    /// cursor already advanced, so a plain retry will NOT re-attempt it; fsck is the backstop").
    /// Bounded (one small run per shard per occurrence) and not a correctness problem.
    ///
    /// The hand-off itself is not going untested: `CASGCRetention.HandOffDeletesSupersededRef` drives
    /// the same transition on an authoritative round and asserts the prefix IS reclaimed.
    runRegularRoundReclaiming(gc);
    EXPECT_FALSE(backend->list(old_prefix, "", 1000).keys.empty())
        << "the hand-off is a one-shot difference: the suppressed round consumed it, so the prefix is "
           "now fsck's problem rather than a later round's";
}

/// THE ORPHAN-MANIFEST SWEEP, which the inventory pool above also cannot reach: it only deletes bodies
/// that no ref names AND whose build is provably dead by the durable watermark floor, so it needs a
/// pool seeded with exactly that -- orphan bodies and a floor above them.
///
/// It is gated with its CURSOR, not just its deletes. The cursor paces a cold-prefix enumeration and
/// nothing revisits a range it passed, so advancing it on a round that swept nothing would silently
/// skip that range forever. A suppressed round therefore declines the whole pass.
TEST(CASGCFrontierGate, TheOrphanManifestSweepAndItsCursorAreInertUnderSuppression)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "gc-runner",
                   .manifest_sweep_list_budget_keys = 1, .manifest_sweep_delete_budget_keys = 1,
                   .gc_fold_max_defer_rounds = 0});
    const Layout & layout = store->layout();
    const RootNamespace ns{"test/aa@cas@"};
    /// The control arm below needs a recoverable catalog life whose frontier is exactly the carried
    /// cursor. An empty non-seal transaction is a valid genesis that recovers to an empty table while
    /// leaving the manifest epoch below the cursor's epoch.
    fixture::admitLive(*backend, layout, ns);
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(),
        .txn_id = RefTxnId{6, 1},
        .ops = {},
        .prev_epoch_seal = std::nullopt,
    });
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 6,
        .committed_through = RefTxnId{6, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    /// Two manifest bodies no ref ever named, under a build the durable floor has already passed.
    const ManifestRef r1{.writer_epoch = 5, .build_sequence = 0xCA01, .manifest_ordinal = 1};
    const ManifestRef r2{.writer_epoch = 5, .build_sequence = 0xCA02, .manifest_ordinal = 1};
    writeManifestRaw(*backend, layout, ns, r1, {blobEntryFor("a", DB::UInt128(0xa1))});
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("b", DB::UInt128(0xb2))});
    setWatermarkMinActive(*backend, layout, "test", r1.writer_epoch, /*min_active*/ 0xCA03);
    /// The §6 deletion premise is a second precondition on the CONTROL arm below: a manifest of an
    /// epoch-`E` build is deletable only once the namespace's sealed fold cursor sits in an epoch
    /// strictly above `E`. Sealing that cursor here is what keeps this test about the GATE — without it
    /// the control arm would stop deleting for the premise's reason, and a removed gate would no longer
    /// show up as a difference between the two arms. A real round rewrites this row with the same cursor
    /// (the namespace is known, quiet and unheld, so the walk probes `cursor+1`, finds the frontier and
    /// carries the cursor), so the seeded fact survives every round below.
    seedFoldCursorForTest(*backend, layout, ns, RefTxnId{r1.writer_epoch + 1, 1});

    Gc gc(store, kGc);
    backend->resetCounts();
    for (int i = 0; i < 4; ++i)
    {
        gc.runRegularRound({}, /*allow_steal*/true, UniversePolicy::StageA_Suppressed);
        store->renewWatermarkOnce();
    }

    EXPECT_EQ(backend->deleteCountForKeysContaining("/cas/manifests/"), 0u)
        << "a suppressed round sweeps nothing. Deleted:" << deletedKeysMessage(*backend);
    EXPECT_TRUE(backend->head(layout.manifestKey(ManifestId{ns, r1})).exists);
    EXPECT_TRUE(backend->head(layout.manifestKey(ManifestId{ns, r2})).exists);
    EXPECT_TRUE(decodeGcState(backend->get(layout.gcStateKey())->bytes).manifest_sweep_cursor.empty())
        << "the sweep cursor must not advance over a range the round declined to sweep -- nothing "
           "revisits it";

    /// The control: the same orphans ARE swept once the universe is authoritative.
    for (int i = 0; i < 4; ++i)
    {
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
    }
    EXPECT_FALSE(backend->head(layout.manifestKey(ManifestId{ns, r1})).exists);
    EXPECT_FALSE(backend->head(layout.manifestKey(ManifestId{ns, r2})).exists);
}


/// ===================== QUIET NAMESPACES AND THE PROBE BUDGET =====================

/// THE TALLY ARITHMETIC, at a PARTIAL budget — the case neither 0 nor the default reaches.
///
/// `frontier_namespaces` is the denominator an operator reads as "the round's universe", and the
/// integration test reads it too. A valid checkpoint at every quiet namespace's carried cursor is
/// authoritative independently of LIST and the probe budget: all three lives are proven without
/// successor probes, so the budget leaves no namespace unprobed.
TEST(CASGCFrontierGate, APartialProbeBudgetPublishesATallyThatMatchesTheSealedSet)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    auto store = openPoolWithProbeBudget(backend, /*budget*/ 1);
    const Layout & layout = store->layout();
    const RootNamespace a{"00/quiet_a@cas@"};
    const RootNamespace b{"00/quiet_b@cas@"};
    const RootNamespace c{"00/quiet_c@cas@"};

    for (const RootNamespace & ns : {a, b, c})
        publish(*backend, layout, ns, "ref_1", 1, DB::UInt128(0x300 + ns.string().size()));

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    for (const RootNamespace & ns : {a, b, c})
        ASSERT_NE(sealedCursorOf(*backend, layout, ns), (RefTxnId{})) << ns.string();

    /// All three go unhinted at once. Their valid checkpoint frontiers still prove their carried
    /// cursors, so this does not consume the successor-probe budget.
    for (const RootNamespace & ns : {a, b, c})
        backend->hidePrefix(layout.namespaceStreamPrefix(fixture::fixtureLife(ns)));

    std::map<String, UInt64> intake;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    runRegularRoundReclaiming(gc);
    gc.setPhaseSink({});

    ASSERT_FALSE(intake.empty()) << "the intake phase must have emitted its row";
    EXPECT_EQ(intake["unhinted_quiet_walked"], 3u)
        << "a valid checkpoint frontier makes every quiet life eligible without a successor probe";
    EXPECT_EQ(intake["frontier_unprobed_budget"], 0u)
        << "the CTE authority, not the probe budget, decides these quiet lives";
    EXPECT_EQ(intake["frontier_proven"], 3u)
        << "each carried cursor equals its valid checkpoint frontier";
    EXPECT_EQ(intake["frontier_namespaces"], 3u)
        << "the denominator is the complete authoritative set of sealed quiet lives";
    EXPECT_EQ(intake["frontier_proven"], intake["frontier_namespaces"])
        << "a valid CTE frontier remains authoritative even when LIST omits every namespace";

    /// And the seal really does carry all three rows — the denominator's claim, checked against the
    /// object it describes rather than against another counter.
    for (const RootNamespace & ns : {a, b, c})
    {
        EXPECT_NE(sealedCursorOf(*backend, layout, ns), (RefTxnId{}))
            << "every namespace in the tally must have a sealed cursor: " << ns.string();
        const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
        const auto checkpoint = readCkpt(*backend, layout, life);
        ASSERT_TRUE(checkpoint.has_value());
        EXPECT_EQ(checkpoint->ckpt.committed_through, (RefTxnId{1, 1}))
            << "LIST omission and the probe budget do not alter a valid CTE";
    }
}

/// A checkpoint boundary already equal to the carried cursor proves a quiet catalog life complete;
/// GC must not manufacture a successor `GET` merely because its LIST is empty.
TEST(CASGCFrontierGate, AQuietKnownNamespaceAtItsCheckpointFrontierCostsNoSuccessorGet)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace quiet{"00/quiet@cas@"};

    publish(*backend, layout, quiet, "ref_1", 1, DB::UInt128(0x11));
    replaceRecoverableCkptForRawFixture(*backend, layout, quiet, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();

    const RefTxnId sealed = sealedCursorOf(*backend, layout, quiet);
    ASSERT_NE(sealed, (RefTxnId{})) << "the seeding round must have sealed a cursor to carry";

    /// Now the store stops listing the namespace entirely.
    backend->hidePrefix(layout.namespaceStreamPrefix(fixture::fixtureLife(quiet)));
    backend->resetCounts();
    std::map<String, UInt64> intake;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    const RoundReport report = runRegularRoundReclaiming(gc);
    gc.setPhaseSink({});

    const String expected_next =
        layout.refLogKey(fixture::fixtureLife(quiet), RefTxnId{sealed.writer_epoch, sealed.ref_sequence + 1});
    EXPECT_EQ(backend->getCount(expected_next), 0u)
        << "the inclusive checkpoint boundary proves this quiet life without a successor probe";
    EXPECT_TRUE(report.anomalies.empty());
    ASSERT_FALSE(intake.empty());
    EXPECT_EQ(intake["frontier_proven"], intake["frontier_namespaces"])
        << "the inherited cursor already at the checkpoint boundary is destructive-eligible";
}

/// A checkpoint must never retreat below a sealed cursor. Its inclusive frontier can prove a cursor
/// already at that point, but cannot explain one that has advanced beyond it.
TEST(CASGCFrontierGate, CheckpointFrontierBehindAnInheritedCursorFailsClosed)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/checkpoint-behind-inherited-cursor@cas@"};

    fixture::admitLive(*backend, layout, ns);
    publish(*backend, layout, ns, "first", 1, DB::UInt128(0xfb));
    publish(*backend, layout, ns, "second", 2, DB::UInt128(0xfc));
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 2}));

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    const String checkpoint_key = layout.refCkptKey(life);
    const HeadResult checkpoint_head = backend->head(checkpoint_key);
    ASSERT_TRUE(checkpoint_head.exists);
    ASSERT_EQ(backend->putOverwrite(checkpoint_key, encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    }), checkpoint_head.token).outcome, PutOutcome::Done);

    std::map<String, UInt64> intake;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    const RoundReport report = runRegularRoundReclaiming(gc);
    gc.setPhaseSink({});

    EXPECT_FALSE(report.anomalies.empty());
    ASSERT_FALSE(intake.empty());
    EXPECT_LT(intake["frontier_proven"], intake["frontier_namespaces"]);
}

/// A carried `EpochSeal` may have its authoritative successor in the next epoch. The arithmetic
/// successor in the sealed epoch is absent by design, so the exact checkpoint frontier must nominate
/// the shared seal-chain crossing before that absence is classified as a same-epoch gap.
TEST(CASGCFrontierGate, CheckpointFrontierCrossesAnInheritedEpochSeal)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/checkpoint-inherited-seal-crossing@cas@"};
    const DB::UInt128 crossed_blob(0xfd);

    fixture::admitLive(*backend, layout, ns);
    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "birth", 1, DB::UInt128(0xfe), /*birth=*/true);
    writeSealAt(*backend, layout, ns, RefTxnId{1, 2});
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 2},
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 2}));

    publishAt(*backend, layout, ns, RefTxnId{2, 1}, "crossed", 2, crossed_blob,
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 2});
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    const String checkpoint_key = layout.refCkptKey(life);
    const HeadResult checkpoint_head = backend->head(checkpoint_key);
    ASSERT_TRUE(checkpoint_head.exists);
    ASSERT_EQ(backend->putOverwrite(checkpoint_key, encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 2},
    }), checkpoint_head.token).outcome, PutOutcome::Done);

    std::map<String, UInt64> intake;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    const RoundReport report = runRegularRoundReclaiming(gc);
    gc.setPhaseSink({});

    EXPECT_TRUE(report.anomalies.empty());
    EXPECT_GT(inDegreeOf(*backend, layout, crossed_blob), 0);
    ASSERT_FALSE(intake.empty());
    EXPECT_EQ(intake["frontier_proven"], intake["frontier_namespaces"]);
}

/// The exact checkpoint successor must chain to the seal just consumed. Merely being in the next epoch
/// is insufficient: an incorrect predecessor would skip an unclosed history segment forever.
TEST(CASGCFrontierGate, CheckpointFrontierRejectsWrongPredecessorAfterFreshEpochSeal)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/checkpoint-wrong-fresh-seal-predecessor@cas@"};

    fixture::admitLive(*backend, layout, ns);
    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "birth", 1, DB::UInt128(0xff), /*birth=*/true);
    writeSealAt(*backend, layout, ns, RefTxnId{1, 2});
    publishAt(*backend, layout, ns, RefTxnId{2, 1}, "wrong_predecessor", 2, DB::UInt128(0x100),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 1});
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 2},
    });

    std::map<String, UInt64> intake;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    const RoundReport report = runRegularRoundReclaiming(gc);
    gc.setPhaseSink({});

    EXPECT_FALSE(report.anomalies.empty());
    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 2}));
    ASSERT_FALSE(intake.empty());
    EXPECT_LT(intake["frontier_proven"], intake["frontier_namespaces"]);
}

/// A namespace that was WRONGLY quiet -- the hint hid a record that is durably there -- is walked this
/// round, not next: the probe finds the record and the walk continues from it.
TEST(CASGCFrontierGate, AWronglyQuietNamespaceIsWalkedTheSameRound)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace quiet{"00/quiet@cas@"};
    const DB::UInt128 late_blob(0x77);

    publish(*backend, layout, quiet, "ref_1", 1, DB::UInt128(0x11));
    replaceRecoverableCkptForRawFixture(*backend, layout, quiet, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    const RefTxnId sealed_before = sealedCursorOf(*backend, layout, quiet);

    /// A second publish lands, and the store hides the namespace from every LIST at the same moment.
    publish(*backend, layout, quiet, "ref_2", 2, late_blob);
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, quiet);
    const String checkpoint_key = layout.refCkptKey(life);
    const HeadResult checkpoint_head = backend->head(checkpoint_key);
    ASSERT_TRUE(checkpoint_head.exists);
    ASSERT_EQ(backend->putOverwrite(checkpoint_key, encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    }), checkpoint_head.token).outcome, PutOutcome::Done);
    backend->hidePrefix(layout.namespaceStreamPrefix(fixture::fixtureLife(quiet)));

    runRegularRoundReclaiming(gc);

    EXPECT_LT(sealed_before, sealedCursorOf(*backend, layout, quiet))
        << "the probe found the hidden record, so the walk folded it and the cursor advanced";
    EXPECT_GT(inDegreeOf(*backend, layout, late_blob), 0)
        << "the hidden publish's edge folded this round -- the hint never mentioned it";
}

/// The catalog life is grounded by its exact decoded `_ckpt`, not by the round's listing or a later
/// absent probe. A durable `F+1` is physically present but not committed history, so this fold may apply
/// only `F`; in particular it must not read `F+2`. Reaching `F` still proves the checkpoint-bounded
/// cut, so the physical successor cannot suppress otherwise eligible destructive work.
TEST(CASGCFrontierGate, CheckpointFrontierBoundsOrdinaryFoldBeforeDurableSuccessor)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/checkpoint-bounds-fold@cas@"};
    const DB::UInt128 committed_blob(0xf1);
    const DB::UInt128 beyond_frontier_blob(0xf2);

    fixture::admitLive(*backend, layout, ns);
    publish(*backend, layout, ns, "committed", 1, committed_blob);
    const ManifestRef uncommitted{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 1};
    writeBlobBody(*backend, layout, beyond_frontier_blob);
    writeManifestRaw(*backend, layout, ns, uncommitted, {blobEntryFor("data.bin", beyond_frontier_blob)});
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(),
        .txn_id = RefTxnId{1, 2},
        .ops = publishCommittedOps("durable_but_uncommitted", uncommitted),
        .prev_epoch_seal = std::nullopt,
    });
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    std::map<String, UInt64> intake;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    backend->resetCounts();
    const RoundReport report = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(report.acquired_lease);
    gc.setPhaseSink({});

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 1}));
    EXPECT_EQ(inDegreeOf(*backend, layout, beyond_frontier_blob), 0)
        << "a durable log above `_ckpt.committed_through` is not foldable history";
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, RefTxnId{1, 2})), 0u)
        << "the checkpoint frontier stops the walk before `F+1`";
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, RefTxnId{1, 3})), 0u)
        << "a 404 above `F+1` must not authorize the destructive frontier";
    EXPECT_TRUE(report.anomalies.empty());
    ASSERT_FALSE(intake.empty());
    EXPECT_EQ(intake["frontier_proven"], intake["frontier_namespaces"])
        << "the consumed checkpoint frontier, not the physical successor, authorizes this cut";
}

/// With no physical successor at all, consuming the exact inclusive checkpoint frontier proves this
/// catalog life complete. This is the control for the same bounded-cut proof exercised with a durable
/// uncommitted successor above.
TEST(CASGCFrontierGate, ConsumedCheckpointFrontierProvesOrdinaryLifeWithoutSuccessor)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/checkpoint-complete-fold@cas@"};

    fixture::admitLive(*backend, layout, ns);
    publish(*backend, layout, ns, "committed", 1, DB::UInt128(0xf3));
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    std::map<String, UInt64> intake;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    backend->resetCounts();
    const RoundReport report = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(report.acquired_lease);
    gc.setPhaseSink({});

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 1}));
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, RefTxnId{1, 2})), 0u)
        << "the checkpoint boundary proves the cut without a post-frontier 404";
    EXPECT_TRUE(report.anomalies.empty());
    ASSERT_FALSE(intake.empty());
    EXPECT_EQ(intake["frontier_proven"], intake["frontier_namespaces"]);
}

/// The same cut remains complete when the durable uncommitted successor is hidden from every LIST.
/// Exact reads still serve that successor, but the checkpoint ceiling must leave it untouched and must
/// not let the list omission suppress the checkpoint-bounded destructive path.
TEST(CASGCFrontierGate, CheckpointFrontierProvesLifeWithHiddenDurableSuccessor)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/checkpoint-hidden-successor@cas@"};
    const DB::UInt128 beyond_frontier_blob(0xf4);

    fixture::admitLive(*backend, layout, ns);
    publish(*backend, layout, ns, "committed", 1, DB::UInt128(0xf5));
    const ManifestRef uncommitted{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 1};
    writeBlobBody(*backend, layout, beyond_frontier_blob);
    writeManifestRaw(*backend, layout, ns, uncommitted, {blobEntryFor("data.bin", beyond_frontier_blob)});
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(),
        .txn_id = RefTxnId{1, 2},
        .ops = publishCommittedOps("hidden_durable_but_uncommitted", uncommitted),
        .prev_epoch_seal = std::nullopt,
    });
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    backend->hide(layout.refLogKey(life, RefTxnId{1, 2}));

    std::map<String, UInt64> intake;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    backend->resetCounts();
    const RoundReport report = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(report.acquired_lease);
    gc.setPhaseSink({});

    EXPECT_GT(backend->holesServed(), 0u) << "the F+1 log must really be hidden from LIST";
    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 1}));
    EXPECT_EQ(inDegreeOf(*backend, layout, beyond_frontier_blob), 0);
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, RefTxnId{1, 2})), 0u)
        << "the hidden durable successor is outside the checkpoint cut";
    EXPECT_TRUE(report.anomalies.empty());
    ASSERT_FALSE(intake.empty());
    EXPECT_EQ(intake["frontier_proven"], intake["frontier_namespaces"]);
}

/// The checkpoint's inclusive endpoint is itself a durable witness. If that exact log is absent,
/// the namespace is corrupt rather than complete; a 404 at the endpoint must not authorize cleanup.
TEST(CASGCFrontierGate, MissingCommittedCheckpointLogHoldsInsteadOfProvingTheFrontier)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/missing-committed-checkpoint-log@cas@"};

    fixture::admitLive(*backend, layout, ns);
    publish(*backend, layout, ns, "first", 1, DB::UInt128(0xf6));
    publish(*backend, layout, ns, "missing_but_committed", 2, DB::UInt128(0xf7));
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    const String missing_key = layout.refLogKey(life, RefTxnId{1, 2});
    const HeadResult missing_head = backend->head(missing_key);
    ASSERT_TRUE(missing_head.exists);
    ASSERT_EQ(backend->deleteExact(missing_key, missing_head.token).kind, DeleteOutcome::Kind::Deleted);

    std::map<String, UInt64> intake;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    const RoundReport report = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(report.acquired_lease);
    gc.setPhaseSink({});

    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 1}));
    EXPECT_FALSE(report.anomalies.empty()) << "the missing committed checkpoint record is corruption";
    ASSERT_FALSE(intake.empty());
    EXPECT_LT(intake["frontier_proven"], intake["frontier_namespaces"]);
}

/// A checkpoint may name a durable record that the round's LIST omitted. Exact GETs must still fold
/// that committed record; the frozen list tail is only a scheduling hint, never a history boundary.
TEST(CASGCFrontierGate, HiddenCommittedCheckpointLogIsFoldedThroughTheAuthorityCeiling)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/hidden-committed-checkpoint-log@cas@"};
    const DB::UInt128 hidden_blob(0xf8);

    fixture::admitLive(*backend, layout, ns);
    publish(*backend, layout, ns, "first", 1, DB::UInt128(0xf9));
    publish(*backend, layout, ns, "hidden_but_committed", 2, hidden_blob);
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    backend->hide(layout.refLogKey(life, RefTxnId{1, 2}));

    std::map<String, UInt64> intake;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    const RoundReport report = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(report.acquired_lease);
    gc.setPhaseSink({});

    EXPECT_GT(backend->holesServed(), 0u) << "the committed endpoint must really be omitted from LIST";
    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 2}));
    EXPECT_GT(inDegreeOf(*backend, layout, hidden_blob), 0);
    EXPECT_TRUE(report.anomalies.empty());
    ASSERT_FALSE(intake.empty());
    EXPECT_EQ(intake["frontier_proven"], intake["frontier_namespaces"]);
}

/// A valid checkpoint with no committed record is an authoritative empty history. It is complete for
/// a never-folded life without probing a fabricated first transaction.
TEST(CASGCFrontierGate, EmptyCheckpointFrontierProvesAnUnfoldedLife)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/empty-checkpoint-frontier@cas@"};

    casAdmitRecoverableEntry(*backend, layout, ns);

    std::map<String, UInt64> intake;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    const RoundReport report = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(report.acquired_lease);
    gc.setPhaseSink({});

    EXPECT_TRUE(report.anomalies.empty());
    ASSERT_FALSE(intake.empty());
    EXPECT_EQ(intake["frontier_proven"], intake["frontier_namespaces"]);
}

/// Empty history cannot explain an inherited cursor. An operator-corrupted checkpoint that erases its
/// own committed boundary must clamp the life rather than silently authorize destruction.
TEST(CASGCFrontierGate, EmptyCheckpointFrontierRejectsAnInheritedCursor)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/empty-checkpoint-after-cursor@cas@"};

    fixture::admitLive(*backend, layout, ns);
    publish(*backend, layout, ns, "first", 1, DB::UInt128(0xfa));
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 1}));

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    const String checkpoint_key = layout.refCkptKey(life);
    const HeadResult checkpoint_head = backend->head(checkpoint_key);
    ASSERT_TRUE(checkpoint_head.exists);
    ASSERT_EQ(backend->putOverwrite(checkpoint_key, encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    }), checkpoint_head.token).outcome, PutOutcome::Done);

    std::map<String, UInt64> intake;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    const RoundReport report = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(report.acquired_lease);
    gc.setPhaseSink({});

    EXPECT_FALSE(report.anomalies.empty()) << "an empty checkpoint cannot explain a nonzero cursor";
    ASSERT_FALSE(intake.empty());
    EXPECT_LT(intake["frontier_proven"], intake["frontier_namespaces"]);
}

/// A catalog `Live` life without its exact checkpoint cannot derive either its genesis or a frontier
/// from the ref LIST. Even a durable listed first log must be retained until the authority is repaired.
TEST(CASGCFrontierGate, CatalogLifeWithoutCheckpointDefersWithoutUsingListedFrontier)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/missing-checkpoint-fold@cas@"};
    const DB::UInt128 blob(0xc7);

    fixture::admitLive(*backend, layout, ns);
    const ManifestRef manifest{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1};
    writeBlobBody(*backend, layout, blob);
    writeManifestRaw(*backend, layout, ns, manifest, {blobEntryFor("data.bin", blob)});
    appendRefLogSeed(*backend, layout, ns, publishCommittedOps("must_remain_unfolded", manifest));

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    ASSERT_FALSE(readCkpt(*backend, layout, life).has_value());

    std::map<String, UInt64> intake;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    backend->resetCounts();
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    gc.setPhaseSink({});

    EXPECT_EQ(foldCursorOf(*backend, layout, ns, /*shard=*/0), 0u);
    EXPECT_EQ(inDegreeOf(*backend, layout, blob), 0)
        << "a missing checkpoint must defer rather than fold the listed log";
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, RefTxnId{1, 1})), 0u)
        << "the listed log is not authority for a checkpoint-less catalog life";
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, RefTxnId{1, 2})), 0u)
        << "the next 404 is not authority for a checkpoint-less catalog life";
    EXPECT_EQ(backend->deleteTotal(), 0u) << deletedKeysMessage(*backend);
    ASSERT_FALSE(intake.empty());
    EXPECT_LT(intake["frontier_proven"], intake["frontier_namespaces"]);
}

/// A valid checkpoint frontier proves a quiet unhinted life without spending the successor-probe budget.
/// A zero budget therefore cannot suppress unrelated destructive work merely because this life is absent
/// from LIST.
TEST(CASGCFrontierGate, AnExhaustedProbeBudgetSealsCursorsAndDeletesNothing)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    auto store = openPoolWithProbeBudget(backend, /*budget*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace quiet{"00/quiet@cas@"};
    const RootNamespace busy{"00/busy@cas@"};
    const DB::UInt128 blob(0xbeef);

    publish(*backend, layout, quiet, "quiet_ref", 1, DB::UInt128(0x11));
    const ManifestRef mref = publish(*backend, layout, busy, "busy_ref", 2, blob);

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    const RefTxnId quiet_cursor = sealedCursorOf(*backend, layout, quiet);
    ASSERT_NE(quiet_cursor, (RefTxnId{}));

    /// The quiet namespace goes unhinted and the budget is zero. Its CTE still proves the carried
    /// cursor, while the busy namespace drops its ref and may proceed through reclamation.
    backend->hidePrefix(layout.namespaceStreamPrefix(fixture::fixtureLife(quiet)));
    dropRefTransition(*backend, layout, busy, "busy_ref", mref);

    backend->resetCounts();
    drive(store, gc, /*rounds*/ 5, UniversePolicy::Authoritative);

    EXPECT_GT(backend->deleteTotal(), 0u)
        << "the quiet life's checkpoint authority leaves unrelated deletion eligible";
    EXPECT_FALSE(backend->head(blobKeyOf(layout, blob)).exists)
        << "the busy life's removal remains reclaimable despite the quiet LIST omission";
    EXPECT_EQ(sealedCursorOf(*backend, layout, quiet), quiet_cursor)
        << "the unprobed namespace's cursor rides verbatim -- it is never dropped";
    const NamespaceLifeId quiet_life = *CasRefCatalog::lifeIfCataloged(*backend, layout, quiet);
    const auto quiet_checkpoint = readCkpt(*backend, layout, quiet_life);
    ASSERT_TRUE(quiet_checkpoint.has_value());
    EXPECT_EQ(quiet_checkpoint->ckpt.committed_through, quiet_cursor)
        << "the quiet life's valid CTE is unaffected by LIST omission and a zero probe budget";
    EXPECT_GT(decodeGcState(backend->get(layout.gcStateKey())->bytes).round, 1u)
        << "the round still commits; only its destructive half is withheld";
}

/// ===================== A COMMITTED GAP IS REDETECTED UNTIL REPAIRED =====================
///
/// A hold's committed checkpoint frontier remains a durable witness of its own gap. Hiding the later
/// log from LIST cannot make that gap quiet: every retry exact-reads the missing position, redetects the
/// hold, and suppresses destructive work until an operator repairs the record stream.
TEST(CASGCFrontierGate, ACommittedGapIsRedetectedAndSuppressesEveryRound)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace held{"00/held@cas@"};
    const RootNamespace busy{"00/busy@cas@"};
    const DB::UInt128 blob(0xbeef);

    /// {1,3} never existed while {1,4} is durable and listed.
    publish(*backend, layout, held, "ref_1", 1, DB::UInt128(0x21));
    publish(*backend, layout, held, "ref_2", 2, DB::UInt128(0x22));
    const ManifestRef orphan_ref{.writer_epoch = 1, .build_sequence = 4, .manifest_ordinal = 1};
    writeBlobBody(*backend, layout, DB::UInt128(0x24));
    writeManifestRaw(*backend, layout, held, orphan_ref, {blobEntryFor("data.bin", DB::UInt128(0x24))});
    RefLogTxn txn;
    txn.ns = held.string();
    txn.txn_id = RefTxnId{1, 4};
    txn.ops = publishCommittedOps("ref_4", orphan_ref);
    fixture::writeRefLogRaw(*backend, layout, txn);
    replaceRecoverableCkptForRawFixture(*backend, layout, held, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 4},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    std::map<String, UInt64> first_intake;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            first_intake = rec.metrics;
    });
    const RoundReport first_round = runRegularRoundReclaiming(gc);
    gc.setPhaseSink({});
    store->renewWatermarkOnce();
    ASSERT_EQ(sealedCursorOf(*backend, layout, held), (RefTxnId{1, 2}))
        << "round 1 must stop below the gap and hold there";
    ASSERT_FALSE(first_intake.empty());
    EXPECT_GT(first_intake["tables_clamped"], 0u);
    EXPECT_GT(first_intake["tables_held"], 0u);
    EXPECT_FALSE(first_round.anomalies.empty());

    const NamespaceLifeId held_life = *CasRefCatalog::lifeIfCataloged(*backend, layout, held);
    const auto held_checkpoint = readCkpt(*backend, layout, held_life);
    ASSERT_TRUE(held_checkpoint.has_value());
    EXPECT_EQ(held_checkpoint->ckpt.committed_through, (RefTxnId{1, 4}));

    /// Hiding `{1,4}` from LIST does not hide the committed CTE frontier. The next round exact-reads
    /// the missing `{1,3}`, re-detects the gap, and seals a fresh hold.
    backend->hidePrefix(layout.refLogKey(fixture::fixtureLife(held), RefTxnId{1, 4}));

    /// Meanwhile a blob elsewhere becomes condemnable, so the round has real destructive work to decline.
    const ManifestRef mref = publish(*backend, layout, busy, "busy_ref", 9, blob);
    dropRefTransition(*backend, layout, busy, "busy_ref", mref);

    backend->resetCounts();
    std::map<String, UInt64> second_intake;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            second_intake = rec.metrics;
    });
    const RoundReport second_round = runRegularRoundReclaiming(gc);
    gc.setPhaseSink({});
    store->renewWatermarkOnce();

    ASSERT_FALSE(second_intake.empty());
    EXPECT_GT(second_intake["tables_clamped"], 0u)
        << "the committed `{1,4}` frontier is a durable witness that re-detects the missing `{1,3}`";
    EXPECT_GT(second_intake["tables_held"], 0u)
        << "the fresh clamp preserves the unresolved hold in the next sealed coverage";
    EXPECT_FALSE(second_round.anomalies.empty());

    drive(store, gc, /*rounds*/ 4, UniversePolicy::Authoritative);

    EXPECT_EQ(backend->deleteTotal(), 0u)
        << "the re-detected committed gap suppresses each round's destructive work. "
           "Deleted:" << deletedKeysMessage(*backend);
    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists);
    EXPECT_EQ(sealedCursorOf(*backend, layout, held), (RefTxnId{1, 2}))
        << "the committed gap remains unresolved and the cursor cannot advance through it";
    const auto final_checkpoint = readCkpt(*backend, layout, held_life);
    ASSERT_TRUE(final_checkpoint.has_value());
    EXPECT_EQ(final_checkpoint->ckpt.committed_through, (RefTxnId{1, 4}));
}

/// ===================== THE TEMPORAL LEMMA, ALL THREE ARMS =====================
///
/// The gate says WHEN a round may destroy. These say that even a round which may destroy cannot
/// destroy a blob some edge still owns, over the three interleavings that matter.

/// ARM (a): a `+1` that lands after this round's probes and is followed by the SAME round's
/// condemnation. Round pacing makes it safe on its own: an entry condemned at round K cannot graduate
/// before K+1 and cannot be deleted before K+2, so the round that condemns never deletes.
TEST(CASGCFrontierGate, ABlobCondemnedThisRoundIsNeverDeletedThisRound)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    const DB::UInt128 blob(0xc04d);

    const ManifestRef mref = publish(*backend, layout, ns, "ref_1", 1, blob);
    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();

    dropRefTransition(*backend, layout, ns, "ref_1", mref);
    backend->resetCounts();
    runRegularRoundReclaiming(gc);

    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists)
        << "the condemning round must not also delete";
    EXPECT_EQ(backend->deleteCount(blobKeyOf(layout, blob)), 0u)
        << "not merely still present -- the delete was never attempted";
}

/// ARM (c) of the temporal lemma is the delete-site in-degree re-read, and it is NORMATIVE (spec §5,
/// third arm): an edge folded AFTER the condemnation but BEFORE the delete pass spares the blob
/// outright, `indeg > 0` winning over `delete_pending` past the floor. The other two arms bound WHEN
/// and WHAT a delete may remove; only this one asks whether the blob is still referenced at the moment
/// the pass decides.
TEST(CASGCFrontierGate, ALateEdgeSparesADeletePendingBlobAtTheDeleteSite)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    const DB::UInt128 blob(0x1a7e);

    const ManifestRef mref = publish(*backend, layout, ns, "ref_1", 1, blob);
    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();

    /// Condemn it, then graduate it to delete_pending.
    dropRefTransition(*backend, layout, ns, "ref_1", mref);
    runRegularRoundReclaiming(gc);            /// condemn
    store->renewWatermarkOnce();
    runRegularRoundReclaiming(gc);            /// graduate: delete_pending published
    store->renewWatermarkOnce();

    /// A new owner appears BEFORE the delete pass. The pass recomputes the in-degree from the merge it
    /// just ran and finds it nonzero.
    const ManifestRef revived{.writer_epoch = 1, .build_sequence = 42, .manifest_ordinal = 1};
    writeManifestRaw(*backend, layout, ns, revived, {blobEntryFor("data.bin", blob)});
    publishCommittedTransition(*backend, layout, ns, "revived_ref", std::nullopt, revived);

    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();

    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists)
        << "the delete-site in-degree re-read spares a blob a fresh edge re-referenced";
    EXPECT_GT(inDegreeOf(*backend, layout, blob), 0);
}

/// ARM (b): a TOKENED adoption of an already-delete-pending blob. The writer's admit gate reads the
/// `Condemned` meta, refuses to adopt the dying incarnation, and rematerializes from its own source as
/// a FRESH incarnation -- so the delayed exact-token delete the previous round published finds a
/// different token and removes nothing. The blob's identity is preserved by re-upload, never by
/// reviving the condemned object.
TEST(CASGCFrontierGate, AResurrectedIncarnationSurvivesTheDelayedStaleTokenDelete)
{
    ensureBlobUploadPoolForTest();

    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    /// A REAL content-addressed blob, so the writer path below addresses exactly the object GC condemns.
    const String payload = "frontier-gate-republish-payload";
    const DB::UInt128 hash = u128Of(payload);
    const BlobRef id = idOf(payload);
    const String key = layout.blobKey(id);
    String raw_body(store->poolMeta().blob_header_len, '\0');
    raw_body += payload;
    writeRawBlobBody(*backend, layout, hash, raw_body);

    /// Publish and drop it so GC condemns and then graduates it to delete_pending.
    const ManifestRef mref{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1};
    writeManifestRaw(*backend, layout, ns, mref, {blobEntryFor("data.bin", hash)});
    publishCommittedTransition(*backend, layout, ns, "ref_1", std::nullopt, mref);

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    dropRefTransition(*backend, layout, ns, "ref_1", mref);
    runRegularRoundReclaiming(gc);            /// condemn: writes the durable Condemned meta
    store->renewWatermarkOnce();
    runRegularRoundReclaiming(gc);            /// graduate: publishes delete_pending against THIS token
    store->renewWatermarkOnce();

    const Token condemned_token = backend->head(key).token;
    const auto condemned_meta = loadMetaForTest(*backend, layout, hash);
    ASSERT_TRUE(condemned_meta.has_value());
    ASSERT_EQ(condemned_meta->meta.state, MetaState::Condemned)
        << "the delete GC is about to execute must be backed by durable Condemned evidence";

    /// A writer now adopts the blob through the REAL admit gate. It point-reads the Condemned meta,
    /// refuses to adopt the dying incarnation, and rematerializes from its OWN source bytes -- never by
    /// reading the condemned object. The key ends up holding a DIFFERENT incarnation.
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/republished";
    auto build = store->beginPartWrite(info);
    const ManifestId republished_manifest
        = build->stageManifest({blobEntryFor("data.bin", hash, payload.size())});
    build->precommitAdd(ns, "republished", republished_manifest);
    const PutBlobResult uploaded = build->putBlob(id, BlobSource::fromString(payload));
    EXPECT_EQ(uploaded.ref, id);
    build->promote(ns, "republished", build->buildId(), republished_manifest);
    const Token fresh_token = backend->head(key).token;
    ASSERT_NE(fresh_token, condemned_token) << "republication must displace the condemned incarnation";

    /// GC's delayed delete still names the OLD token. It cannot touch the new object.
    drive(store, gc, /*rounds*/ 2, UniversePolicy::Authoritative);

    ASSERT_TRUE(backend->head(key).exists)
        << "the resurrected incarnation survives the delete published against its predecessor";
    EXPECT_EQ(backend->head(key).token, fresh_token) << "and it is still the writer's incarnation";
    EXPECT_EQ(backend->deleteExact(key, condemned_token).kind, DeleteOutcome::Kind::TokenMismatch)
        << "the condemned token can never remove the fresh object (INV-NO-RETURN)";
}

/// ARM (c): a TOKENLESS relink -- the receiver adopts by evidence, holding no token at all. Safety
/// then rests entirely on ORDER, so the operation journal has to show it: the receiver's `+1` is
/// durable BEFORE the source releases its own committed edge, and no point in the schedule leaves the
/// blob with zero durable owners.
TEST(CASGCFrontierGate, ATokenlessRelinkMakesTheReceiverEdgeDurableBeforeTheSourceReleases)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace source{"00/source@cas@"};
    const RootNamespace receiver{"00/receiver@cas@"};
    const DB::UInt128 blob(0x8e11);

    const ManifestRef source_ref = publish(*backend, layout, source, "part_1", 1, blob);
    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    ASSERT_GT(inDegreeOf(*backend, layout, blob), 0);

    /// The relink, in the only order the protocol permits: the receiver's manifest body and its
    /// committed edge first (tokenless -- it never HEADs the blob), and only afterwards the source's
    /// removal. Between the two writes the blob has TWO durable owners; it never has zero.
    const ManifestRef receiver_ref{.writer_epoch = 1, .build_sequence = 5, .manifest_ordinal = 1};
    writeManifestRaw(*backend, layout, receiver, receiver_ref, {blobEntryFor("data.bin", blob)});
    publishCommittedTransition(*backend, layout, receiver, "part_1", std::nullopt, receiver_ref);

    /// The round that observes ONLY the receiver's `+1` -- the exact midpoint of the schedule.
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    EXPECT_GE(inDegreeOf(*backend, layout, blob), 2)
        << "at the midpoint both owners are durable; the handoff never dips to zero";

    dropRefTransition(*backend, layout, source, "part_1", source_ref);
    drive(store, gc, /*rounds*/ 4, UniversePolicy::Authoritative);

    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists)
        << "the source released its edge only after the receiver's was durable, so nothing may collect it";
    EXPECT_EQ(inDegreeOf(*backend, layout, blob), 1)
        << "the receiver is the sole remaining owner";
}

/// ===================== CLEANUP RANGES ARE COMPUTED, NOT ENUMERATED =====================
///
/// `planRefCleanup` is pure, so the boundary arithmetic is pinned directly rather than inferred from a
/// round's side effects. Its sole coverage authority is the checkpoint-named base; a listed snapshot
/// is merely a physical observation until the same-id triple has been validated.

TEST(CASGCFrontierGateCleanupRange, CoveredLogsStopAtTheMinimumOfCheckpointAndCursor)
{
    RefTableListing listing;
    listing.logs = {{1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}};
    listing.snapshots = {{1, 5}};

    /// No checkpoint means no recovery base at all. A snapshot PUT that has not reached the `_ckpt`
    /// CAS must retain every listed object.
    const RefCleanupPlan without = planRefCleanup(listing, RefTxnId{1, 4});
    EXPECT_TRUE(without.deletable_logs.empty());
    EXPECT_TRUE(without.deletable_snapshots.empty());

    /// A checkpoint BELOW the cursor tightens it to {1,2}. Its exact `_log` witness must survive,
    /// so cleanup may remove only the strictly older entry.
    const RefCleanupPlan with = planRefCleanup(listing, RefTxnId{1, 4}, RefTxnId{1, 2});
    EXPECT_EQ(with.deletable_logs, (std::vector<RefTxnId>{{1, 1}}))
        << "the checkpoint witness and everything above it must survive";

    /// Once validation has established a later checkpoint base, its earlier covered history is
    /// reclaimable even if the hot fold cursor has not yet reached that base.
    const RefCleanupPlan ahead = planRefCleanup(listing, RefTxnId{1, 4}, RefTxnId{1, 9});
    EXPECT_EQ(ahead.deletable_logs, (std::vector<RefTxnId>{{1, 1}, {1, 2}, {1, 3}, {1, 4}}));
    EXPECT_EQ(ahead.deletable_snapshots, (std::vector<RefTxnId>{{1, 5}}));
}

TEST(CASGCFrontierGateCleanupRange, ASnapshotAtTheCheckpointSurvivesAndOnlyStrictlyOlderOnesGo)
{
    RefTableListing listing;
    listing.logs = {{1, 1}, {1, 2}, {1, 3}};
    listing.snapshots = {{1, 1}, {1, 2}, {1, 3}};

    /// A LIST-only newest snapshot is never a cleanup boundary.
    const RefCleanupPlan without = planRefCleanup(listing, RefTxnId{1, 3});
    EXPECT_TRUE(without.deletable_snapshots.empty());

    /// With the checkpoint AT {1,2}, only {1,1} is strictly below it. The snapshot the checkpoint names
    /// is the one a recovering reader samples, so it must survive its own cleanup.
    const RefCleanupPlan with = planRefCleanup(listing, RefTxnId{1, 3}, RefTxnId{1, 2});
    EXPECT_EQ(with.deletable_snapshots, (std::vector<RefTxnId>{{1, 1}}));

    /// The oldest checkpoint deletes nothing at all.
    const RefCleanupPlan oldest = planRefCleanup(listing, RefTxnId{1, 3}, RefTxnId{1, 1});
    EXPECT_TRUE(oldest.deletable_snapshots.empty());
}

/// Cleanup shares recovery's validator rather than inferring its own authority from a LIST. The
/// missing-base case is the no-checkpoint range above; the three physical triple failures below must
/// each reject exactly the checkpoint-named candidate.
TEST(CASGCFrontierGateCleanupRange, CheckpointBaseValidatorRejectsMissingLogSnapshotAndSeal)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout{"p"};
    const RefTxnId base{1, 1};
    const RefCkpt checkpoint{
        .life_epoch = 1,
        .committed_through = base,
        .checkpoint_snapshot_id = base,
        .last_epoch_seal = std::nullopt};
    CasRefCatalog::initializeEmptyForNewPool(*backend, layout);

    {
        const RootNamespace ns{"00/cleanup-missing-base-log@cas@"};
        fixture::admitLive(*backend, layout, ns);
        const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, layout, ns).value();
        writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), base));
        EXPECT_THROW((void)readCheckpointSnapshotBase(*backend, layout, life, checkpoint), DB::Exception);
    }
    {
        const RootNamespace ns{"00/cleanup-missing-base-snapshot@cas@"};
        fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
            .ns = ns.string(), .txn_id = base, .ops = {namespaceBirthOp()}, .prev_epoch_seal = std::nullopt});
        const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, layout, ns).value();
        EXPECT_THROW((void)readCheckpointSnapshotBase(*backend, layout, life, checkpoint), DB::Exception);
    }
    {
        const RootNamespace ns{"00/cleanup-seal-is-not-base@cas@"};
        writeSealAt(*backend, layout, ns, base);
        const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, layout, ns).value();
        writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), base));
        EXPECT_THROW((void)readCheckpointSnapshotBase(*backend, layout, life, checkpoint), DB::Exception);
    }
}

TEST(CASGCFrontierGateCleanupRange, LaterEpochBaseWithoutItsContextualBacklinkCannotLicenseDeletion)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout{"p"};
    CasRefCatalog::initializeEmptyForNewPool(*backend, layout);
    const RefTxnId seal_id{1, 2};
    const RefTxnId base_id{2, 1};

    const auto expect_no_deletion_authority = [&](const RootNamespace & ns, std::optional<RefTxnId> backlink)
    {
        fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
            .ns = ns.string(), .txn_id = RefTxnId{1, 1}, .ops = {namespaceBirthOp()},
            .prev_epoch_seal = std::nullopt});
        writeSealAt(*backend, layout, ns, seal_id);
        fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
            .ns = ns.string(), .txn_id = base_id, .ops = {}, .prev_epoch_seal = backlink});
        writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), base_id));
        const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, layout, ns).value();

        std::optional<RefTxnId> validated_base;
        try
        {
            (void)readCheckpointSnapshotBase(*backend, layout, life, RefCkpt{
                .life_epoch = 1,
                .committed_through = base_id,
                .checkpoint_snapshot_id = base_id,
                .last_epoch_seal = seal_id});
            validated_base = base_id;
        }
        catch (const DB::Exception &) // NOLINT(bugprone-empty-catch): failure to validate is the tested case -- `validated_base` deliberately stays nullopt
        {
        }

        RefTableListing listing;
        listing.logs = {{1, 1}, seal_id, base_id};
        listing.snapshots = {{1, 1}, base_id};
        const RefCleanupPlan plan = planRefCleanup(listing, base_id, validated_base);
        EXPECT_TRUE(plan.deletable_logs.empty());
        EXPECT_TRUE(plan.deletable_snapshots.empty());
    };

    expect_no_deletion_authority(RootNamespace{"00/cleanup-base-missing-backlink@cas@"}, std::nullopt);
    expect_no_deletion_authority(RootNamespace{"00/cleanup-base-wrong-backlink@cas@"}, RefTxnId{1, 99});
}

/// Folding a namespace terminal records evidence but performs no lifecycle-specific physical cleanup.
/// The checkpoint is inert debris for the perpetual janitor, and no `_cleanup` marker is published.
TEST(CASGCFrontierGate, CleanupEvidenceLeavesRemovedNamespaceCheckpointForJanitor)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace removed{"00/removed@cas@"};
    const RefOp birth_op = namespaceBirthOp();
    RefOp remove_op;
    remove_op.kind = RefOpKind::RemoveNamespace;
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = removed.string(), .txn_id = RefTxnId{1, 1}, .ops = {birth_op}, .prev_epoch_seal = std::nullopt});
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = removed.string(), .txn_id = RefTxnId{1, 2}, .ops = {remove_op}, .prev_epoch_seal = std::nullopt});
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, layout, removed).value();
    CasRefCatalog::casUpdate(*backend, layout, [&](const RefCatalog & current)
    {
        RefCatalog next = current;
        const auto it = std::find_if(next.entries.begin(), next.entries.end(), [&](const CatalogEntry & entry)
        {
            return entry.ns == removed;
        });
        EXPECT_NE(it, next.entries.end());
        it->state = NsState::Removing;
        it->removal_started_round = 1;
        return next;
    });
    const String ckpt_key = layout.refCkptKey(life);
    backend->putIfAbsent(ckpt_key, encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    }));

    /// The removal evidence must arise from a replay-valid terminal lifecycle, rather than merely
    /// from a raw terminal record that the recovery state machine refuses.
    const RecoveredRefTable recovered = recoverRefTableDetailedAtCatalogCutForTest(
        *backend, layout, CasRefCatalog::read(*backend, layout), removed);
    EXPECT_EQ(recovered.state.getLifecycle(), RefLifecycle::Removed);
    EXPECT_EQ(recovered.state.getRemoveTxnId(), (RefTxnId{1, 2}));

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    const GcState st = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    const CasFoldSeal seal = decodeFoldSeal(
        backend->get(layout.foldSealKey(st.snap_generation, st.snap_attempt))->bytes);
    const auto row_it = seal.ref_lives.find(life.incarnation);
    ASSERT_NE(row_it, seal.ref_lives.end());
    ASSERT_TRUE(row_it->second.cleanup_evidence.has_value());
    EXPECT_EQ(row_it->second.cleanup_evidence->remove_txn_id, (RefTxnId{1, 2}));
    EXPECT_TRUE(backend->head(ckpt_key).exists);
    for (const String & key : backend->touchedKeys())
        EXPECT_EQ(key.find("/_cleanup/"), String::npos) << key;

    /// Round 2 drops `removed`'s catalog row (the pre-fold drain, using round 1's `cleanup_evidence`),
    /// which makes THIS round's own hot-scan catalog cut genuinely, provably empty -- so its destructive
    /// gate opens for the first time (`catalog_cut_proved_empty`), and the namespace janitor -- a
    /// separate `namespace_cleanup` phase the SAME round call also runs -- reclaims the now-orphaned
    /// checkpoint. Reclaiming a removed namespace's `_ckpt` once the pool empties is exactly the
    /// standstill this gate exists to fix, so the janitor running here is the fix working, not a
    /// regression. What this test still pins is the DISCRIMINATION the title promises: the FOLD stage
    /// itself performs no lifecycle-specific physical cleanup (asserted above, unchanged), and the
    /// janitor is attributed the delete via its OWN phase counters -- never inferred from end-state
    /// absence, which would not distinguish "the janitor did it" from "something else did".
    std::map<String, UInt64> janitor_metrics;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "namespace_cleanup")
            janitor_metrics = rec.metrics;
    });
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    gc.setPhaseSink({});

    EXPECT_TRUE(CasRefCatalog::read(*backend, layout).catalog.entries.empty());
    EXPECT_FALSE(CasRefCatalog::lifeIfCataloged(*backend, layout, removed));
    ASSERT_FALSE(janitor_metrics.empty()) << "the namespace_cleanup phase must have run this round";
    EXPECT_GE(janitor_metrics.at("janitor_deleted"), 1u)
        << "the janitor's OWN counter must show the delete -- now that the proved-empty gate has "
           "opened, not because some other site happened to remove the key";
    EXPECT_FALSE(backend->head(ckpt_key).exists);
    EXPECT_EQ(backend->deleteCount(ckpt_key), 1);
}

/// Once a terminal has folded, a later physical read failure is janitor debt, not lifecycle evidence
/// loss. Removing this per-key leak handling would either make the signal disappear or let one dead
/// object prevent the janitor from considering the rest of its page.
TEST(CASGCFrontierGate, PostFoldUnreadableTerminalIsCountedWithoutSuppressingProgress)
{
    auto backend = std::make_shared<PostFoldUnreadableTerminalBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace removed{"00/post-fold-unreadable@cas@"};
    const RootNamespace progressing{"00/post-fold-progress@cas@"};

    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = removed.string(), .txn_id = RefTxnId{1, 1}, .ops = {namespaceBirthOp()},
        .prev_epoch_seal = std::nullopt});
    RefOp remove_op;
    remove_op.kind = RefOpKind::RemoveNamespace;
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = removed.string(), .txn_id = RefTxnId{1, 2}, .ops = {remove_op},
        .prev_epoch_seal = std::nullopt});
    const NamespaceLifeId removed_life = CasRefCatalog::lifeIfCataloged(*backend, layout, removed).value();
    CasRefCatalog::casUpdate(*backend, layout, [&](const RefCatalog & current)
    {
        RefCatalog next = current;
        const auto it = std::find_if(next.entries.begin(), next.entries.end(), [&](const CatalogEntry & entry)
        {
            return entry.ns == removed;
        });
        if (it == next.entries.end())
            throw std::runtime_error("test fixture lost removing catalog row");
        it->state = NsState::Removing;
        it->removal_started_round = 1;
        return next;
    });
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(removed_life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    })).outcome, PutOutcome::Done);

    const DB::UInt128 blob(0xfeed);
    const ManifestRef manifest = publish(*backend, layout, progressing, "victim", 1, blob);
    const ManifestId manifest_id{progressing, manifest};

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    const GcState folded_state = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    const CasFoldSeal folded_seal = decodeFoldSeal(
        backend->get(layout.foldSealKey(folded_state.snap_generation, folded_state.snap_attempt))->bytes);
    const auto folded_row = folded_seal.ref_lives.find(removed_life.incarnation);
    ASSERT_NE(folded_row, folded_seal.ref_lives.end());
    ASSERT_TRUE(folded_row->second.cleanup_evidence.has_value());

    dropRefTransition(*backend, layout, progressing, "victim", manifest);
    const String terminal_key = layout.refLogKey(removed_life, RefTxnId{1, 2});
    const String later_dead_residue = layout.refLogKey(removed_life, RefTxnId{1, 3});
    ASSERT_EQ(backend->putIfAbsent(later_dead_residue, "dead residue after the folded terminal").outcome,
        PutOutcome::Done);
    backend->makeUnreadable(terminal_key);

    std::map<String, UInt64> namespace_cleanup;
    const uint64_t leaks_before
        = ProfileEvents::global_counters[ProfileEvents::CASGCNamespaceCleanupLeaks].load();
    gc.setPhaseSink([&](const GcPhaseRecord & record)
    {
        if (record.phase == "namespace_cleanup")
            namespace_cleanup = record.metrics;
    });
    ScopedCasGcLogCapture log_capture;
    const RoundReport report = runRegularRoundReclaiming(gc);
    gc.setPhaseSink({});

    ASSERT_TRUE(report.acquired_lease);
    EXPECT_FALSE(CasRefCatalog::lifeIfCataloged(*backend, layout, removed))
        << "post-fold physical cleanup cannot gate catalog removal";
    EXPECT_TRUE(CasRefCatalog::lifeIfCataloged(*backend, layout, progressing));
    EXPECT_EQ(report.manifests_deleted, 1u)
        << "the janitor leak cannot promote itself into pool-wide destructive suppression";
    EXPECT_FALSE(backend->head(layout.manifestKey(manifest_id)).exists);
    EXPECT_TRUE(backend->existsIgnoringFault(terminal_key));
    EXPECT_FALSE(backend->existsIgnoringFault(later_dead_residue))
        << "one unreadable key cannot stop the perpetual janitor from deciding the rest of its page";
    ASSERT_FALSE(namespace_cleanup.empty());
    EXPECT_EQ(namespace_cleanup["leaked"], 1u);
    EXPECT_EQ(
        ProfileEvents::global_counters[ProfileEvents::CASGCNamespaceCleanupLeaks].load() - leaks_before,
        1u);
    const String captured = log_capture.captured();
    EXPECT_NE(captured.find(terminal_key), String::npos);
    EXPECT_NE(captured.find("leak"), String::npos);
}

TEST(CASGCFrontierGate, UnmatchedAdoptedParentLifeDoesNotSuppressAuthoritativeDeletion)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/unmatched-parent@cas@"};
    const DB::UInt128 blob(0xcafe);
    const ManifestRef mref = publish(*backend, layout, ns, "victim", 1, blob);
    const ManifestId manifest_id{ns, mref};

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_TRUE(backend->head(layout.manifestKey(manifest_id)).exists);

    const GcState before = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    const String parent_seal_key = layout.foldSealKey(before.snap_generation, before.snap_attempt);
    const auto parent_object = backend->get(parent_seal_key);
    ASSERT_TRUE(parent_object);
    CasFoldSeal parent = decodeFoldSeal(parent_object->bytes, before.snap_generation);
    const UInt128 unmatched_life = hexToU128("fedcba98765432100123456789abcdef");
    ASSERT_FALSE(parent.ref_lives.contains(unmatched_life));
    parent.ref_lives.emplace(unmatched_life, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{9, 9}}});
    ASSERT_EQ(
        backend->putOverwrite(parent_seal_key, encodeFoldSeal(parent), parent_object->token).outcome,
        PutOutcome::Done);

    dropRefTransition(*backend, layout, ns, "victim", mref);
    const uint64_t events_before =
        ProfileEvents::global_counters[ProfileEvents::CASGCUnmatchedAdoptedParentLives].load();
    const RoundReport report = runRegularRoundReclaiming(gc);

    ASSERT_TRUE(report.acquired_lease);
    EXPECT_EQ(
        ProfileEvents::global_counters[ProfileEvents::CASGCUnmatchedAdoptedParentLives].load() - events_before,
        1u);
    EXPECT_EQ(report.manifests_deleted, 1u)
        << "an unmatched adopted-parent row is observed and dropped, not promoted to pool-wide suppression";
    EXPECT_FALSE(backend->head(layout.manifestKey(manifest_id)).exists)
        << "the valid manifest candidate must be physically deleted by the same authoritative round";

    const GcState after = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    const CasFoldSeal successor = decodeFoldSeal(
        backend->get(layout.foldSealKey(after.snap_generation, after.snap_attempt))->bytes,
        after.snap_generation);
    EXPECT_FALSE(successor.ref_lives.contains(unmatched_life));
}

TEST(CASCatalogLifecycleReconciler, EmptyCatalogReturnsAuthoritativeCompleteCut)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    ASSERT_TRUE(CasRefCatalog::initializeEmptyForNewPool(*backend, layout).catalog.entries.empty());

    CasFoldSeal parent;
    CatalogLifecycleReconciler reconciler(
        *backend,
        layout,
        parent,
        /*admitted_generation=*/1,
        [](uint64_t)
        {
            return CasRefCatalog::LeaderFenceStatus::Held;
        });
    const CatalogLifecycleReconcileResult result = reconciler.reconcile();

    EXPECT_EQ(result.authority_status, AuthorityStatus::Authoritative);
    EXPECT_EQ(result.catalog_resolution, CatalogResolution::DrainComplete);
    ASSERT_TRUE(result.final_catalog_cut);
    EXPECT_TRUE(result.final_catalog_cut->catalog.entries.empty());
    EXPECT_TRUE(result.retired_lives.empty());
    EXPECT_EQ(result.deleted, 0);
}

TEST(CASCatalogLifecycleReconciler, DeletesEligibleRowsFromReturnedResolutionCuts)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    constexpr size_t deletes = 3;
    seedCompletedRemovingBatch(*backend, store, kGc, deletes);
    const auto parent_object = backend->get(layout.foldSealKey(1, 1));
    ASSERT_TRUE(parent_object);
    const CasFoldSeal parent = decodeFoldSeal(parent_object->bytes);
    backend->clearJournal();
    backend->resetCounts();

    CatalogLifecycleReconciler reconciler(
        *backend,
        layout,
        parent,
        /*admitted_generation=*/1,
        [](uint64_t)
        {
            return CasRefCatalog::LeaderFenceStatus::Held;
        });
    const CatalogLifecycleReconcileResult result = reconciler.reconcile();

    EXPECT_EQ(result.authority_status, AuthorityStatus::Authoritative);
    EXPECT_EQ(result.catalog_resolution, CatalogResolution::DrainComplete);
    EXPECT_EQ(result.deleted, deletes);
    ASSERT_EQ(result.retired_lives.size(), deletes);
    ASSERT_TRUE(result.final_catalog_cut);
    EXPECT_TRUE(result.final_catalog_cut->catalog.entries.empty());
    const std::vector<String> journal = backend->journalSnapshot();
    const String catalog_get = "get " + layout.refCatalogKey();
    EXPECT_EQ(std::count(journal.begin(), journal.end(), catalog_get), deletes + 1);
}

TEST(CASCatalogLifecycleReconciler, ReturnsRetiredLifeWhenAuthorityMovesAfterResolution)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const CompletedRemovingFixture fixture = seedCompletedRemoving(*backend, store, kGc);
    const auto parent_object = backend->get(layout.foldSealKey(1, 1));
    ASSERT_TRUE(parent_object);
    const CasFoldSeal parent = decodeFoldSeal(parent_object->bytes);
    size_t fence_checks = 0;

    CatalogLifecycleReconciler reconciler(
        *backend,
        layout,
        parent,
        /*admitted_generation=*/1,
        [&fence_checks](uint64_t)
        {
            ++fence_checks;
            return fence_checks == 2
                ? CasRefCatalog::LeaderFenceStatus::Moved
                : CasRefCatalog::LeaderFenceStatus::Held;
        });
    const CatalogLifecycleReconcileResult result = reconciler.reconcile();

    EXPECT_EQ(result.authority_status, AuthorityStatus::FencedOut);
    EXPECT_EQ(result.catalog_resolution, CatalogResolution::ExactRowAbsent);
    ASSERT_EQ(result.retired_lives.size(), 1);
    EXPECT_EQ(result.retired_lives.front(),
        NamespaceLifeId::fromCatalogEntry(fixture.ns, fixture.life_id));
    EXPECT_EQ(result.deleted, 0);
    EXPECT_FALSE(result.final_catalog_cut);
}

TEST(CASCatalogLifecycleReconciler, InitialFenceLossReportsEligibleRowStillPresent)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const CompletedRemovingFixture fixture = seedCompletedRemoving(*backend, store, kGc);
    const auto parent_object = backend->get(layout.foldSealKey(1, 1));
    ASSERT_TRUE(parent_object);
    const CasFoldSeal parent = decodeFoldSeal(parent_object->bytes);
    backend->resetCounts();

    CatalogLifecycleReconciler reconciler(
        *backend,
        layout,
        parent,
        /*admitted_generation=*/1,
        [](uint64_t)
        {
            return CasRefCatalog::LeaderFenceStatus::Moved;
        });
    const CatalogLifecycleReconcileResult result = reconciler.reconcile();

    EXPECT_EQ(result.authority_status, AuthorityStatus::FencedOut);
    EXPECT_EQ(result.catalog_resolution, CatalogResolution::ExactRowStillPresent);
    EXPECT_TRUE(result.retired_lives.empty());
    EXPECT_EQ(result.deleted, 0);
    EXPECT_FALSE(result.final_catalog_cut);
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 2)
        << "the initial selection and mandatory erase-resolution cuts are the only catalog reads";
    EXPECT_EQ(backend->casPutCount(layout.refCatalogKey()), 0);
    EXPECT_EQ(CasRefCatalog::lifeIfCataloged(*backend, layout, fixture.ns),
        NamespaceLifeId::fromCatalogEntry(fixture.ns, fixture.life_id));
}

TEST(CASCatalogLifecycleReconciler, RetriesFromTheMandatoryConflictResolutionCut)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    seedCompletedRemoving(*backend, store, kGc);
    const auto parent_object = backend->get(layout.foldSealKey(1, 1));
    ASSERT_TRUE(parent_object);
    const CasFoldSeal parent = decodeFoldSeal(parent_object->bytes);
    backend->clearJournal();
    backend->resetCounts();
    backend->conflictNextCatalogCas(layout.refCatalogKey());

    CatalogLifecycleReconciler reconciler(
        *backend,
        layout,
        parent,
        /*admitted_generation=*/1,
        [](uint64_t)
        {
            return CasRefCatalog::LeaderFenceStatus::Held;
        });
    const CatalogLifecycleReconcileResult result = reconciler.reconcile();

    EXPECT_EQ(result.authority_status, AuthorityStatus::Authoritative);
    EXPECT_EQ(result.catalog_resolution, CatalogResolution::DrainComplete);
    EXPECT_EQ(result.deleted, 1);
    const std::vector<String> journal = backend->journalSnapshot();
    const String catalog_get = "get " + layout.refCatalogKey();
    EXPECT_EQ(std::count(journal.begin(), journal.end(), catalog_get), 3)
        << "the token-conflict retry must reuse its mandatory resolution cut";
}

TEST(CASCatalogLifecycleReconciler, PropagatesAuthorityFailureBeforeEraseCas)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    seedCompletedRemoving(*backend, store, kGc);
    const auto parent_object = backend->get(layout.foldSealKey(1, 1));
    ASSERT_TRUE(parent_object);
    const CasFoldSeal parent = decodeFoldSeal(parent_object->bytes);
    size_t fence_checks = 0;

    CatalogLifecycleReconciler reconciler(
        *backend,
        layout,
        parent,
        /*admitted_generation=*/1,
        [&fence_checks](uint64_t)
        {
            if (++fence_checks == 2)
                throw std::runtime_error("injected reconciler authority failure before CAS");
            return CasRefCatalog::LeaderFenceStatus::Held;
        });
    try
    {
        (void)reconciler.reconcile();
        FAIL() << "the authority exception must propagate";
    }
    catch (const std::runtime_error & e)
    {
        EXPECT_STREQ(e.what(), "injected reconciler authority failure before CAS");
    }
}

TEST(CASCatalogLifecycleReconciler, PropagatesAuthorityFailureAfterMandatoryResolution)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const CompletedRemovingFixture fixture = seedCompletedRemoving(*backend, store, kGc);
    const auto parent_object = backend->get(layout.foldSealKey(1, 1));
    ASSERT_TRUE(parent_object);
    const CasFoldSeal parent = decodeFoldSeal(parent_object->bytes);
    size_t fence_checks = 0;

    CatalogLifecycleReconciler reconciler(
        *backend,
        layout,
        parent,
        /*admitted_generation=*/1,
        [&fence_checks](uint64_t)
        {
            if (++fence_checks == 3)
                throw std::runtime_error("injected reconciler authority failure after resolution");
            return CasRefCatalog::LeaderFenceStatus::Held;
        });
    try
    {
        (void)reconciler.reconcile();
        FAIL() << "the authority exception must propagate";
    }
    catch (const std::runtime_error & e)
    {
        EXPECT_STREQ(e.what(), "injected reconciler authority failure after resolution");
        EXPECT_FALSE(CasRefCatalog::lifeIfCataloged(*backend, layout, fixture.ns));
    }
}

TEST(CASGCFrontierGate, HealthyRebuildUsesTheCatalogLifecycleReconciler)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const CompletedRemovingFixture fixture = seedCompletedRemoving(*backend, store, kGc);
    const uint64_t catalog_cas_before = backend->casPutCount(layout.refCatalogKey());

    Gc gc(store, kGc);
    const RebuildReport result = gc.rebuildBaseline(/*force=*/true);

    EXPECT_TRUE(result.performed);
    EXPECT_FALSE(CasRefCatalog::lifeIfCataloged(*backend, layout, fixture.ns));
    EXPECT_EQ(backend->casPutCount(layout.refCatalogKey()), catalog_cas_before + 1);
}

TEST(CASGCFrontierGate, DamagedStateRebuildDoesNotDeleteCompletedRemovingRows)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/damaged-rebuild-removing@cas@"};
    CasRefCatalog::casAdmitEntry(*backend, layout, store->poolConfig().gc_shards, CatalogEntry{
        .ns = ns, .state = NsState::Live, .incarnation = UInt128{901}});
    CasRefCatalog::casUpdate(*backend, layout, [](const RefCatalog & current)
    {
        RefCatalog next = current;
        next.entries.front().state = NsState::Removing;
        next.entries.front().removal_started_round = 1;
        return next;
    });
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
    const uint64_t catalog_cas_before = backend->casPutCount(layout.refCatalogKey());

    Gc gc(store, kGc);
    const RebuildReport result = gc.rebuildBaseline(/*force=*/false);

    EXPECT_TRUE(result.performed);
    EXPECT_TRUE(CasRefCatalog::lifeIfCataloged(*backend, layout, ns));
    EXPECT_EQ(backend->casPutCount(layout.refCatalogKey()), catalog_cas_before);
}

TEST(CASGCFrontierGate, DeferredRoundDrainsCompletedRemovingBeforeReturning)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/100);
    const Layout & layout = store->layout();
    const RootNamespace removed{"00/deferred-removed@cas@"};
    const UInt128 life_id{77};
    CasRefCatalog::casAdmitEntry(*backend, layout, store->poolConfig().gc_shards, CatalogEntry{
        .ns = removed, .state = NsState::Live, .incarnation = life_id});
    CasRefCatalog::casUpdate(*backend, layout, [&](const RefCatalog & current)
    {
        RefCatalog next = current;
        next.entries[0].state = NsState::Removing;
        next.entries[0].removal_started_round = 1;
        return next;
    });

    CasFoldSeal parent;
    parent.generation = 1;
    parent.ref_lives.emplace(life_id, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 1}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 1}}});
    for (uint64_t shard = 0; shard < store->poolConfig().gc_shards; ++shard)
        parent.condemned_summary.emplace(shard, CondemnedSummary{});
    ASSERT_EQ(backend->putIfAbsent(layout.foldSealKey(1, 1), encodeFoldSeal(parent)).outcome, PutOutcome::Done);
    GcState state;
    state.round = 1;
    state.gc_shards = store->poolConfig().gc_shards;
    state.snap_generation = 1;
    state.snap_attempt = 1;
    state.lease = GcLease{.owner = kGc, .seq = 1};
    ASSERT_EQ(backend->putIfAbsent(layout.gcStateKey(), encodeGcState(state)).outcome, PutOutcome::Done);

    const String ckpt_key = layout.refCkptKey(NamespaceLifeId::fromCatalogEntry(removed, life_id));
    ASSERT_EQ(backend->putIfAbsent(ckpt_key, "inert checkpoint debris").outcome, PutOutcome::Done);
    const uint64_t catalog_cas_before = backend->casPutCount(layout.refCatalogKey());

    Gc gc(store, kGc);
    const RoundReport report = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(report.acquired_lease);
    EXPECT_TRUE(report.deferred);
    EXPECT_TRUE(CasRefCatalog::read(*backend, layout).catalog.entries.empty());
    EXPECT_FALSE(CasRefCatalog::lifeIfCataloged(*backend, layout, removed));
    EXPECT_EQ(backend->casPutCount(layout.refCatalogKey()), catalog_cas_before + 1);
    EXPECT_TRUE(backend->head(ckpt_key).exists);
    EXPECT_EQ(backend->deleteCount(ckpt_key), 0);
}

TEST(CASGCFrontierGate, StaleIssuedCatalogCasLosesAfterNewLeaderHelpsBeforeListing)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const UInt128 leader_b = hexToU128("00000000000000000000000000000002");
    const CompletedRemovingFixture fixture = seedCompletedRemoving(*backend, store, kGc);
    backend->clearJournal();
    backend->blockNextCatalogCas(layout.refCatalogKey());

    std::exception_ptr leader_a_failure;
    std::thread leader_a([&]
    {
        try
        {
            Gc gc_a(store, kGc);
            (void)runRegularRoundReclaiming(gc_a);
        }
        catch (...)
        {
            leader_a_failure = std::current_exception();
        }
    });
    backend->waitForBlockedCatalogCas();

    transferGcLease(*backend, layout, leader_b);
    RoundReport report_b;
    std::exception_ptr leader_b_failure;
    /// `fixture.ns` is Removing with a durable `_ckpt`, same shape as
    /// `CleanupEvidenceLeavesRemovedNamespaceCheckpointForJanitor`: once leader_b's round drops its
    /// catalog row, the resulting cut is genuinely, provably empty, the destructive gate opens, and the
    /// namespace janitor -- a separate `namespace_cleanup` phase within this SAME round -- reclaims the
    /// checkpoint. Captured so the assertions below can attribute the delete to the janitor rather than
    /// assume survival.
    std::map<String, UInt64> janitor_metrics_b;
    try
    {
        Gc gc_b(store, leader_b);
        gc_b.setPhaseSink([&](const GcPhaseRecord & rec)
        {
            if (rec.phase == "namespace_cleanup")
                janitor_metrics_b = rec.metrics;
        });
        report_b = runRegularRoundReclaiming(gc_b);
        gc_b.setPhaseSink({});
    }
    catch (...)
    {
        leader_b_failure = std::current_exception();
    }

    const std::vector<String> before_a_release = backend->journalSnapshot();
    backend->releaseBlockedCatalogCas();
    leader_a.join();

    ASSERT_FALSE(leader_b_failure);
    ASSERT_TRUE(report_b.acquired_lease);
    ASSERT_FALSE(report_b.deferred);
    ASSERT_TRUE(CasRefCatalog::read(*backend, layout).catalog.entries.empty());

    const size_t catalog_cas_end = findJournalAfter(before_a_release, "cas_end " + layout.refCatalogKey(), 0);
    ASSERT_LT(catalog_cas_end, before_a_release.size());
    const size_t conclusive_rescan = findJournalAfter(
        before_a_release, "get " + layout.refCatalogKey(), catalog_cas_end + 1);
    ASSERT_LT(conclusive_rescan, before_a_release.size());
    const size_t stream_list = findJournalAfter(
        before_a_release, "list " + layout.casRefsPrefix(), conclusive_rescan + 1);
    ASSERT_LT(stream_list, before_a_release.size());
    const size_t fresh_catalog_cut = findJournalAfter(
        before_a_release, "get " + layout.refCatalogKey(), stream_list + 1);
    ASSERT_LT(fresh_catalog_cut, before_a_release.size());
    const GcState adopted = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    const String successor_seal_key = layout.foldSealKey(adopted.snap_generation, adopted.snap_attempt);
    const size_t successor_seal_put = findJournalAfter(
        before_a_release, "put_end " + successor_seal_key, fresh_catalog_cut + 1);
    ASSERT_LT(successor_seal_put, before_a_release.size());
    const size_t successor_adoption = findJournalAfter(
        before_a_release, "cas_end " + layout.gcStateKey(), successor_seal_put + 1);
    ASSERT_LT(successor_adoption, before_a_release.size());
    EXPECT_LT(catalog_cas_end, conclusive_rescan);
    EXPECT_LT(conclusive_rescan, stream_list);
    EXPECT_LT(stream_list, fresh_catalog_cut);
    /// The invariant this ordering must still prove: the fold's OWN walk plan is built from the single
    /// hot-scan cut, taken immediately after the ref-object LIST, with no earlier catalog read sneaking
    /// into that construction. `fresh_catalog_cut` is defined as the FIRST catalog `get` after
    /// `stream_list` (the `findJournalAfter` search above), so that already holds by construction --
    /// the walk plan physically cannot have consumed an earlier one.
    ///
    /// What this test used to also assert -- no SECOND catalog read anywhere before the seal PUT -- is
    /// no longer the right claim once the destructive gate can open on a proved-empty cut: other
    /// destructive families this SAME round now also runs (the orphan-manifest sweep, the namespace
    /// janitor) take their OWN separate catalog cuts by design, each after its own candidate listing,
    /// to resolve authority against a fresh read rather than the fold's frozen one -- exactly the shape
    /// measured here (`list p/cas/manifests/` immediately followed by a second `get
    /// p/cas/ref_catalog`, before the seal PUT, from the orphan sweep). That is expected, not redundant,
    /// so it is not asserted against; the fold's own single-cut plan construction is what remains pinned.
    EXPECT_LT(fresh_catalog_cut, successor_seal_put);
    EXPECT_LT(successor_seal_put, successor_adoption);

    ASSERT_TRUE(leader_a_failure);
    EXPECT_FALSE(CasRefCatalog::lifeIfCataloged(*backend, layout, fixture.ns));
    /// Same discrimination as `CleanupEvidenceLeavesRemovedNamespaceCheckpointForJanitor`: leader_b's
    /// round both drops `fixture.ns`'s catalog row AND, because the resulting cut is genuinely,
    /// provably empty, opens the destructive gate -- so the namespace janitor reclaims the checkpoint
    /// in this SAME round. Attribute the delete to the janitor's own counter rather than assume either
    /// survival (the old expectation) or absence (which an unchecked dereference here cannot
    /// distinguish from "never existed").
    ASSERT_FALSE(janitor_metrics_b.empty()) << "the namespace_cleanup phase must have run this round";
    EXPECT_GE(janitor_metrics_b.at("janitor_deleted"), 1u)
        << "the janitor's OWN counter must show the delete, now that the proved-empty gate has opened";
    EXPECT_FALSE(backend->get(fixture.checkpoint_key).has_value());
    EXPECT_EQ(backend->deleteCount(fixture.checkpoint_key), 1);
}

TEST(CASGCFrontierGate, LostCatalogCasResponseIsResolvedBeforeListing)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const CompletedRemovingFixture fixture = seedCompletedRemoving(*backend, store, kGc);
    backend->clearJournal();
    backend->loseNextCatalogCasResponse(layout.refCatalogKey());

    /// Same shape as `StaleIssuedCatalogCasLosesAfterNewLeaderHelpsBeforeListing`: `fixture.ns` is
    /// Removing with a durable `_ckpt`, so this round both drops its catalog row and, because the
    /// resulting cut is genuinely, provably empty, opens the destructive gate -- the namespace janitor
    /// (a separate `namespace_cleanup` phase within this SAME round) reclaims the checkpoint. Captured
    /// so the assertions below attribute the delete to the janitor's own counter.
    std::map<String, UInt64> janitor_metrics;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "namespace_cleanup")
            janitor_metrics = rec.metrics;
    });
    const RoundReport report = runRegularRoundReclaiming(gc);
    gc.setPhaseSink({});
    ASSERT_TRUE(report.acquired_lease);
    ASSERT_FALSE(report.deferred);

    const std::vector<String> journal = backend->journalSnapshot();
    const size_t response_lost = findJournalAfter(
        journal, "cas_response_lost " + layout.refCatalogKey(), 0);
    ASSERT_LT(response_lost, journal.size());
    const size_t conclusive_rescan = findJournalAfter(
        journal, "get " + layout.refCatalogKey(), response_lost + 1);
    ASSERT_LT(conclusive_rescan, journal.size());
    const size_t stream_list = findJournalAfter(
        journal, "list " + layout.casRefsPrefix(), conclusive_rescan + 1);
    ASSERT_LT(stream_list, journal.size());
    const size_t fresh_catalog_cut = findJournalAfter(
        journal, "get " + layout.refCatalogKey(), stream_list + 1);
    ASSERT_LT(fresh_catalog_cut, journal.size());
    EXPECT_LT(response_lost, conclusive_rescan);
    EXPECT_LT(conclusive_rescan, stream_list);
    EXPECT_LT(stream_list, fresh_catalog_cut);

    EXPECT_FALSE(CasRefCatalog::lifeIfCataloged(*backend, layout, fixture.ns));
    /// See the discrimination comment in `CleanupEvidenceLeavesRemovedNamespaceCheckpointForJanitor`:
    /// attribute the delete to the janitor's own counter, never to end-state absence alone, and never
    /// assume survival -- both would be indistinguishable from a bug on this exact line (the old
    /// unchecked `->bytes` here is what aborted the whole binary once the janitor started reclaiming).
    ASSERT_FALSE(janitor_metrics.empty()) << "the namespace_cleanup phase must have run this round";
    EXPECT_GE(janitor_metrics.at("janitor_deleted"), 1u)
        << "the janitor's OWN counter must show the delete, now that the proved-empty gate has opened";
    EXPECT_FALSE(backend->get(fixture.checkpoint_key).has_value());
    EXPECT_EQ(backend->deleteCount(fixture.checkpoint_key), 1);
}

/// A stale leader may learn from its mandatory resolution read that the old life is gone, and must
/// invalidate that exact runtime, but loss of the leader fence remains the control outcome. It must
/// abort before the hot LIST and cannot build or publish any successor generation.
TEST_P(CASGCCompletedRemovalFenceRace, FencedLeaderStopsAfterWinnerRemovesOrReplacesLife)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const UInt128 leader_b = hexToU128("00000000000000000000000000000002");
    const CompletedRemovingFixture fixture = seedCompletedRemoving(*backend, store, kGc);
    const NamespaceLifeId predecessor_life
        = NamespaceLifeId::fromCatalogEntry(fixture.ns, fixture.life_id);
    ASSERT_TRUE(store->refTableRecoveredForTest(fixture.ns))
        << "the fixture must retain a resident predecessor runtime before removal";
    ASSERT_EQ(store->refTableLifeForTest(fixture.ns), predecessor_life);
    const uint64_t predecessor_runtime = store->refTableRuntimeIdentityForTest(fixture.ns);
    ASSERT_NE(predecessor_runtime, 0u);
    backend->clearJournal();
    backend->blockNextCatalogCas(layout.refCatalogKey());

    std::exception_ptr leader_a_failure;
    std::thread leader_a([&]
    {
        try
        {
            Gc gc_a(store, kGc);
            (void)runRegularRoundReclaiming(gc_a);
        }
        catch (...)
        {
            leader_a_failure = std::current_exception();
        }
    });
    backend->waitForBlockedCatalogCas();

    transferGcLease(*backend, layout, leader_b);
    const CasRefCatalog::Snapshot observed = CasRefCatalog::read(*backend, layout);
    RefCatalog winner_catalog;
    if (GetParam() == CompetingCatalogOutcome::Replacement)
    {
        winner_catalog.entries.push_back(CatalogEntry{
            .ns = fixture.ns,
            .state = NsState::Live,
            .incarnation = UInt128{178}});
        /// Mirror production's publish-then-flip order: the successor life needs a readable `_ckpt`
        /// before its catalog row can read `Live`, or `chooseRecoveryGrounding` rejects it.
        const NamespaceLifeId successor_life = NamespaceLifeId::fromCatalogEntry(fixture.ns, UInt128{178});
        backend->putIfAbsent(layout.refCkptKey(successor_life), encodeRefCkpt(RefCkpt{
            .life_epoch = 1,
            .committed_through = std::nullopt,
            .checkpoint_snapshot_id = std::nullopt,
            .last_epoch_seal = std::nullopt,
        }));
    }
    ASSERT_EQ(backend->casPut(
        layout.refCatalogKey(), encodeRefCatalog(winner_catalog), observed.token).outcome,
        CasOutcome::Committed);

    backend->clearJournal();
    const uint64_t plans_before  /// NOLINT(clang-analyzer-deadcode.DeadStores)
        = ProfileEvents::global_counters[ProfileEvents::CASGCRefWalkPlansBuilt].load();
    backend->releaseBlockedCatalogCas();
    leader_a.join();

    const std::vector<String> journal = backend->journalSnapshot();
    ASSERT_TRUE(leader_a_failure);
    try
    {
        std::rethrow_exception(leader_a_failure);
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR);
        EXPECT_NE(e.message().find("pre-fold drain lost authority"), String::npos) << e.message();
    }
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASGCRefWalkPlansBuilt].load() - plans_before, 0u);
    EXPECT_EQ(findJournalAfter(journal, "list " + layout.casRefsPrefix(), 0), journal.size());
    EXPECT_EQ(findJournalAfter(journal, "cas_begin " + layout.gcStateKey(), 0), journal.size());
    EXPECT_FALSE(std::any_of(journal.begin(), journal.end(), [](const String & entry)
    {
        return entry.starts_with("put_begin ") && entry.ends_with("/fold_seal");
    }));
    EXPECT_LT(findJournalAfter(journal, "get " + layout.refCatalogKey(), 0), journal.size())
        << "the stale leader must still complete mandatory erase resolution";

    (void)store->namespaceLife(fixture.ns);
    EXPECT_NE(store->refTableRuntimeIdentityForTest(fixture.ns), 0u);
    ASSERT_TRUE(store->refTableLifeForTest(fixture.ns));
    EXPECT_NE(store->refTableLifeForTest(fixture.ns), predecessor_life)
        << "the next name-based resolution must not retain the retired predecessor life";
}

INSTANTIATE_TEST_SUITE_P(
    CASWinnerShape,
    CASGCCompletedRemovalFenceRace,
    testing::Values(CompetingCatalogOutcome::Absent, CompetingCatalogOutcome::Replacement),
    [](const testing::TestParamInfo<CompetingCatalogOutcome> & parameter)
    {
        return parameter.param == CompetingCatalogOutcome::Absent ? "Absent" : "Replacement";
    });

/// One initial full catalog read selects the first row; each successful erase's mandatory resolution
/// read becomes the next selection snapshot. Therefore N uncontended deletes cost N+1 reads before
/// the hot LIST. The round then takes one post-LIST walk-plan cut and, later in the separate
/// `namespace_cleanup` phase, one post-page janitor cut.
TEST(CASGCFrontierGate, CompletedRemovalDrainUsesNPlusOneCatalogReads)
{
    auto backend = std::make_shared<DrainRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    constexpr size_t deletes = 3;
    seedCompletedRemovingBatch(*backend, store, kGc, deletes);
    backend->clearJournal();
    backend->resetCounts();

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    const std::vector<String> journal = backend->journalSnapshot();
    const size_t stream_list = findJournalAfter(journal, "list " + layout.casRefsPrefix(), 0);
    ASSERT_LT(stream_list, journal.size());
    const String catalog_get = "get " + layout.refCatalogKey();
    EXPECT_EQ(std::count(journal.begin(), journal.begin() + static_cast<ptrdiff_t>(stream_list), catalog_get),
        deletes + 1);
    const size_t walk_plan_cut = findJournalAfter(journal, catalog_get, stream_list);
    ASSERT_LT(walk_plan_cut, journal.size());
    /// Between the hot walk-plan cut and the janitor's own page, the orphan-manifest sweep -- ANOTHER
    /// destructive family the now-open gate also unlocks (the batch drain above empties the catalog, so
    /// this round's frontier is proved empty and the sweep's own `!suppress_destructive` gate opens
    /// too) -- lists its own manifest candidates and takes its OWN separate catalog cut to resolve
    /// authority, exactly as the janitor does. Located explicitly so the final read count below states
    /// what it counts rather than drifting silently the next time a family is unlocked.
    const size_t orphan_sweep_list = findJournalAfter(journal, "list " + layout.casManifestsPrefix(), walk_plan_cut);
    ASSERT_LT(orphan_sweep_list, journal.size());
    const size_t orphan_sweep_cut = findJournalAfter(journal, catalog_get, orphan_sweep_list);
    ASSERT_LT(orphan_sweep_cut, journal.size());
    const size_t janitor_list
        = findJournalAfter(journal, "list " + layout.namespaceRootPrefix(), orphan_sweep_cut);
    ASSERT_LT(janitor_list, journal.size());
    const size_t janitor_cut = findJournalAfter(journal, catalog_get, janitor_list);
    ASSERT_LT(janitor_cut, journal.size());
    EXPECT_EQ(findJournalAfter(journal, catalog_get, janitor_cut + 1), journal.size())
        << "one hot walk-plan cut, one orphan-sweep cut, and one janitor page cut are the only "
           "post-drain catalog reads";
    EXPECT_EQ(backend->listCount(layout.namespaceStreamRootPrefix()), 1u);
    EXPECT_EQ(backend->listCount(layout.namespaceRootPrefix()), 1u);
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), deletes + 4)
        << "N+1 drain reads, one post-hot-LIST walk-plan cut, one orphan-manifest-sweep cut (now that "
           "the proved-empty gate has opened, unlocking that destructive family too), and one separate "
           "post-janitor-page cut";
}
