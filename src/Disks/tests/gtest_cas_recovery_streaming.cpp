#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include "cas_test_helpers.h"

#include <Common/Exception.h>
#include <Common/scope_guard_safe.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <thread>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int S3_ERROR;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

/// A deterministic accountant for the streaming-recovery memory probe: each recovery loop reports
/// `+footprint` while one decoded transaction is resident and `-footprint` once it is discarded, so
/// `peak` is the maximum summed decoded-transaction footprint ever resident at one instant. Streaming
/// holds one transaction; the retired whole-tail materialiser -- and the test-local control that stands
/// in for it -- held the entire tail. Deterministic (it accounts the footprints the probe is handed, a
/// pure function of decoded content, not RSS), so it is stable under ASan quarantine noise.
struct PeakTracker
{
    std::atomic<int64_t> alive_bytes{0};
    std::atomic<int64_t> peak_bytes{0};

    std::function<void(int64_t)> probe()
    {
        return [this](int64_t delta)
        {
            const int64_t now = alive_bytes.fetch_add(delta, std::memory_order_relaxed) + delta;
            int64_t prev = peak_bytes.load(std::memory_order_relaxed);
            while (now > prev && !peak_bytes.compare_exchange_weak(prev, now, std::memory_order_relaxed))
            {
            }
        };
    }

    int64_t peak() const { return peak_bytes.load(std::memory_order_relaxed); }
    int64_t alive() const { return alive_bytes.load(std::memory_order_relaxed); }
};

/// A distinct manifest per call: `build_sequence` carries the identity so every generated
/// `(ref_name, manifest_ref)` add-precommit is a legal transition (no manifest is owned twice).
ManifestRef mref(uint64_t seq)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = 1};
}

/// One maximum-shaped ref-log transaction: `num_ops` add-precommit ops over distinct
/// `(ref_name, manifest_ref)` pairs (plus a leading `namespace_birth` for the first transaction of a
/// never-born table). Each pair is unique across the whole tail (the running `manifest_seq`), so the
/// tail replays cleanly and the candidate state simply grows -- the point is a large decoded body per
/// transaction, which is what makes the whole-tail vector's resident footprint N times a single
/// transaction's.
RefLogTxn makeBigTxn(const String & ns, RefTxnId id, size_t num_ops, uint64_t & manifest_seq, bool birth)
{
    RefLogTxn txn;
    txn.ns = ns;
    txn.txn_id = id;
    if (birth)
        txn.ops.push_back(namespaceBirthOp());
    for (size_t i = 0; i < num_ops; ++i)
    {
        const String ref_name = "rs_" + std::to_string(id.ref_sequence) + "_" + std::to_string(i);
        txn.ops.push_back(ownerTransitionOp(
            std::nullopt, RefOwnerBinding{RefOwnerKind::Precommit, ref_name, mref(manifest_seq)}));
        ++manifest_seq;
    }
    return txn;
}

/// Seed `num_txns` maximum-shaped transactions at ids {1,1}..{1,num_txns} directly into `ns`'s `_log/`
/// stream, and return the resident DECODED footprint (`decodedRefLogTxnFootprint`) of the largest single
/// transaction plus the total across all. The largest single footprint is the streaming peak (one
/// transaction resident at a time); the total is what a whole-tail materialiser holds resident at once.
/// Footprint -- not the compressed stored size -- is the bound's currency: it is what actually sits in
/// memory and what a materialising regression accumulates N-fold, and it is a deterministic function of
/// the decoded content (identical whether computed on the built or the decoded transaction).
struct SeededTail
{
    uint64_t max_single_footprint = 0;
    uint64_t total_footprint = 0;
};

SeededTail seedBigTail(
    InMemoryBackend & backend, const Layout & layout, const RootNamespace & ns,
    size_t num_txns, size_t ops_per_txn, uint64_t & manifest_seq)
{
    SeededTail seeded;
    for (size_t t = 0; t < num_txns; ++t)
    {
        const RefLogTxn txn = makeBigTxn(ns.string(), RefTxnId{1, t + 1}, ops_per_txn, manifest_seq, /*birth=*/t == 0);
        const uint64_t footprint = decodedRefLogTxnFootprint(txn);
        seeded.max_single_footprint = std::max(seeded.max_single_footprint, footprint);
        seeded.total_footprint += footprint;
        fixture::writeRefLogRaw(backend, layout, txn);
    }
    /// This helper always builds a recoverable `Live` life. Tests that need the distinct missing-
    /// checkpoint corruption shape use the lower-level raw writers directly instead.
    writeRecoverableCkptForRawFixture(
        backend, layout, ns,
        RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, static_cast<uint64_t>(num_txns)},
                .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});
    return seeded;
}

/// Bounded busy-wait on a test-observable predicate (the established `yield()`-poll idiom for recovery
/// waiters, see `CasPool::refRecoveryWaitersForTest`). Bound is a generous wall-clock ceiling that only
/// trips on a genuine hang, never in the normal fast path; returns false on timeout so the caller can
/// release any blocked threads before asserting.
template <typename Pred>
bool pollUntil(Pred pred)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!pred())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }
    return true;
}

/// Backend that drops one selected `_log/` object on its FIRST GET (a concurrent-cleanup vanish),
/// then serves it normally, and counts fresh (cursor-empty) LISTs of the ref prefix so a test can
/// prove a stable checkpoint verdict did not spin on the advisory listing.
class VanishMidTailOnceBackend : public InMemoryBackend
{
public:
    using InMemoryBackend::get;   /// keep the one-arg convenience overload visible past our override

    String target_log_key;
    String refs_prefix;
    std::atomic<bool> armed{false};
    std::atomic<bool> vanished{false};
    std::atomic<int> fresh_list_count{0};

    std::optional<GetResult> get(const String & key, Range range) override
    {
        if (armed.load() && key == target_log_key && !vanished.exchange(true))
            return std::nullopt;   /// selected object gone between LIST and GET; recovery must re-LIST
        return InMemoryBackend::get(key, range);
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        if (armed.load() && prefix == refs_prefix && cursor.empty())
            fresh_list_count.fetch_add(1, std::memory_order_relaxed);
        return InMemoryBackend::list(prefix, cursor, limit);
    }
};

/// Backend that replaces one selected `_log/` object's body with a valid-but-foreign ref-log object
/// (a different namespace in the body): decoding it fails with CORRUPTED_DATA (body/key mismatch), the
/// durable-corruption class recovery must fail fast on -- no re-LIST loop.
class CorruptLogOnGetBackend : public InMemoryBackend
{
public:
    using InMemoryBackend::get;   /// keep the one-arg convenience overload visible past our override

    String target_log_key;
    String corrupt_bytes;
    String refs_prefix;
    std::atomic<bool> armed{false};
    std::atomic<int> refs_list_count{0};

    std::optional<GetResult> get(const String & key, Range range) override
    {
        auto got = InMemoryBackend::get(key, range);
        if (armed.load() && got && key == target_log_key)
            got->bytes = corrupt_bytes;
        return got;
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        if (armed.load() && prefix == refs_prefix && cursor.empty())
            refs_list_count.fetch_add(1, std::memory_order_relaxed);
        return InMemoryBackend::list(prefix, cursor, limit);
    }
};

/// Backend that blocks the first exact log GET while recovery holds no state lock. A concurrent second
/// caller can then reach `recovery_cv`, while the LIST counter proves neither caller enumerates the
/// recovery stream.
class BlockingFirstLogGetBackend : public InMemoryBackend
{
public:
    using InMemoryBackend::get;

    String refs_prefix;
    String target_log_key;
    std::atomic<bool> armed{false};
    std::atomic<bool> blocked{false};
    std::atomic<int> list_calls{0};
    std::function<void()> on_first_target_get;

    std::optional<GetResult> get(const String & key, Range range) override
    {
        if (armed.load() && key == target_log_key && !blocked.exchange(true))
            on_first_target_get();
        return InMemoryBackend::get(key, range);
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        if (armed.load() && prefix == refs_prefix && cursor.empty())
            list_calls.fetch_add(1, std::memory_order_relaxed);
        return InMemoryBackend::list(prefix, cursor, limit);
    }
};

}

/// Test 14 (load-bearing memory bound): a long tail of maximum-shaped transactions replays under a hard
/// peak bound (twice the largest single transaction's decoded footprint) that a whole-tail materialiser
/// -- which holds every decoded transaction resident at once -- provably exceeds. The bound is computed
/// from the fixture's own footprints and the whole-tail total is asserted to exceed it, so the bound is
/// a property of the fixture, not a lucky constant. Its materialising counterpart,
/// `MaterializingControlExceedsMemoryBound`, trips this same bound.
TEST(CASRecoveryStreaming, LongTailReplaysUnderMemoryBound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    seedPoolMetaForRestart(*backend);
    const RootNamespace ns{"00/aa@cas@"};

    constexpr size_t kTxns = 24;
    constexpr size_t kOpsPerTxn = 250;
    uint64_t manifest_seq = 1;
    const SeededTail seeded = seedBigTail(*backend, layout, ns, kTxns, kOpsPerTxn, manifest_seq);

    const uint64_t bound = 2 * seeded.max_single_footprint;
    ASSERT_GT(seeded.total_footprint, bound)
        << "fixture must make the whole tail (" << seeded.total_footprint
        << " B) provably exceed the bound (" << bound << " B)";

    PeakTracker tracker;
    setRecoveryReplayMemoryProbeForTest(tracker.probe());
    SCOPE_EXIT({ setRecoveryReplayMemoryProbeForTest({}); });

    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(*backend, layout);
    const RefTableState state = recoverRefTableDetailedAtCatalogCutForTest(*backend, layout, catalog_cut, ns).state;
    EXPECT_EQ(state.getPrecommits().size(), kTxns * kOpsPerTxn) << "the whole tail must have replayed";
    EXPECT_LE(tracker.peak(), static_cast<int64_t>(bound))
        << "streaming recovery must hold at most ~one decoded transaction (peak " << tracker.peak()
        << " B) not the whole " << kTxns << "-transaction tail (" << seeded.total_footprint << " B)";
    /// Lower bound (the accountant's fail-close): the peak must reach at least one whole decoded
    /// transaction's footprint. This couples the assertion to the production report calls -- delete them
    /// and the peak collapses to zero, failing HERE instead of passing vacuously under the upper bound.
    EXPECT_GE(tracker.peak(), static_cast<int64_t>(seeded.max_single_footprint))
        << "the probe must observe at least one whole decoded transaction resident (peak " << tracker.peak()
        << " B, one transaction " << seeded.max_single_footprint
        << " B) -- a zero peak means the production report calls were removed and the bound guards nothing";
    EXPECT_EQ(tracker.alive(), 0) << "every decoded transaction must be discarded after it is applied";
}

/// Test 14 (materialising RED control): the discriminating counterpart to the streaming bound above. A
/// control that GETs+decodes the WHOLE tail into a vector BEFORE applying it -- the retired whole-tail
/// shape -- holds every decoded transaction resident at once. Driven through the SAME memory probe as
/// streaming recovery, its peak must EXCEED the same bound the streaming path stays under. This is the
/// regression the memory guard exists to catch, and the guard discriminates precisely because the probe
/// now accounts the caller's whole resident set (each decoded transaction for the span it is held), not
/// one apply in isolation. Under the retired stored-byte-in-`applyOne` probe this control's peak stayed
/// at one transaction (see the RED capture in the round-2 fix report); it now correctly trips.
TEST(CASRecoveryStreaming, MaterializingControlExceedsMemoryBound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    seedPoolMetaForRestart(*backend);
    const RootNamespace ns{"00/aa@cas@"};

    constexpr size_t kTxns = 24;
    constexpr size_t kOpsPerTxn = 250;
    uint64_t manifest_seq = 1;
    const SeededTail seeded = seedBigTail(*backend, layout, ns, kTxns, kOpsPerTxn, manifest_seq);

    const uint64_t bound = 2 * seeded.max_single_footprint;
    ASSERT_GT(seeded.total_footprint, bound)
        << "fixture must make the whole tail (" << seeded.total_footprint
        << " B) provably exceed the bound (" << bound << " B)";

    PeakTracker tracker;
    setRecoveryReplayMemoryProbeForTest(tracker.probe());
    SCOPE_EXIT({ setRecoveryReplayMemoryProbeForTest({}); });

    /// Materialise the WHOLE tail first (retired shape): every decoded transaction stays resident in
    /// `resident_txns`, and its footprint is reported to the probe up front, released only AFTER the whole
    /// tail has been applied -- exactly the memory profile streaming recovery replaced.
    std::vector<RefLogTxn> resident_txns;
    int64_t held = 0;
    for (size_t t = 1; t <= kTxns; ++t)
    {
        const auto got = backend->get(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, t}));
        ASSERT_TRUE(got.has_value());
        RefLogTxn txn = decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), RefTxnId{1, t});
        const int64_t footprint = static_cast<int64_t>(decodedRefLogTxnFootprint(txn));
        reportReplayMemoryDelta(footprint);
        held += footprint;
        resident_txns.push_back(std::move(txn));
    }

    RefReplayBuilder builder(std::nullopt);
    for (RefLogTxn & txn : resident_txns)
        builder.applyOne(std::move(txn), 0);
    const RecoveryResult result = std::move(builder).finish();
    EXPECT_EQ(result.state.getPrecommits().size(), kTxns * kOpsPerTxn) << "the whole tail must have applied";

    EXPECT_GT(tracker.peak(), static_cast<int64_t>(bound))
        << "the materialising control holds the whole tail resident; the probe must exceed the "
           "single-transaction bound (peak " << tracker.peak() << " B, bound " << bound << " B)";

    reportReplayMemoryDelta(-held);   /// release the whole tail
    EXPECT_EQ(tracker.alive(), 0);
}

/// Test 14 (vanished-selected-object leg): once the exact `_ckpt` commits a finite frontier, a missing
/// record inside it is not something a fresh LIST may reinterpret as a shorter stream. With the same
/// checkpoint token still durable, recovery fails closed immediately instead of accepting incomplete
/// state or spinning on an advisory enumeration.
TEST(CASRecoveryStreaming, MidTailVanishedObjectFailsClosedAgainstStableAuthority)
{
    auto backend = std::make_shared<VanishMidTailOnceBackend>();
    seedPoolMetaForRestart(*backend);
    const Layout layout("p");
    const RootNamespace ns{"00/aa@cas@"};

    const uint64_t seq1 = publishCommittedTransition(*backend, layout, ns, "a", std::nullopt, mref(1));
    const uint64_t seq2 = publishCommittedTransition(*backend, layout, ns, "b", std::nullopt, mref(2));
    const uint64_t seq3 = publishCommittedTransition(*backend, layout, ns, "c", std::nullopt, mref(3));
    ASSERT_LT(seq1, seq2);
    ASSERT_LT(seq2, seq3);
    /// Semantic publication already durably advances the exact checkpoint frontier to `seq3`.

    auto store = openPoolForTest(backend);
    backend->refs_prefix = layout.namespaceStreamPrefix(fixture::fixtureLife(ns));
    backend->target_log_key = layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, seq2});   /// vanish a mid-tail object
    backend->armed = true;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)store->listRefs(ns); });
    EXPECT_TRUE(backend->vanished.load()) << "the selected committed object must actually have vanished";
    EXPECT_EQ(backend->fresh_list_count.load(), 0)
        << "the exact checkpoint frontier makes recovery stream enumeration unnecessary";
}

/// Test 14 (durable-corruption leg): a `_log/` object whose body decodes to a foreign namespace is
/// durable corruption, not a transient vanish -- recovery discards the candidate and fails fast with
/// no re-LIST loop. Asserts the throw, zero restarts, and a single LIST.
TEST(CASRecoveryStreaming, CorruptObjectFailsFast)
{
    auto backend = std::make_shared<CorruptLogOnGetBackend>();
    seedPoolMetaForRestart(*backend);
    const Layout layout("p");
    const RootNamespace ns{"00/aa@cas@"};

    publishCommittedTransition(*backend, layout, ns, "a", std::nullopt, mref(1));
    const uint64_t seq2 = publishCommittedTransition(*backend, layout, ns, "b", std::nullopt, mref(2));
    publishCommittedTransition(*backend, layout, ns, "c", std::nullopt, mref(3));
    /// Semantic publication already durably advances the exact checkpoint frontier.

    /// A structurally valid ref-log object for a DIFFERENT namespace: it decompresses and parses, but
    /// its body namespace does not match the key, which `decodeRefLogTxn` rejects as CORRUPTED_DATA.
    RefLogTxn foreign;
    foreign.ns = "99/zz@cas@";
    foreign.txn_id = RefTxnId{1, seq2};
    foreign.ops = {namespaceBirthOp()};
    backend->corrupt_bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(foreign));

    auto store = openPoolForTest(backend);
    backend->refs_prefix = layout.namespaceStreamPrefix(fixture::fixtureLife(ns));
    backend->target_log_key = layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, seq2});
    backend->armed = true;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->resolveRef(ns, "a"); });
    /// The exact checkpoint frontier determines this GET. A corrupt committed object fails fast without
    /// asking a stream enumeration to reinterpret the durable recovery boundary.
    EXPECT_EQ(backend->refs_list_count.load(), 0) << "durable corruption must not trigger recovery LIST";
}

/// Test 14 (concurrent-waiter leg): while one caller is blocked in recovery's unlocked exact-log GET,
/// a second caller for the same table parks on `recovery_cv` and is woken exactly once when recovery
/// completes. Neither caller may race an independent stream LIST.
TEST(CASRecoveryStreaming, ConcurrentWaiterUnblockedOnce)
{
    auto backend = std::make_shared<BlockingFirstLogGetBackend>();
    seedPoolMetaForRestart(*backend);
    const Layout layout("p");
    const RootNamespace ns{"00/aa@cas@"};

    publishCommittedTransition(*backend, layout, ns, "x", std::nullopt, mref(1));
    publishCommittedTransition(*backend, layout, ns, "y", std::nullopt, mref(2));
    /// Semantic publication already durably advances the exact checkpoint frontier.

    auto store = openPoolForTest(backend);
    backend->refs_prefix = layout.namespaceStreamPrefix(fixture::fixtureLife(ns));

    /// Gate the first exact replay GET. The leader reaches it with `state_mutex` released, which is
    /// the window in which a second caller must be able to park on `recovery_cv`.
    std::atomic<bool> get_entered{false};
    std::promise<void> entered_promise;
    std::promise<void> release_promise;
    std::shared_future<void> release_future = release_promise.get_future().share();
    backend->target_log_key = layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 1});
    backend->on_first_target_get = [&]
    {
        if (!get_entered.exchange(true))
            entered_promise.set_value();
        release_future.wait();
    };
    backend->armed = true;

    std::thread t1([&] { store->listRefs(ns); });   /// exact GET blocks with recovery unlocked
    entered_promise.get_future().wait();

    std::thread t2([&] { store->listRefs(ns); });    /// second caller must park on recovery_cv
    const bool parked = pollUntil([&] { return store->refRecoveryWaitersForTest(ns) >= 1; });

    release_promise.set_value();
    t1.join();
    t2.join();

    EXPECT_TRUE(parked) << "the second caller must reach recovery_cv while the first is in the retry window";
    EXPECT_EQ(store->refRecoveryWaitersForTest(ns), 0u) << "no phantom waiter after recovery completes";
    EXPECT_TRUE(store->resolveRef(ns, "x").has_value());
    EXPECT_TRUE(store->resolveRef(ns, "y").has_value());
    EXPECT_EQ(backend->list_calls.load(), 0)
        << "the leader and parked waiter must both recover without stream enumeration";
}

/// Test 14 (other materializers leg): the orphan-sweep recovery (`recoverRefTableDetailedFromAuthority`)
/// and fsck's exact-authority recovery stream through the SAME builder and hold under the SAME
/// per-transaction bound as primary recovery.
TEST(CASRecoveryStreaming, OrphanSweepAndFsckSameBound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    seedPoolMetaForRestart(*backend);
    const Layout layout("p");
    const RootNamespace ns_sweep{"00/sweep@cas@"};
    const RootNamespace ns_fsck{"00/fsck@cas@"};

    constexpr size_t kTxns = 16;
    constexpr size_t kOpsPerTxn = 250;
    uint64_t manifest_seq = 1;
    const SeededTail sweep_tail = seedBigTail(*backend, layout, ns_sweep, kTxns, kOpsPerTxn, manifest_seq);
    const SeededTail fsck_tail = seedBigTail(*backend, layout, ns_fsck, kTxns, kOpsPerTxn, manifest_seq);

    const uint64_t max_single = std::max(sweep_tail.max_single_footprint, fsck_tail.max_single_footprint);
    const uint64_t bound = 2 * max_single;
    ASSERT_GT(sweep_tail.total_footprint, bound);
    ASSERT_GT(fsck_tail.total_footprint, bound);

    auto store = openPoolForTest(backend);

    {
        PeakTracker tracker;
        setRecoveryReplayMemoryProbeForTest(tracker.probe());
        SCOPE_EXIT({ setRecoveryReplayMemoryProbeForTest({}); });
        const CasRefCatalog::Snapshot sweep_catalog_cut = CasRefCatalog::read(*backend, layout);
        const RecoveredRefTable recovered =
            recoverRefTableDetailedAtCatalogCutForTest(*backend, layout, sweep_catalog_cut, ns_sweep);
        EXPECT_EQ(recovered.state.getPrecommits().size(), kTxns * kOpsPerTxn);
        EXPECT_LE(tracker.peak(), static_cast<int64_t>(bound))
            << "orphan-sweep recovery must stream: peak " << tracker.peak() << " B, bound " << bound << " B";
        EXPECT_GE(tracker.peak(), static_cast<int64_t>(sweep_tail.max_single_footprint))
            << "the probe must observe at least one decoded sweep transaction (peak " << tracker.peak()
            << " B) -- a zero peak means the orphan-sweep report call was silently removed";
        EXPECT_EQ(tracker.alive(), 0);
    }

    {
        PeakTracker tracker;
        setRecoveryReplayMemoryProbeForTest(tracker.probe());
        SCOPE_EXIT({ setRecoveryReplayMemoryProbeForTest({}); });
        const FsckReport report = runFsck(*store, /*detail=*/true);
        EXPECT_TRUE(report.clean());
        EXPECT_LE(tracker.peak(), static_cast<int64_t>(bound))
            << "fsck exact-authority recovery must stream: peak " << tracker.peak() << " B, bound " << bound << " B";
        EXPECT_GE(tracker.peak(), static_cast<int64_t>(fsck_tail.max_single_footprint))
            << "the probe must observe at least one decoded fsck-recovery transaction (peak " << tracker.peak()
            << " B) -- a zero peak means fsck stopped recovering catalog-authoritative namespaces";
        EXPECT_EQ(tracker.alive(), 0);
    }
}

/// Test 14 (writer-ledger leg -- the production recovery path): the writer ledger's OWN recovery loop
/// (`CasRefLedger::ensureRefTableRecovered`, reached through any Pool touch) must stream the tail under
/// the SAME per-transaction bound the free recovery does. This is the exact production path the original
/// memory finding named; `LongTailReplaysUnderMemoryBound` above exercises the free authoritative recovery,
/// NOT the ledger loop, so the ledger could regress to whole-tail materialisation while every other
/// bound stayed green. Recovery is driven through the production non-minting namespace-file read path,
/// which does NOT dispatch the stale-precommit sweep `listRefs` would (that sweep
/// would append removals over the seeded epoch-1 precommit bindings and perturb both the count and the
/// probe). The whole tail sits above a never-born base, so the retained tail count equals the whole tail.
TEST(CASRecoveryStreaming, LedgerRecoveryReplaysUnderMemoryBound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    seedPoolMetaForRestart(*backend);
    const Layout layout("p");
    const RootNamespace ns{"00/ledger@cas@"};

    constexpr size_t kTxns = 24;
    constexpr size_t kOpsPerTxn = 250;
    uint64_t manifest_seq = 1;
    const SeededTail seeded = seedBigTail(*backend, layout, ns, kTxns, kOpsPerTxn, manifest_seq);

    const uint64_t bound = 2 * seeded.max_single_footprint;
    ASSERT_GT(seeded.total_footprint, bound)
        << "fixture must make the whole tail (" << seeded.total_footprint
        << " B) provably exceed the bound (" << bound << " B)";

    auto store = openPoolForTest(backend);

    PeakTracker tracker;
    setRecoveryReplayMemoryProbeForTest(tracker.probe());
    SCOPE_EXIT({ setRecoveryReplayMemoryProbeForTest({}); });

    /// The production Task 4b reader drives `CasRefLedger::ensureRefTableRecovered` without the
    /// stale-precommit sweep; the resident-only observer then reads the retained tail count.
    ASSERT_TRUE(store->namespaceFilesLifeIfReadable(ns));
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), kTxns)
        << "the whole tail must have replayed through the ledger's own recovery loop";
    EXPECT_LE(tracker.peak(), static_cast<int64_t>(bound))
        << "ledger recovery must hold at most ~one decoded transaction (peak " << tracker.peak()
        << " B) not the whole " << kTxns << "-transaction tail (" << seeded.total_footprint << " B)";
    /// Lower bound (the accountant's fail-close): a zero peak means the ledger loop's production report
    /// call was removed and this bound would guard nothing -- the exact silent-decoupling this leg exists
    /// to catch on the production path.
    EXPECT_GE(tracker.peak(), static_cast<int64_t>(seeded.max_single_footprint))
        << "the probe must observe at least one whole decoded transaction resident (peak " << tracker.peak()
        << " B, one transaction " << seeded.max_single_footprint
        << " B) -- a zero peak means the ledger's production report call was removed and the bound guards nothing";
    EXPECT_EQ(tracker.alive(), 0) << "every decoded transaction must be discarded after it is applied";
}

/// Test 15 (publication inventory): after streaming recovery of a table with a non-trivial snapshot
/// base, precommit bindings, and a tail of committed transactions, EVERY field the
/// recovery publication seeds is asserted -- not just the two a prose inventory would keep. This is a
/// regression guard: streaming recovery must install exactly what the whole-tail recovery installed.
TEST(CASRecoveryStreaming, RecoveryResultInventoryComplete)
{
    auto backend = std::make_shared<CountingBackend>();
    seedPoolMetaForRestart(*backend);
    const Layout layout("p");
    const RootNamespace ns{"00/inv@cas@"};

    /// A non-trivial base snapshot: two committed rows plus a stale predecessor precommit binding.
    RefTableSnapshot base;
    base.ns = ns.string();
    base.snapshot_id = RefTxnId{1, 5};
    base.committed = {committedRow("c_one", mref(11)), committedRow("c_two", mref(12))};
    base.precommits = {RefOwnerBinding{RefOwnerKind::Precommit, "p_stale", mref(13)}};
    RefLogTxn base_txn;
    base_txn.ns = ns.string();
    base_txn.txn_id = base.snapshot_id;
    base_txn.ops = publishCommittedOps("c_two", mref(12));
    fixture::writeRefLogRaw(*backend, layout, base_txn);
    writeRefSnapshotRaw(*backend, layout, base);
    const auto base_got = backend->get(layout.refSnapshotKey(fixture::fixtureLife(ns), base.snapshot_id));
    ASSERT_TRUE(base_got.has_value());
    const uint64_t base_stored_bytes = base_got->bytes.size();

    /// Two committed transactions strictly above the base -- the tail.
    RefLogTxn t6;
    t6.ns = ns.string();
    t6.txn_id = RefTxnId{1, 6};
    t6.ops = publishCommittedOps("c_three", mref(21));
    fixture::writeRefLogRaw(*backend, layout, t6);
    RefLogTxn t7;
    t7.ns = ns.string();
    t7.txn_id = RefTxnId{1, 7};
    t7.ops = publishCommittedOps("c_four", mref(22));
    fixture::writeRefLogRaw(*backend, layout, t7);

    writeRecoverableCkptForRawFixture(
        *backend, layout, ns, RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 7},
                                       .checkpoint_snapshot_id = RefTxnId{1, 5},
                                       .last_epoch_seal = std::nullopt});

    const uint64_t tail6 = backend->get(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 6}))->bytes.size();
    const uint64_t tail7 = backend->get(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 7}))->bytes.size();

    backend->resetCounts();
    auto store = openPoolForTest(backend);

    /// Drive recovery via the production namespace-file reader WITHOUT the stale-precommit sweep that
    /// `resolveRef`/`listRefs` dispatch (that
    /// sweep would clear `needs_stale_precommit_sweep` before it could be observed). Every inventory
    /// field below is then read straight off the seeded runtime, and the read-path state assertion is
    /// left for LAST (after `needs_stale_precommit_sweep` has been observed).

    ASSERT_TRUE(store->namespaceFilesLifeIfReadable(ns));
    const NamespaceLifeId life = fixture::fixtureLife(ns);
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, base.snapshot_id)), 1u)
        << "recovery must validate the selected base's matching ordinary log";
    EXPECT_EQ(backend->getCount(layout.refSnapshotKey(life, base.snapshot_id)), 1u)
        << "the inventory must come from the selected snapshot, not a pre-snapshot failure";

    /// newest snapshot identity: the recovered base id, no seal on this clean mount.
    EXPECT_EQ(store->newestPublishedSnapshotIdForTest(ns), std::optional<RefTxnId>(base.snapshot_id));

    /// last_epoch_seal: this mount's live epoch is the one the seeded stream was written in, so the
    /// CAS-walk crossed no epoch transition and installed no chain link. Pins that the field IS part of
    /// the published inventory (its non-empty counterpart lives in
    /// `CASRefRecoveryCasWalk.DeadEpochIsClosedByOurOwnSealAtTPlusOne`).
    EXPECT_EQ(store->lastEpochSealForTest(ns), std::nullopt);

    /// stale-precommit sweep: recovery always arms it (asserted BEFORE any read-side sweep runs).
    EXPECT_TRUE(store->needsStalePrecommitSweepForTest(ns));

    /// tail count / bytes: exactly the two transactions above the base and their stored sizes.
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), 2u);
    EXPECT_EQ(store->refTailBytesSinceSnapshotForTest(ns), tail6 + tail7);

    /// base snapshot bytes: the encoded body size of the recovered base snapshot.
    EXPECT_EQ(store->refBaseSnapshotBytesForTest(ns), base_stored_bytes);

    /// admission budgets: the raw hard limits minus this table's wire overhead and the safety margin.
    const uint64_t overhead = 4 + ns.string().size() + 4096;
    const uint64_t expected_budget = 64ULL * 1024 * 1024 - overhead;
    EXPECT_EQ(store->refSnapshotBudgetForTest(ns), expected_budget);
    EXPECT_EQ(store->refRemovalBudgetForTest(ns), expected_budget);

    /// state: four committed rows (two from the base, two from the tail). This read dispatches the
    /// read-side sweep, hence it comes last -- after the sweep flag has been observed above.
    const auto refs = store->listRefs(ns);
    EXPECT_EQ(refs.size(), 4u);
    EXPECT_TRUE(store->resolveRef(ns, "c_one").has_value());
    EXPECT_TRUE(store->resolveRef(ns, "c_two").has_value());
    EXPECT_TRUE(store->resolveRef(ns, "c_three").has_value());
    EXPECT_TRUE(store->resolveRef(ns, "c_four").has_value());
}
