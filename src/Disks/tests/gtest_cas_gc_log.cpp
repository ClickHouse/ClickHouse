#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/tests/cas_test_helpers.h>

#include <Common/Exception.h>

#include <string>
#include <vector>

/// Unit coverage for the CA GC scheduler's logging sink (the source of
/// `system.cas_gc_log`). The scheduler emits a Start + Finish
/// `GcRoundLogRecord` per round through the injected `GcRoundLogger`; here we capture the records in
/// a vector and assert their shape over a real (in-memory) Pool driven through a dropped-then-
/// collectable object — the same Pool/Backend fixture the B140 reclaim test uses.
///
/// NOTE on ProfileEvents: `runOneRoundNow` runs on THIS (bare gtest) thread, which has no attached
/// `ThreadStatus`, so the scheduler's `CurrentThread::isInitialized()` guard skips per-round
/// ProfileEvents capture. The `profile_events` map is therefore EXPECTED to be empty here and this
/// test does NOT assert it non-empty (the on-server paths are attached; the functional/soak coverage
/// asserts non-empty there).

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;
using Rec = DB::Cas::GcRoundLogRecord;

namespace
{

/// Publish one part `ref` with a single content blob whose payload is `payload`. Returns the manifest id.
ManifestId publishPart(const PoolPtr & s, const String & ns, const String & ref, const String & payload)
{
    const RootNamespace nsr{ns};
    PartWriteInfo info;
    info.intended_ref = ns + "/" + ref;
    auto build = s->beginPartWrite(info);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));

    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();

    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(nsr, ref, id);
    build->promote(nsr, ref, build->buildId(), id);
    return id;
}

}

namespace
{
/// One round emits a Start, then one Phase row per GC phase it reached, then a Finish. Tests that care
/// only about the round-outcome rows filter the phase rows out through this.
std::vector<Rec> roundRowsOnly(const std::vector<Rec> & rows)
{
    std::vector<Rec> out;
    for (const Rec & r : rows)
        if (r.event_type != Rec::EventType::Phase)
            out.push_back(r);
    return out;
}
}

/// The happy path: a marking round (candidates_marked > 0). Each `runOneRoundNow` must emit exactly one
/// Start, then its phase rows, then one Finish, with `disk_name`/`gc_id` set and `duration_ms`
/// populated on the Finish.
///
/// It drives the PRODUCTION scheduler, so it covers both halves of the pipeline: a MARKING round
/// (candidates condemned, nothing deleted) and, some rounds later once the mount's ack floor graduates
/// them, a DELETING round whose Finish carries the count through. The ordering is asserted, because a
/// deletion reported before its marking would mean the row is not describing the round it names.
TEST(CASGCLog, EmitsStartFinishWithCounts)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// gc_fold_max_defer_rounds=0: this test drives up to 16 consecutive rounds through the scheduler
    /// (no direct Gc handle to override per-instance) expecting each to fold; force fold-every-round
    /// (Phase-4 Lever A would otherwise defer once the pool quiesces, stalling the mark-then-delete
    /// pipeline within the round budget).
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"srv1/tbl"};

    /// Publish a part, then drop it so its blob/tree become collectable.
    publishPart(store, ns.string(), "all_0_0_0", "hello-cas-gc-log");
    store->dropRef(ns, "all_0_0_0");
    /// Advance the durable watermark floor past the build's seq so the build-watermark guard no
    /// longer spares the now-dropped objects (the background renewer is off in this test).
    store->renewWatermarkOnce();

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        [&](const Rec & r) { rows.push_back(r); });

    /// Drive rounds until we observe both a marking round and a deletion round. Under the ack-floor
    /// pipeline a candidate is marked (condemned) in one round and physically deleted a few rounds later,
    /// once the mount's ack floor graduates it — so advance the store's own mount ack after each round
    /// (renewWatermarkOnce runs the beat) and give the pipeline a generous round budget. Each
    /// runOneRoundNow call appends a Start, the round's phase rows, and a Finish.
    bool saw_marked = false;
    bool saw_deleted = false;
    size_t marking_finish_idx = 0;
    size_t deleting_finish_idx = 0;
    uint64_t total_deleted = 0;
    constexpr size_t max_rounds = 16;
    for (size_t round = 0; round < max_rounds; ++round)
    {
        const size_t before = rows.size();
        sched.runOneRoundNow(Rec::Trigger::Manual);
        store->renewWatermarkOnce();

        /// Each call emits exactly one Start (first) and one Finish (last), with the round's phase rows
        /// in between.
        ASSERT_GE(rows.size(), before + 2u) << "each round must emit at least a Start and a Finish";
        ASSERT_EQ(rows[before].event_type, Rec::EventType::Start);
        ASSERT_EQ(rows.back().event_type, Rec::EventType::Finish);
        for (size_t i = before + 1; i + 1 < rows.size(); ++i)
            ASSERT_EQ(rows[i].event_type, Rec::EventType::Phase)
                << "only Phase rows may sit between a round's Start and Finish";

        const size_t finish_idx = rows.size() - 1;
        const Rec & fin = rows[finish_idx];
        if (!saw_marked && fin.candidates_marked > 0)
        {
            saw_marked = true;
            marking_finish_idx = finish_idx;
        }
        if (!saw_deleted && fin.objects_deleted > 0)
        {
            saw_deleted = true;
            deleting_finish_idx = finish_idx;
        }
        total_deleted += fin.objects_deleted;
    }

    ASSERT_TRUE(saw_marked) << "expected a round that marked at least one candidate";
    EXPECT_GT(rows[marking_finish_idx].candidates_marked, 0u);
    EXPECT_GT(rows[marking_finish_idx].entries_condemned, 0u);
    ASSERT_TRUE(saw_deleted) << "expected a round that physically deleted at least one object";
    EXPECT_GE(deleting_finish_idx, marking_finish_idx)
        << "an object cannot be reported deleted before the round that condemned it";
    EXPECT_GT(total_deleted, 0u)
        << "the deleted count must reach the Finish row, not stop inside the round";

    /// Identity + timing fields are set on every record.
    for (const Rec & r : rows)
    {
        EXPECT_EQ(r.disk_name, "ca");
        EXPECT_FALSE(r.gc_id.empty());
        EXPECT_EQ(r.trigger, Rec::Trigger::Manual);
    }
    /// The round-outcome rows alternate Start, Finish, Start, Finish, ... once the phase rows are
    /// filtered out; `duration_ms` is meaningful on each Finish (populated unconditionally there).
    const std::vector<Rec> round_rows = roundRowsOnly(rows);
    ASSERT_EQ(round_rows.size() % 2, 0u);
    for (size_t i = 0; i < round_rows.size(); ++i)
        EXPECT_EQ(round_rows[i].event_type,
                  i % 2 == 0 ? Rec::EventType::Start : Rec::EventType::Finish);
}

namespace
{

/// A backend that throws on `list`, the first thing the GC round does (namespace discovery via the
/// roots registry / listing). Used to drive the Aborted-Finish path: the round throws, the scheduler
/// emits an Aborted Finish with the exception text, and `runOneRoundNow` rethrows.
class ThrowingBackend : public InMemoryBackend
{
public:
    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        if (arm)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "injected backend list failure");
        return InMemoryBackend::list(prefix, cursor, limit);
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        if (arm)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "injected backend get failure");
        return InMemoryBackend::get(key, range);
    }

    HeadResult head(const String & key) override
    {
        if (arm)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "injected backend head failure");
        return InMemoryBackend::head(key);
    }

    /// Armed only after Pool::open, so opening (which reads/initialises gc state) succeeds.
    std::atomic<bool> arm{false};
};

}

/// A7-HIGH-fix: the manual `SYSTEM ... GC` path (runOneRoundNow) reuses ONE stable Gc instance across
/// calls (A7 — the lease's observation-window steal protocol compares consecutive observations of the
/// SAME observer), but it must be OBSERVE-ONLY with respect to STEALING: the protocol's safety argument
/// requires the two observations that flag an incumbent "frozen" to be spaced by real wall time (>= the
/// heartbeat cadence H) so a live incumbent gets a chance to pulse in between — a guarantee only the
/// background loop's own interval-paced ticks provide. Two manual calls have no such guarantee (they
/// can land microseconds apart in a real query), so a manual round must NEVER execute the steal CAS,
/// no matter how many times it re-observes the same frozen tuple. Dead-incumbent recovery stays the
/// loop's job (bounded ~2*interval; covered by the CASGCLease loop-driven steal tests in
/// gtest_cas_gc_round.cpp, e.g. StealAfterObservedNonRenewalBumpsEpoch / FailoverStealOnceHeartbeatStops).
/// Deterministic: "time" is the order of runRegularRound calls; no sleep, no clock, no threads.
TEST(CASGCSchedulerSteal, ManualRoundNeverStealsEvenADeadIncumbent)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    /// A foreign incumbent takes the lease and then DIES (never renews, never heartbeats).
    const UInt128 kIncumbent = hexToU128("00000000000000000000000000000abc");
    Gc incumbent(store, kIncumbent);
    ASSERT_TRUE(incumbent.runRegularRound().acquired_lease);

    DB::Cas::CasGcScheduler sched(store, std::chrono::seconds(1), "test::gc", "ca");

    /// obs #1: records the incumbent's (owner, seq, hb=absent).
    EXPECT_FALSE(sched.runOneRoundNow(Rec::Trigger::Manual).acquired_lease);
    /// obs #2 and #3: the same frozen (owner, seq, hb) observed repeatedly would be steal-eligible on
    /// the loop path (see the Core-level test this mirrors), but the manual path keeps backing off.
    EXPECT_FALSE(sched.runOneRoundNow(Rec::Trigger::Manual).acquired_lease);
    EXPECT_FALSE(sched.runOneRoundNow(Rec::Trigger::Manual).acquired_lease);
}

/// Negative-control companion to the test above (reviewer-requested): with the incumbent visibly alive
/// (its heartbeat advancing between the manual round's observations, exactly like
/// CASGCLease.HeartbeatBlocksFalseStealOfAliveLeader at the Core level), the manual round must still
/// correctly back off — confirming the new observe-only branch didn't regress the PRE-EXISTING
/// incumbent_renewed/hb_alive liveness detection (this test would already pass on the protocol's own
/// terms even without the A7-HIGH-fix; it pins that the fix didn't break it).
TEST(CASGCSchedulerSteal, ManualRoundNeverStealsALiveHeartbeatingIncumbent)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    const UInt128 kIncumbent = hexToU128("00000000000000000000000000000abc");
    Gc incumbent(store, kIncumbent);
    ASSERT_TRUE(incumbent.runRegularRound().acquired_lease);

    DB::Cas::CasGcScheduler sched(store, std::chrono::seconds(1), "test::gc", "ca");

    /// obs #1: records (owner=incumbent, seq, hb=absent).
    EXPECT_FALSE(sched.runOneRoundNow(Rec::Trigger::Manual).acquired_lease);
    Gc::pulseHeartbeat(*store, kIncumbent);   /// the incumbent is alive and pulsing (hb 0->1)
    /// obs #2: hb advanced since obs #1 => alive => no steal (never reaches the observe-only branch).
    EXPECT_FALSE(sched.runOneRoundNow(Rec::Trigger::Manual).acquired_lease);
    Gc::pulseHeartbeat(*store, kIncumbent);   /// hb 1->2
    EXPECT_FALSE(sched.runOneRoundNow(Rec::Trigger::Manual).acquired_lease);
}

/// A round whose backend throws must produce a Finish with `outcome == Aborted` and a non-empty
/// `error`, and `runOneRoundNow` must rethrow the exception (the round failure is observable, not
/// swallowed — the logging sink itself is best-effort, but the round error propagates).
TEST(CASGCLog, AbortedFinishOnThrowingRound)
{
    auto backend = std::make_shared<ThrowingBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        [&](const Rec & r) { rows.push_back(r); });

    backend->arm.store(true);

    EXPECT_THROW(sched.runOneRoundNow(Rec::Trigger::Manual), DB::Exception);

    /// A throwing round still emits a Start and an Aborted Finish. It also emits the phase row of the
    /// phase it died in -- the timer is RAII, so it fires during unwinding, which is exactly the forensic
    /// record a failed round needs. That is also why `round_id`, not `round`, is the correlator: this
    /// round has no round number at all.
    const std::vector<Rec> round_rows = roundRowsOnly(rows);
    ASSERT_EQ(round_rows.size(), 2u) << "a throwing round still emits a Start and a (Aborted) Finish";
    EXPECT_EQ(round_rows[0].event_type, Rec::EventType::Start);
    EXPECT_EQ(round_rows[1].event_type, Rec::EventType::Finish);
    EXPECT_EQ(round_rows[1].outcome, Rec::Outcome::Failed);
    EXPECT_FALSE(round_rows[1].error.empty()) << "a failed Finish must carry the exception text";
    EXPECT_EQ(round_rows[1].disk_name, "ca");
    EXPECT_FALSE(round_rows[1].gc_id.empty());
    EXPECT_FALSE(round_rows[1].round_id.empty());
    for (const Rec & r : rows)
        EXPECT_EQ(r.round_id, round_rows[0].round_id)
            << "every row of a FAILED round must still correlate through round_id";
}

/// Every row of one round -- its Start, each of its Phase rows, and its Finish -- carries the SAME
/// non-empty `round_id`, and two rounds carry DIFFERENT ones. That is the property the column exists
/// for: `round` is 0 on Start, is only known after the round's single `gc/state` CAS, and is absent on a
/// round that never led, so it cannot serve as the correlator.
TEST(CASGCLog, EveryRowOfARoundSharesOneRoundId)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"srv1/tbl"};
    publishPart(store, ns.string(), "all_0_0_0", "hello-round-id");
    store->dropRef(ns, "all_0_0_0");
    store->renewWatermarkOnce();

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        [&](const Rec & r) { rows.push_back(r); });

    sched.runOneRoundNow(Rec::Trigger::Manual);
    const size_t after_first = rows.size();
    ASSERT_GE(after_first, 2u);
    const String first_id = rows.front().round_id;
    EXPECT_FALSE(first_id.empty());
    for (size_t i = 0; i < after_first; ++i)
        EXPECT_EQ(rows[i].round_id, first_id) << "row " << i << " of the first round has a different round_id";

    store->renewWatermarkOnce();
    sched.runOneRoundNow(Rec::Trigger::Manual);
    ASSERT_GT(rows.size(), after_first);
    const String second_id = rows[after_first].round_id;
    EXPECT_FALSE(second_id.empty());
    EXPECT_NE(second_id, first_id) << "two rounds must not share a round_id";
    for (size_t i = after_first; i < rows.size(); ++i)
        EXPECT_EQ(rows[i].round_id, second_id);
}

namespace
{
/// The phase names of one round, in emission order.
std::vector<String> phaseNames(const std::vector<Rec> & rows, size_t from)
{
    std::vector<String> out;
    for (size_t i = from; i < rows.size(); ++i)
        if (rows[i].event_type == Rec::EventType::Phase)
            out.push_back(rows[i].phase);
    return out;
}

/// The `phase_metrics` of the named phase of one round. Fails the caller's expectation if absent.
std::map<String, UInt64> metricsOf(const std::vector<Rec> & rows, size_t from, const String & phase)
{
    for (size_t i = from; i < rows.size(); ++i)
        if (rows[i].event_type == Rec::EventType::Phase && rows[i].phase == phase)
            return rows[i].phase_metrics;
    return {};
}
}

/// A FOLDING round emits every phase, in execution order, and each phase's row carries the semantic
/// counts only that phase can compute. This is the test that would catch an instrumentation site
/// silently dropping out of the round -- a phase that stops emitting reads exactly like a phase that
/// costs nothing, which is the failure mode this whole change exists to prevent.
///
/// ProfileEvents are deliberately NOT asserted: `runOneRoundNow` runs on the bare gtest thread, which
/// has no attached `ThreadStatus`, so per-phase capture degrades to an empty map exactly as the
/// round-level capture already does (see the note at the top of this file).
TEST(CASGCLog, FoldingRoundEmitsEveryPhaseInOrder)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"srv1/tbl"};
    publishPart(store, ns.string(), "all_0_0_0", "hello-cas-gc-phases");
    store->dropRef(ns, "all_0_0_0");
    store->renewWatermarkOnce();

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        [&](const Rec & r) { rows.push_back(r); });

    ASSERT_TRUE(sched.runOneRoundNow(Rec::Trigger::Manual).acquired_lease);

    const std::vector<String> expected = {
        "lease", "pre_fold_ref_drain", "heartbeat_floor", "defer_decision", "parent_seal_read",
        "fold_ref_group", "fold_seal_read", "fold_ref_intake",
        "fold_reduce", "fold_seal_write",
        "pending_deletes", "meta_pool_wait", "round_commit", "handoff_reclaim",
        "manifest_deletes", "namespace_cleanup", "ref_object_cleanup", "orphan_sweep"};
    EXPECT_EQ(phaseNames(rows, 0), expected);

    /// Every phase row is a Phase row of THIS round and carries a duration field (0 is a legitimate
    /// microsecond reading for a phase that did nothing, so only the shape is asserted).
    for (const Rec & r : rows)
        if (r.event_type == Rec::EventType::Phase)
        {
            EXPECT_EQ(r.round_id, rows.front().round_id);
            EXPECT_FALSE(r.phase.empty());
            EXPECT_TRUE(r.error.empty());
        }

    /// The defer decision reports the signal it decided on, and the two fold-seal reads it paid for.
    const auto defer = metricsOf(rows, 0, "defer_decision");
    EXPECT_EQ(defer.at("deferred"), 0u) << "this round folded, so it cannot report itself deferred";
    EXPECT_EQ(defer.at("fold_seal_reads"), 2u);
    EXPECT_GT(defer.at("namespaces_seen"), 0u);

    const auto ref_group = metricsOf(rows, 0, "fold_ref_group");
    EXPECT_EQ(ref_group.at("ref_folding_aborted"), 0u);
    EXPECT_GT(ref_group.at("ref_keys_listed"), 0u);

    /// Probe B1's identity, as an OBSERVABLE property of the table rather than an assumption in a
    /// comment: the round sealed coverage over exactly the logs it folded.
    const auto intake = metricsOf(rows, 0, "fold_ref_intake");
    EXPECT_EQ(intake.at("logs_accounted"), intake.at("logs_applied"));
    EXPECT_GT(intake.at("logs_applied"), 0u);
    EXPECT_GT(intake.at("deltas_emitted"), 0u);

    /// Probe B2's verdict. Nonzero would have thrown, so the row can only ever read 0 on a round that
    /// reached its Finish -- which is the point: the column is the round's own attestation.
    EXPECT_EQ(metricsOf(rows, 0, "fold_reduce").at("transactions_unapplied"), 0u);

    /// The honest gap: the meta pool's work runs on other threads, so this row's ProfileEvents delta is
    /// empty by construction and these two counts are its ONLY signal. They must be real numbers.
    const auto meta = metricsOf(rows, 0, "meta_pool_wait");
    EXPECT_GT(meta.at("jobs_scheduled"), 0u) << "this round condemns, so it schedules condemn-marker writes";
    EXPECT_EQ(meta.at("jobs_completed"), meta.at("jobs_scheduled"))
        << "every scheduled job must have finished by the time the wait returns";
}

/// A round that never leads emits ONLY the phase it reached. `round` does not exist for such a round,
/// so `round_id` is the only thing tying its rows together -- which is why it is the correlator.
TEST(CASGCLog, NotALeaderRoundEmitsOnlyTheLeasePhase)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    /// A foreign incumbent holds the lease, so the scheduler's round backs off immediately.
    Gc incumbent(store, hexToU128("00000000000000000000000000000abc"));
    ASSERT_TRUE(incumbent.runRegularRound().acquired_lease);

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        [&](const Rec & r) { rows.push_back(r); });
    EXPECT_FALSE(sched.runOneRoundNow(Rec::Trigger::Manual).acquired_lease);

    EXPECT_EQ(phaseNames(rows, 0), (std::vector<String>{"lease"}));
    EXPECT_EQ(metricsOf(rows, 0, "lease").at("acquired"), 0u);
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(rows.back().outcome, Rec::Outcome::NotALeader);
    for (const Rec & r : rows)
        EXPECT_EQ(r.round_id, rows.front().round_id);
}

/// B3: the scheduler exposes per-disk GC health for system.cas_mounts (the process-
/// global CurrentMetrics gauges were clobbered with >= 2 CAS disks). Drive one leader round and
/// assert the health snapshot reflects leadership, the pending-reclaim backlog and a fresh success.
TEST(CASGCHealth, ReflectsLeadershipAndPendingReclaim)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"srv1/tbl"};
    publishPart(store, ns.string(), "all_0_0_0", "hello-cas-gc-health");
    store->dropRef(ns, "all_0_0_0");
    store->renewWatermarkOnce();

    DB::Cas::CasGcScheduler sched(store, std::chrono::seconds(1), "test::gc", "ca", {});

    const auto h0 = sched.gcHealth();
    EXPECT_FALSE(h0.is_leader);
    EXPECT_FALSE(h0.ever_succeeded);
    EXPECT_EQ(h0.pending_reclaim, 0);
    EXPECT_EQ(h0.wedged_namespace_count, 0u);

    const RoundReport rep = sched.runOneRoundNow(Rec::Trigger::Manual);
    ASSERT_TRUE(rep.acquired_lease);

    const auto h1 = sched.gcHealth();
    EXPECT_TRUE(h1.is_leader);
    EXPECT_TRUE(h1.ever_succeeded);
    EXPECT_EQ(h1.pending_reclaim,
              static_cast<Int64>(rep.condemned) - static_cast<Int64>(rep.redeleted));
    EXPECT_EQ(h1.wedged_namespace_count, 0u);
    EXPECT_LT(h1.last_success_age_seconds, 60u);
}
