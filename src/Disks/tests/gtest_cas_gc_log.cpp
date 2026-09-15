#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/tests/cas_test_helpers.h>

#include <Common/Exception.h>

#include <condition_variable>
#include <functional>
#include <thread>
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
    extern const int CORRUPTED_DATA;
    extern const int NETWORK_ERROR;
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

    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();

    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(nsr, ref, id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
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
    /// Unhide the names the primitive overrides below would otherwise shadow.
    using InMemoryBackend::head;
    using InMemoryBackend::list;

    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        if (arm)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "injected backend list failure");
        return InMemoryBackend::list(prefix, cursor, limit, access);
    }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (arm)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "injected backend get failure");
        return InMemoryBackend::read(key, access);
    }

    std::optional<RawMeta> head(const String & key, TransportAccess & access) override
    {
        if (arm)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "injected backend head failure");
        return InMemoryBackend::head(key, access);
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
    EXPECT_EQ(round_rows[1].outcome, Rec::Outcome::Failed)
        << "BAD_ARGUMENTS is not on the transient list, so the row must read as a real failure";
    EXPECT_EQ(round_rows[1].error_code, DB::ErrorCodes::BAD_ARGUMENTS)
        << "the Finish row must carry the structured exception code, not only the message text";
    EXPECT_FALSE(round_rows[1].error.empty()) << "a failed Finish must carry the exception text";
    EXPECT_EQ(round_rows[1].disk_name, "ca");
    EXPECT_FALSE(round_rows[1].gc_id.empty());
    EXPECT_FALSE(round_rows[1].round_id.empty());
    for (const Rec & r : rows)
        EXPECT_EQ(r.round_id, round_rows[0].round_id)
            << "every row of a FAILED round must still correlate through round_id";
}

/// A round that dies with a TRANSIENT code -- the backend was unreachable, timed out, or another
/// actor moved shared state -- must be classified `Aborted`, not `Failed`: the next scheduled round
/// is the retry and nothing durable is wrong. The classifier keys on the exception CODE
/// (`isTransientGcRoundError`), never on message wording.
class NetworkThrowingBackend : public InMemoryBackend
{
public:
    /// Unhide the names the primitive overrides below would otherwise shadow.
    using InMemoryBackend::list;

    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        if (arm)
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected backend outage");
        return InMemoryBackend::list(prefix, cursor, limit, access);
    }
    std::atomic<bool> arm{false};
};

TEST(CASGCLog, TransientThrowIsClassifiedAborted)
{
    auto backend = std::make_shared<NetworkThrowingBackend>();
    /// A PERSISTENT transient fault is reissued for the whole retry window, so the window has to run
    /// on a clock this test advances -- otherwise one read spends ninety real seconds. Heap-owned, not
    /// a plain local: the Pool can outlive this stack frame (a background publish holds
    /// `shared_from_this()`), so a by-reference capture of a local -- even an already-atomic one --
    /// would dangle once the frame returns.
    auto engine_now_ms = std::make_shared<std::atomic<uint64_t>>(0);
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    store->setCasRequestNowFnForTest([engine_now_ms] { return engine_now_ms->fetch_add(10'000) + 10'000; });
    store->setCasRetrySleepForTest([](uint64_t) {});

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        [&](const Rec & r) { rows.push_back(r); });

    backend->arm.store(true);
    EXPECT_THROW(sched.runOneRoundNow(Rec::Trigger::Manual), DB::Exception);

    const std::vector<Rec> round_rows = roundRowsOnly(rows);
    ASSERT_EQ(round_rows.size(), 2u);
    EXPECT_EQ(round_rows[1].event_type, Rec::EventType::Finish);
    EXPECT_EQ(round_rows[1].outcome, Rec::Outcome::Aborted)
        << "NETWORK_ERROR names a transient condition; the row must not read as a GC defect";
    EXPECT_EQ(round_rows[1].error_code, DB::ErrorCodes::NETWORK_ERROR);
    EXPECT_FALSE(round_rows[1].error.empty());
}

/// The classifier itself, pinned direct: the transient list is exact and everything else fails closed.
TEST(CASGCLog, TransientErrorClassifierFailsClosed)
{
    EXPECT_TRUE(DB::Cas::isTransientGcRoundError(DB::ErrorCodes::NETWORK_ERROR));
    EXPECT_FALSE(DB::Cas::isTransientGcRoundError(DB::ErrorCodes::BAD_ARGUMENTS));
    EXPECT_FALSE(DB::Cas::isTransientGcRoundError(0));
    EXPECT_FALSE(DB::Cas::isTransientGcRoundError(-1));
}

/// A backend that REFUSES the round-closing `gc/state` write -- the one that advances
/// `snap_generation` -- and lets every other write through, the lease acquire/renew included. The round
/// therefore does all of its pre-CAS work, condemning the dropped part included, and dies at
/// `round_commit`.
///
/// A refusal and not a throw, for two reasons. A thrown transport error is an ambiguity the engine
/// settles by an exact read and, while the precondition it named is unmoved, reissues to the policy
/// deadline -- so an armed fault would spend the whole retry window and end as a transport give-up. And
/// the arm is keyed on the generation rather than on a call count, because the acquire on a fresh pool
/// is an UNCONDITIONAL create that no count of conditional writes can see.
class StateCommitRefusingBackend : public InMemoryBackend
{
public:
    std::expected<String, RawConflict> write(
        const String & key, const String & bytes, const std::optional<String> & expected_value,
        TransportAccess & access) override
    {
        if (arm.load() && expected_value && key.ends_with("gc/state"))
        {
            const auto stored = InMemoryBackend::read(key, access);
            if (stored
                && decodeGcState(bytes).snap_generation > decodeGcState(stored->bytes).snap_generation)
            {
                arm.store(false);
                /// A store refuses a precondition only when the object moved, so move it: the same
                /// bytes under a fresh incarnation is the smallest faithful move, and it leaves the
                /// content alone so the assertions below stay about this round.
                (void)InMemoryBackend::write(key, stored->bytes, stored->value, access);
                return std::unexpected(RawConflict{});
            }
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
    std::atomic<bool> arm{false};
};

/// The Finish row of a THROWING round must still carry the counters of everything the round did
/// before it died. Before this existed, the exception path emitted a row with `round = 0` and every
/// counter zero, so a round that condemned entries and then lost its commit CAS was
/// indistinguishable from a round that never got past the lease.
TEST(CASGCLog, AbortedFinishCarriesProgressiveCounters)
{
    auto backend = std::make_shared<StateCommitRefusingBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"srv1/tbl"};

    publishPart(store, ns.string(), "all_0_0_0", "hello-progressive-counters");
    store->dropRef(ns, "all_0_0_0");
    store->renewWatermarkOnce();

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        [&](const Rec & r) { rows.push_back(r); });

    backend->arm.store(true);
    EXPECT_THROW(sched.runOneRoundNow(Rec::Trigger::Manual), DB::Exception);

    const std::vector<Rec> round_rows = roundRowsOnly(rows);
    ASSERT_EQ(round_rows.size(), 2u);
    const Rec & fin = round_rows[1];
    EXPECT_EQ(fin.outcome, Rec::Outcome::Aborted);
    /// A refused precondition is settled by one exact read and reported as a conflict, so the round
    /// is dropped whole and names the conflict rather than a transport error.
    EXPECT_EQ(fin.error_code, DB::ErrorCodes::ABORTED);
    EXPECT_EQ(fin.round, 0u) << "the commit never landed, so the round number must stay unstamped";
    EXPECT_GT(fin.candidates_marked + fin.entries_condemned + fin.entries_graduated
              + fin.entries_redeleted + fin.objects_deleted + fin.fence_outs, 0u)
        << "the pre-CAS work the round performed must survive into its failure row";
}

/// The pacing loop drops leadership only on a NON-transient round failure. A transient failure
/// (backend outage class) keeps `i_am_leader` set, so the advisory heartbeat keeps pulsing and a
/// live leader blocked on a flaky store is not deposed -- dropping the flag on every failure was
/// half of the dead-leader signature (`!incumbent_renewed && !hb_alive`) and produced leadership
/// ping-pong under backend fault windows. A non-transient failure must still clear the flag: a
/// logic-broken leader has to stay depositable.
class ModalThrowingBackend : public InMemoryBackend
{
public:
    /// Unhide the names the primitive overrides below would otherwise shadow.
    using InMemoryBackend::list;

    enum Mode : int { Off = 0, Transient = 1, Logic = 2 };
    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        const int m = mode.load();
        if (m == Transient)
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected backend outage");
        if (m == Logic)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "injected logic failure");
        return InMemoryBackend::list(prefix, cursor, limit, access);
    }
    std::expected<String, RawConflict> write(
        const String & key, const String & bytes, const std::optional<String> & expected_value,
        TransportAccess & access) override
    {
        if (key.ends_with("gc/hb"))
            ++hb_puts;
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
    std::atomic<int> mode{Off};
    std::atomic<uint64_t> hb_puts{0};
};

TEST(CASGCScheduler, TransientRoundFailureKeepsLeadershipAndHeartbeat)
{
    auto backend = std::make_shared<ModalThrowingBackend>();
    /// See `TransientThrowIsClassifiedAborted`: the transient mode is persistent while it is armed, so
    /// the retry window runs on a clock this test advances. Heap-owned, not a plain local: the Pool can
    /// outlive this stack frame (a background publish holds `shared_from_this()`), so a by-reference
    /// capture of a local -- even an already-atomic one -- would dangle once the frame returns.
    auto engine_now_ms = std::make_shared<std::atomic<uint64_t>>(0);
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    store->setCasRequestNowFnForTest([engine_now_ms] { return engine_now_ms->fetch_add(10'000) + 10'000; });
    store->setCasRetrySleepForTest([](uint64_t) {});

    std::mutex rows_mutex;
    std::condition_variable rows_cv;
    std::vector<Rec> finishes;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        [&](const Rec & r)
        {
            if (r.event_type != Rec::EventType::Finish)
                return;
            std::lock_guard g(rows_mutex);
            finishes.push_back(r);
            rows_cv.notify_all();
        });

    const auto wait_for_finish = [&](size_t count) -> Rec
    {
        std::unique_lock lock(rows_mutex);
        const bool ok = rows_cv.wait_for(lock, std::chrono::seconds(30), [&] { return finishes.size() >= count; });
        EXPECT_TRUE(ok) << "timed out waiting for Finish row #" << count;
        return finishes.at(count - 1);
    };
    /// Bounded poll for an ASYNC flag change. The loop stores `i_am_leader` after `runRoundLogged`
    /// returns (after the Finish row was emitted), so the row alone is not a happens-before for the
    /// flag -- poll to the expected value instead of asserting a racy instantaneous read.
    const auto poll_leader = [&](bool expected) -> bool
    {
        for (int i = 0; i < 3000; ++i)
        {
            if (sched.gcHealth().is_leader == expected)
                return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return sched.gcHealth().is_leader == expected;
    };

    sched.start();
    sched.requestRoundSoon();
    const Rec first = wait_for_finish(1);
    EXPECT_TRUE(first.outcome == Rec::Outcome::Success || first.outcome == Rec::Outcome::Deferred)
        << "outcome=" << static_cast<int>(first.outcome);
    EXPECT_TRUE(poll_leader(true)) << "a successful round must establish leadership";

    backend->mode.store(ModalThrowingBackend::Transient);
    sched.requestRoundSoon();
    const Rec aborted = wait_for_finish(2);
    EXPECT_EQ(aborted.outcome, Rec::Outcome::Aborted);
    /// Leadership kept => the advisory heartbeat keeps pulsing. Waiting for a NEW pulse after the
    /// failed round is the happens-after proof that the flag survived; with the flag dropped the
    /// heartbeat loop skips every pulse until the next successful round, and this wait times out.
    const uint64_t hb_before = backend->hb_puts.load();
    bool pulsed = false;
    for (int i = 0; i < 3000 && !pulsed; ++i)
    {
        pulsed = backend->hb_puts.load() > hb_before;
        if (!pulsed)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(pulsed) << "a transient round failure must not silence the advisory heartbeat";
    EXPECT_TRUE(sched.gcHealth().is_leader) << "a transient round failure must not drop leadership";

    backend->mode.store(ModalThrowingBackend::Logic);
    sched.requestRoundSoon();
    const Rec failed = wait_for_finish(3);
    EXPECT_EQ(failed.outcome, Rec::Outcome::Failed);
    EXPECT_TRUE(poll_leader(false)) << "a non-transient round failure must still surrender leadership";

    backend->mode.store(ModalThrowingBackend::Off);
    sched.stop();
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

namespace
{

/// A backend that arms the pool's teardown the moment a chosen key has been read -- after the read
/// returned, before the round can act on it -- so the arm lands mid-round at a known point.
class ArmAfterReadBackend : public InMemoryBackend
{
public:
    using InMemoryBackend::read;

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto result = InMemoryBackend::read(key, access);
        if (key == arm_key && on_read)
            on_read();
        return result;
    }

    String arm_key;
    std::function<void()> on_read;
};

}

/// `Stopped` is a transient failure observed after the pool's teardown began -- a correlation the row
/// records honestly. The arm lands right after the lease read; the round's next request is refused by
/// the open plane's fence, which the engine reports like any lost fence (a transient code).
TEST(CASGCLog, TransientFailureAfterTeardownBeganIsStopped)
{
    auto backend = std::make_shared<ArmAfterReadBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    store->setCasRetrySleepForTest([](uint64_t) {});
    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        [&](const Rec & r) { rows.push_back(r); });

    backend->arm_key = store->layout().gcStateKey();
    backend->on_read = [&store] { store->beginTeardown(); };
    EXPECT_THROW(sched.runOneRoundNow(Rec::Trigger::Manual), DB::Exception);

    const std::vector<Rec> round_rows = roundRowsOnly(rows);
    ASSERT_EQ(round_rows.size(), 2u);
    EXPECT_EQ(round_rows[1].event_type, Rec::EventType::Finish);
    EXPECT_EQ(round_rows[1].outcome, Rec::Outcome::Stopped)
        << "a transient refusal after the arm is the teardown cutting the round short, not an incident";
    EXPECT_EQ(round_rows[1].error_code, DB::ErrorCodes::NETWORK_ERROR);
    EXPECT_FALSE(round_rows[1].error.empty());
}

/// The rule is fail-closed: a non-transient failure that coincides with the arm stays `Failed`. An
/// undecodable `gc/state` throws `CORRUPTED_DATA` out of the lease phase after the very read that arms.
TEST(CASGCLog, NonTransientFailureCoincidingWithTeardownStaysFailed)
{
    auto backend = std::make_shared<ArmAfterReadBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    store->setCasRetrySleepForTest([](uint64_t) {});
    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        [&](const Rec & r) { rows.push_back(r); });

    {
        /// `gc/state` does not exist until a round writes it, so the undecodable value is planted,
        /// not substituted: the lease phase's own decode is what must fail.
        DB::Cas::tests::OperationForTest raw_op(*backend);
        const auto current = (*raw_op).read(store->layout().gcStateKey(), Retry::once());
        const WriteResult planted = current
            ? (*raw_op).replace(store->layout().gcStateKey(), "not a gc state", current->etag, Retry::once())
            : (*raw_op).create(store->layout().gcStateKey(), "not a gc state", Retry::once());
        ASSERT_TRUE(std::holds_alternative<Committed>(planted));
    }
    backend->arm_key = store->layout().gcStateKey();
    backend->on_read = [&store] { store->beginTeardown(); };
    EXPECT_THROW(sched.runOneRoundNow(Rec::Trigger::Manual), DB::Exception);

    const std::vector<Rec> round_rows = roundRowsOnly(rows);
    ASSERT_EQ(round_rows.size(), 2u);
    EXPECT_EQ(round_rows[1].outcome, Rec::Outcome::Failed)
        << "a bug that coincides with a restart is not masked as Stopped";
    EXPECT_EQ(round_rows[1].error_code, DB::ErrorCodes::CORRUPTED_DATA);
}
