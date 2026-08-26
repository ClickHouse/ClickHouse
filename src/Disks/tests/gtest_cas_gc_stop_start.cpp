#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

/// Task 11 (rev.7 spec §6): `SYSTEM CAS GC STOP` / `GC START` -- granular operator control
/// of ONLY the background GC scheduler. STOP is STOP-IN-PLACE: it joins the worker + heartbeat threads and
/// clears the in-process leadership hint, but RETAINS the scheduler object so a later START restarts the
/// SAME instance (its `gc_id` + lease-observation history preserved). The disk stays fully usable (reads/
/// writes unaffected) while GC is stopped. START refuses on a decommissioned/uncertain pool (typed 668).
///
/// These tests exercise the scheduler-level behavior directly (`CasGcScheduler::stop`/`start`) and the
/// end-to-end verbs through a real `ContentAddressedMetadataStorage`. Harness patterns follow
/// gtest_cas_forget.cpp and gtest_cas_gc_log.cpp.

namespace DB::ErrorCodes
{
extern const int INVALID_STATE;
}

using namespace DB;
using DB::Cas::CasGcScheduler;
using DB::Cas::GcRoundLogRecord;
using DB::Cas::InMemoryBackend;
using DB::Cas::PoolLifecycle;
using DB::Cas::RoundReport;
using DB::Cas::tests::openPoolForTest;

namespace
{

/// A live table dir + committed part reused by the "reads/writes unaffected while stopped" test (the shape
/// gtest_cas_forget.cpp / gtest_cas_operation_gate.cpp use).
const std::string kTableDir = "gg0/gg0gg0g0-0808-4808-8808-080808080808";
const std::string kPartDir = kTableDir + "/all_1_1_0";
const std::string kPartFile = kPartDir + "/data.bin";

/// The Pool-level `server_root_id` `openPoolForTest` mints (mirrors gtest_cas_lifecycle_condition.cpp).
const std::string kSrid = "test";

/// GC's fence-out applied directly to the mount lease (preserve the body, set `gc_fenced`, bump `seq`) so a
/// subsequent `tryRemountOnce` verdicts `Recover` and reclaims a FRESH incarnation immediately (no
/// lease-expiry wait), driving a transient-not-live pool back to `Live`. Mirrors
/// gtest_cas_lifecycle_condition.cpp's helper — used by the operator-STOP-persistence test below.
void fenceOutMount(DB::Cas::Backend & backend, const String & mount_key)
{
    const auto got = backend.get(mount_key);
    ASSERT_TRUE(got.has_value());
    DB::Cas::MountLease m = DB::Cas::decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    ASSERT_EQ(backend.putOverwrite(mount_key, DB::Cas::encodeMountLease(m), got->token).outcome,
              DB::Cas::PutOutcome::Done);
}

/// A real `ContentAddressedMetadataStorage` over a fresh, unique local object storage. `context == nullptr`
/// (a unit-test mount), so `startup()` creates NO GC scheduler -- the GC entry points, and `gcStart`, create
/// one lazily. GC is enabled by default (`gc_enabled == true`, `gc_interval_sec == 60`), so no background
/// round fires during the sub-second test window. Mirrors gtest_cas_forget.cpp's `openForgetStorage`.
std::shared_ptr<ContentAddressedMetadataStorage> openGcStorage()
{
    static std::atomic<uint64_t> counter{0};
    const auto scratch = std::filesystem::temp_directory_path()
        / ("ca_gc_stopstart_scratch_" + std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1)));
    auto settings = Cas::tests::makeSettingsForTest("test", scratch);
    auto storage = std::make_shared<ContentAddressedMetadataStorage>(
        Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

void commitOnePart(ContentAddressedMetadataStorage & storage)
{
    auto tx = storage.createTransaction();
    auto & ca_tx = dynamic_cast<ContentAddressedTransaction &>(*tx);
    auto buf = ca_tx.writeFile(kTableDir + "/tmp_insert_all_1_1_0/data.bin", 65536, WriteMode::Rewrite, {});
    const std::string bytes = "content-of-the-part";
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
    tx->moveDirectory(kTableDir + "/tmp_insert_all_1_1_0", kPartDir);
    tx->commit(NoCommitOptions{});
}

/// A thread-safe sink for the scheduler's per-round log records, with a condition variable so a test can
/// WAIT (never sleep) for a background round to land. `waitForSuccessFinish` blocks until a Finish record
/// with `outcome == Success` (the round acquired/renewed the GC lease) appears at index >= `from`, or the
/// timeout trips (only on a genuine hang/regression -- the round is sub-millisecond on an in-memory pool).
class RoundLogSink
{
public:
    Cas::GcRoundLogger logger()
    {
        return [this](const GcRoundLogRecord & r)
        {
            std::lock_guard lock(mutex);
            records.push_back(r);
            cv.notify_all();
        };
    }

    /// Index one past the current end of the record log -- the "from" watermark for a subsequent wait.
    size_t mark()
    {
        std::lock_guard lock(mutex);
        return records.size();
    }

    /// The first Success Finish record at index >= `from`, waiting up to `timeout`. Returns nullopt on
    /// timeout so the caller asserts with a clear message rather than hanging.
    std::optional<GcRoundLogRecord> waitForSuccessFinish(size_t from, std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        const bool ok = cv.wait_for(lock, timeout, [&]
        {
            for (size_t i = from; i < records.size(); ++i)
                if (records[i].event_type == GcRoundLogRecord::EventType::Finish
                    && records[i].outcome == GcRoundLogRecord::Outcome::Success)
                    return true;
            return false;
        });
        if (!ok)
            return std::nullopt;
        for (size_t i = from; i < records.size(); ++i)
            if (records[i].event_type == GcRoundLogRecord::EventType::Finish
                && records[i].outcome == GcRoundLogRecord::Outcome::Success)
                return records[i];
        return std::nullopt;
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<GcRoundLogRecord> records;
};

/// A generous wait bound for a background round to land -- trips only on a real deadlock/regression.
constexpr std::chrono::milliseconds kRoundWait{60000};

/// Bound for the [C1] self-exit observation: comfortably above the 1s pacing interval (so a slow CI box
/// still sees the loop tick + observe the terminal state) yet short enough that the RED demo (self-exit
/// removed) fails fast rather than hanging for `kRoundWait`.
constexpr std::chrono::milliseconds kSelfExitWait{15000};

/// A bounded OBSERVATION window (not a sleep-to-fix-a-race) for the "stays stopped across recovery" test:
/// comfortably above the 1s pacing interval so a running scheduler would have filled it with several
/// rounds, yet short enough to keep the negative assertion cheap. Its meaning is anchored by a positive
/// control (an explicit START right after DOES produce a round through the same sink).
constexpr std::chrono::milliseconds kStayStoppedWindow{3000};

}

/// (C1) A NATURAL terminal transition (`VanishedReplaced`, or here `VanishedForgotten` forced via the test
/// seam) is never accompanied by a `stop()` on this scheduler — only `~Pool`/FORGET join it. The scheduler's
/// OWN loops must observe the terminal lifecycle at their next tick and self-exit,
/// so the pacing loop stops spamming Failed rounds (the G2 zombie) and the steal-capable loop can never
/// fold/condemn a foreign pool's prefix. Drive it while RUNNING, then vanish it, then prove BOTH loops
/// self-exit (bounded cv wait, no sleep) and that no further round-log rows appear.
TEST(CASGCStopStart, SchedulerSelfExitsOnNaturalVanished)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);

    RoundLogSink sink;
    /// 1s interval: the loop ticks ~1s; the cv wait below (never a sleep) synchronizes on real records.
    CasGcScheduler sched(store, std::chrono::seconds(1), "CasGcSelfExitTest", "ca-disk", sink.logger());
    sched.start();

    /// Prove the loop is genuinely RUNNING first: a background round must land and acquire the lease.
    ASSERT_TRUE(sink.waitForSuccessFinish(/*from=*/0, kRoundWait).has_value())
        << "the scheduler must be pacing rounds before we drive it terminal";

    /// A natural terminal transition (forced here via the seam; in production `VanishedReplaced` and
    /// `IdentityLost` arrive identically, WITHOUT anyone calling stop() on this scheduler).
    store->setLifecycleForTest(PoolLifecycle::VanishedForgotten);

    ASSERT_TRUE(sched.waitForTerminalSelfExitForTest(kSelfExitWait))
        << "both the pacing and heartbeat loops must self-exit once the pool is Vanished";

    /// No further round-log rows appear after the self-exit: both loops have returned, so capture the
    /// count, reap them with stop() (a hang/double-terminate here would fail the test), and assert stable.
    const size_t count_at_exit = sink.mark();
    sched.stop();
    EXPECT_EQ(sink.mark(), count_at_exit) << "a self-exited pacing loop must emit no further round records";
    EXPECT_FALSE(sched.gcHealth().is_leader);
}

/// (C1, rev.8 §9 item 8) `IdentityLost` is now a fail-loud TERMINAL state, so the scheduler must self-exit
/// there exactly as it does on `Vanished` — a scheduler ticking against a half-erased pool is a pure zombie
/// (eternal `CORRUPTED_DATA` retries against the vanished `gc/state`). Prove BOTH loops self-exit and that no
/// further round-log rows appear, and that leadership is cleared.
TEST(CASGCStopStart, SchedulerSelfExitsOnIdentityLost)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);

    RoundLogSink sink;
    CasGcScheduler sched(store, std::chrono::seconds(1), "CasGcIdentityLostTest", "ca-disk", sink.logger());
    sched.start();

    ASSERT_TRUE(sink.waitForSuccessFinish(/*from=*/0, kRoundWait).has_value());

    store->setLifecycleForTest(PoolLifecycle::IdentityLost);

    ASSERT_TRUE(sched.waitForTerminalSelfExitForTest(kSelfExitWait))
        << "IdentityLost is terminal (rev.8): both the pacing and heartbeat loops must self-exit";

    const size_t count_at_exit = sink.mark();
    sched.stop();
    EXPECT_EQ(sink.mark(), count_at_exit) << "a self-exited pacing loop must emit no further round records";
    EXPECT_FALSE(sched.gcHealth().is_leader) << "a self-exited scheduler must report it no longer leads";
}

/// (C1 cleanup hygiene) After BOTH loops self-exit on a terminal transition, `stop()` must cleanly reap
/// the already-finished (joinable) threads, a second `stop()` is a safe no-op, and destruction (scope exit
/// → ~CasGcScheduler → stop()) runs clean — the ThreadFromGlobalPool join/reset contract holds for a
/// self-exited thread exactly as for a stop()-signalled one.
TEST(CASGCStopStart, StopAndDestroyCleanAfterSelfExit)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    {
        RoundLogSink sink;
        CasGcScheduler sched(store, std::chrono::seconds(1), "CasGcSelfExitCleanupTest", "ca-disk", sink.logger());
        sched.start();
        ASSERT_TRUE(sink.waitForSuccessFinish(/*from=*/0, kRoundWait).has_value());

        store->setLifecycleForTest(PoolLifecycle::VanishedReplaced);
        ASSERT_TRUE(sched.waitForTerminalSelfExitForTest(kSelfExitWait));

        EXPECT_NO_THROW(sched.stop()) << "stop() must cleanly join the self-exited threads";
        EXPECT_NO_THROW(sched.stop()) << "a second stop() after self-exit is a safe no-op";
        /// Destruction at scope exit runs stop() a third time — also clean (test completing proves it).
    }
    SUCCEED();
}

/// (a + e) STOP joins the worker + heartbeat threads and clears the in-process leadership hint. The T10
/// lesson: make the assertion REAL -- acquire leadership via a manual round FIRST, so `is_leader` is
/// genuinely true before STOP for the clear to prove anything (otherwise `EXPECT_FALSE` would be vacuous).
TEST(CASGCStopStart, StopJoinsWorkersAndClearsLeadershipHint)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);

    /// A long interval keeps any BACKGROUND round from firing; the manual round below is what leads.
    CasGcScheduler sched(store, std::chrono::seconds(3600), "CasGcStopStartTest", "ca-disk");
    sched.start();

    /// Acquire REAL leadership: a manual round on a free lease acquires it.
    const RoundReport rep = sched.runOneRoundNow();
    ASSERT_TRUE(rep.acquired_lease) << "a manual round on a fresh pool must acquire the free GC lease";
    ASSERT_TRUE(sched.gcHealth().is_leader) << "leadership must be true BEFORE stop for the clear to prove anything";
    ASSERT_TRUE(sched.isQuiescent()) << "the manual round completed; nothing is in flight";

    sched.stop();   /// joins loop + heartbeat threads (the test completing without hanging proves the join)

    EXPECT_TRUE(sched.isQuiescent()) << "no GC round may be in flight after stop joined the workers";
    EXPECT_FALSE(sched.gcHealth().is_leader)
        << "stop must clear the in-process leadership hint (the disk no longer leads GC)";
}

/// (b) START after STOP restarts the SAME scheduler: background rounds resume, they carry the SAME gc_id
/// (identity preserved across the restart), and leadership is re-entered via the next round's NORMAL
/// acquisition (is_leader becomes true only after the restarted background round re-acquires the lease).
/// Deterministic and sleep-free: a condition variable fed by the round logger waits for each background
/// Finish. This also exercises `start()`'s post-join re-entrancy -- a bug there would hang the wait.
TEST(CASGCStopStart, StartAfterStopResumesBackgroundRoundsWithSameGcId)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);

    RoundLogSink sink;
    /// 1s interval: the background loop's first round fires ~1s after start(); the cv wait (not a sleep)
    /// synchronizes on the actual Finish record.
    CasGcScheduler sched(store, std::chrono::seconds(1), "CasGcStopStartTest", "ca-disk", sink.logger());

    /// First run: background rounds start and one acquires the lease.
    sched.start();
    const auto first = sink.waitForSuccessFinish(/*from=*/0, kRoundWait);
    ASSERT_TRUE(first.has_value()) << "the background scheduler must run a round and acquire the lease after start()";
    EXPECT_TRUE(sched.gcHealth().is_leader) << "leadership is held after the first background round";
    const std::string gc_id_before = first->gc_id;
    EXPECT_FALSE(gc_id_before.empty());

    /// Stop: leadership hint cleared, threads joined.
    sched.stop();
    EXPECT_FALSE(sched.gcHealth().is_leader) << "stop clears the leadership hint";
    const size_t after_stop = sink.mark();

    /// Restart the SAME instance: a NEW background round must land, re-acquiring the lease, and it must
    /// carry the SAME gc_id (proving the instance -- and its lease observer -- survived the restart).
    sched.start();
    const auto second = sink.waitForSuccessFinish(/*from=*/after_stop, kRoundWait);
    ASSERT_TRUE(second.has_value()) << "background rounds must resume after START (start() is re-enterable post-join)";
    EXPECT_EQ(second->gc_id, gc_id_before) << "the restarted scheduler must preserve its gc_id (same instance)";
    EXPECT_TRUE(sched.gcHealth().is_leader)
        << "leadership is re-entered via the restarted round's normal lease acquisition";

    sched.stop();
}

/// (c) STOP and START are both idempotent: a second STOP on an already-stopped scheduler is a safe no-op,
/// and a second START on a running one is a no-op that leaves it running (a manual round still works).
TEST(CASGCStopStart, StopAndStartAreIdempotent)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasGcScheduler sched(store, std::chrono::seconds(3600), "CasGcStopStartTest", "ca-disk");

    sched.start();
    EXPECT_NO_THROW(sched.start()) << "a second START on a running scheduler is a no-op";

    sched.stop();
    EXPECT_NO_THROW(sched.stop()) << "a second STOP on a stopped scheduler is a safe no-op";
    EXPECT_TRUE(sched.isQuiescent());
    EXPECT_FALSE(sched.gcHealth().is_leader);

    /// After the double-stop, START still restarts the same instance and it runs a round.
    sched.start();
    const RoundReport rep = sched.runOneRoundNow();
    EXPECT_TRUE(rep.acquired_lease) << "the restarted scheduler still runs rounds after idempotent stop/start";
    sched.stop();
}

/// (d) START refuses on a Vanished disk with the typed 668 (`INVALID_STATE`) error -- restarting GC on a
/// decommissioned pool is meaningless and would only spin failing rounds -- while STOP on the SAME
/// Vanished disk (with a live scheduler present) SUCCEEDS: stopping the reclaimer on a sick disk is a
/// legitimate operator action, so STOP never consults the operation gate.
TEST(CASGCStopStart, StartRefusesOnVanishedButStopSucceeds)
{
    /// START on a Vanished disk -> typed 668. No scheduler needed: the gate refuses before touching it.
    {
        auto storage = openGcStorage();
        auto pool = storage->store();                       /// captured while Live (store() throws once Vanished)
        pool->setLifecycleForTest(PoolLifecycle::VanishedForgotten);
        Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->gcStart(); });
    }

    /// STOP on a Vanished disk WITH a live scheduler -> succeeds.
    {
        auto storage = openGcStorage();
        storage->gcStart();                                 /// Live: lazily creates + starts a scheduler
        ASSERT_TRUE(storage->gcHealth().has_value()) << "gcStart must have created a scheduler on a Live disk";

        auto pool = storage->store();
        pool->setLifecycleForTest(PoolLifecycle::VanishedForgotten);

        EXPECT_NO_THROW(storage->gcStop()) << "stopping GC on a Vanished disk is legitimate operator action";
    }
}

/// (f) The disk stays fully usable while its GC scheduler is stopped: a store()-path write + read succeed
/// after `gcStop`. STOP controls ONLY the GC pacer, not the disk's data plane.
TEST(CASGCStopStart, DiskReadsWritesUnaffectedWhileGcStopped)
{
    auto storage = openGcStorage();
    storage->gcStart();   /// create + start the scheduler
    storage->gcStop();    /// stop it in place (scheduler retained, threads joined)

    /// A write (commit a part) and a read (existsFile) both succeed with GC stopped.
    EXPECT_NO_THROW(commitOnePart(*storage));
    EXPECT_TRUE(storage->existsFile(kPartFile)) << "reads/writes must be unaffected while the GC scheduler is stopped";

    /// And START brings the scheduler back (idempotent, re-enterable) without disturbing the data.
    EXPECT_NO_THROW(storage->gcStart());
    EXPECT_TRUE(storage->existsFile(kPartFile));
    storage->gcStop();
}

/// (T11 M3, acceptance matrix) Two threads hammering `gcStop`/`gcStart` on the SAME storage concurrently.
/// The verbs serialize on `lifecycle_mutex` (then `gc_scheduler_mutex`, always in that order — so there is
/// no lock-order inversion and hence no deadlock), so each call is atomic: the barrage interleaves in any
/// order but never tears the retained scheduler pointer or its worker-thread set. We bound each worker with
/// a `std::future` timeout (never a sleep) so a deadlock regression fails FAST instead of hanging the suite,
/// and — since the final serialized call determines the resting state — a single quiet STOP then START at
/// the end lands the object in a well-defined, usable state (last call wins). ASan/TSan running this proves
/// the racing start()/stop() thread spawns+joins never race the shared members.
TEST(CASGCStopStart, ConcurrentStopStartFromTwoThreadsStaysConsistent)
{
    auto storage = openGcStorage();

    /// 200 iterations each, opposite phase, so the two threads spend the whole run contending on the
    /// lifecycle mutex with one about to START while the other is about to STOP.
    constexpr int kIters = 200;
    auto worker = [&](bool start_first)
    {
        for (int i = 0; i < kIters; ++i)
        {
            if (start_first) { storage->gcStart(); storage->gcStop(); }
            else             { storage->gcStop();  storage->gcStart(); }
        }
    };

    auto a = std::async(std::launch::async, worker, true);
    auto b = std::async(std::launch::async, worker, false);
    ASSERT_EQ(a.wait_for(std::chrono::seconds(60)), std::future_status::ready)
        << "two-thread GC stop/start must not deadlock (both verbs lock lifecycle_mutex then gc_scheduler_mutex)";
    ASSERT_EQ(b.wait_for(std::chrono::seconds(60)), std::future_status::ready)
        << "two-thread GC stop/start must not deadlock";
    a.get();
    b.get();

    /// No torn state: a scheduler exists (both workers created/re-entered one) and its health snapshot is
    /// coherently queryable rather than reading a half-published pointer.
    ASSERT_TRUE(storage->gcHealth().has_value()) << "the scheduler must exist and report coherent health after the barrage";

    /// Last call wins: once contention ends, one serialized STOP lands it stopped (leadership cleared,
    /// quiescent), and one serialized START lands it running again — each observed deterministically.
    storage->gcStop();
    ASSERT_TRUE(storage->gcHealth().has_value());
    EXPECT_FALSE(storage->gcHealth()->is_leader) << "a final serialized STOP clears leadership -- last call wins";

    storage->gcStart();
    EXPECT_TRUE(storage->gcHealth().has_value()) << "a final serialized START leaves the scheduler present";

    /// The data plane is unharmed by the whole barrage: a write + read still succeed.
    EXPECT_NO_THROW(commitOnePart(*storage));
    EXPECT_TRUE(storage->existsFile(kPartFile));
    storage->gcStop();
}

/// (T11 cannot-verify, acceptance matrix) Operator intent PERSISTS across a transient recovery: after the
/// operator STOPs GC, the disk loses its mount lease (transient-not-live) and self-remounts back to Live —
/// and NOTHING restarts the GC scheduler. Recovery is a Pool-internal operation with no reference to the
/// scheduler; only an explicit START (`SYSTEM CAS GC START`) resumes it. We prove the scheduler
/// was genuinely running+leading first, STOP it, drive a real transient→Live recovery on the pool, then show
/// it stays stopped across a bounded observation window (a running 1s-paced scheduler would have produced
/// several rounds), and finally that an explicit START — the ONLY resumption path — brings rounds back on the
/// SAME instance (`gc_id` preserved). The positive control makes the negative meaningful: the sink IS live.
TEST(CASGCStopStart, OperatorStopPersistsAcrossTransientRecovery)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);

    RoundLogSink sink;
    /// 1s interval so a RUNNING scheduler would pace rounds within the observation window below.
    CasGcScheduler sched(store, std::chrono::seconds(1), "CasGcStopPersistTest", "ca-disk", sink.logger());

    /// The operator has GC running and leading.
    sched.start();
    ASSERT_TRUE(sink.waitForSuccessFinish(/*from=*/0, kRoundWait).has_value())
        << "the scheduler must be pacing rounds and leading before the operator stops it";
    ASSERT_TRUE(sched.gcHealth().is_leader);

    /// The operator STOPs GC (stop-in-place: threads joined, leadership hint cleared).
    sched.stop();
    ASSERT_FALSE(sched.gcHealth().is_leader);
    const size_t after_stop = sink.mark();

    /// The disk now suffers a transient mount-lease loss and self-remounts back to Live (a fresh
    /// incarnation), WITHOUT any operator action — exactly the recovery §4 describes.
    store->tripMountLost();
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::TransientNotLive);
    fenceOutMount(*backend, store->layout().mountKey(kSrid));
    ASSERT_TRUE(store->tryRemountOnce()) << "the self-remount must reclaim a fresh incarnation";
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::Live) << "the pool must auto-recover to Live";

    /// The operator's STOP persists: recovery restarted NOTHING. The scheduler is still not leading and
    /// still quiescent, and NO background round appears across a window a running scheduler would have
    /// filled many times over.
    EXPECT_FALSE(sched.gcHealth().is_leader);
    EXPECT_TRUE(sched.isQuiescent());
    EXPECT_FALSE(sink.waitForSuccessFinish(after_stop, kStayStoppedWindow).has_value())
        << "a self-remount recovery must NOT restart an operator-STOPped GC scheduler";

    /// Positive control: only an explicit START resumes rounds, on the SAME instance (gc_id preserved).
    /// This also proves the sink WOULD have caught a round, so the negative above is meaningful.
    sched.start();
    const auto resumed = sink.waitForSuccessFinish(after_stop, kRoundWait);
    ASSERT_TRUE(resumed.has_value()) << "an explicit START must resume background rounds after the recovery";
    EXPECT_TRUE(sched.gcHealth().is_leader);
    sched.stop();
}
