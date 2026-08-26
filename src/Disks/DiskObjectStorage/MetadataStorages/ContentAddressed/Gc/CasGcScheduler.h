#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Common/ThreadPool.h>
#include <base/types.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>

namespace DB::Cas
{

/// The decoupled, pure-data record the Disks-layer GC scheduler emits per round. It carries NO
/// Interpreters dependency: the metadata storage converts it into a
/// ContentAddressedGarbageCollectionLogElement and forwards it to the SystemLog. Keeping it a plain
/// POD lets the scheduler (and its unit tests) stay free of the system-log machinery.
struct GcRoundLogRecord
{
    /// `Phase`: one row per GC phase of the round, carrying that phase's own duration, semantic counts,
    /// and `ProfileEvents` delta. Emitted between the round's `Start` and `Finish`, correlated with them
    /// by `round_id`.
    enum class EventType { Start, Finish, Phase };
    /// `Deferred`: the round acquired the lease and took the skip-unchanged fast path (`RoundReport::deferred`)
    /// -- no fold, no pre-CAS deletes, no `gc/state` CAS. Distinct from `Success` so a reader of
    /// `system.cas_gc_log` (or this scheduler's own log line) can tell a round
    /// that genuinely folded and found nothing apart from one that never folded at all.
    enum class Outcome { Unknown, Success, NotALeader, Failed, Deferred };
    enum class Trigger { Scheduled, Manual };

    EventType event_type = EventType::Start;
    Outcome outcome = Outcome::Unknown;   /// Unknown until a round finishes
    Trigger trigger = Trigger::Scheduled;
    String disk_name;
    String srid;          /// server_root_id of the mount that ran this round (this pool's poolConfig())
    String gc_id;        /// hex of the scheduler's gc_id
    UInt64 round = 0;
    UInt64 candidates_marked = 0;
    UInt64 objects_deleted = 0;
    UInt64 objects_absent = 0;
    UInt64 objects_replaced = 0;
    UInt64 objects_spared = 0;
    UInt64 manifests_deleted = 0;   /// owner-removed manifest bodies deleted, distinct from blob deletes
    /// Counts for the three-stage deletion pipeline reported by `Cas::RoundReport`.
    UInt64 entries_condemned = 0;   /// entries newly condemned into the retired list this round
    UInt64 entries_graduated = 0;   /// entries newly floor-passed (published delete_pending) this round
    UInt64 entries_redeleted = 0;   /// pending exact-token blob deletes executed this round
    UInt64 fence_outs = 0;          /// expired mounts fenced-out by the round's heartbeat floor
    UInt64 anomalies = 0;           /// fold clamps surfaced (never wedging) this round
    UInt64 duration_ms = 0;
    String error;
    /// On a `Start`/`Finish` row: the whole round's delta. On a `Phase` row: THAT PHASE's delta.
    std::map<String, UInt64> profile_events;

    /// Correlator for every row of ONE round attempt: a fresh random hex id minted per `runRoundLogged`
    /// invocation and stamped on the `Start` row, every `Phase` row, and the `Finish` row.
    ///
    /// DELIBERATELY NOT `round`. `round` is 0 on `Start`, is only known after the round's single
    /// `gc/state` CAS on a folding round, and does not exist AT ALL on a `NotALeader` round -- and the
    /// rounds a reader most needs to reconstruct are exactly the ones that never got that far.
    String round_id;
    /// The GC phase this row describes; empty on `Start`/`Finish`. The literals are the ones passed to
    /// `Cas::GcPhaseTimer` at each phase's single instrumentation site.
    String phase;
    /// Wall time of this phase. MICROseconds, not milliseconds: `meta_pool_wait`, `round_commit` and
    /// `parent_seal_read` are routinely sub-millisecond and the whole point of the row is seeing when
    /// they are not.
    UInt64 phase_duration_microseconds = 0;
    /// Phase-specific semantic counts (`Phase` rows only). The per-phase verb counts ride
    /// `profile_events` above instead, so no verb columns are invented.
    std::map<String, UInt64> phase_metrics;
};

using GcRoundLogger = std::function<void(const GcRoundLogRecord &)>;

/// Paces regular content-addressed garbage-collection rounds for one pool. The scheduler does not
/// implement the GC protocol: `Cas::Gc` owns lease acquisition, work deduplication, and the
/// split-brain-safe round operations, so schedulers on different mounters may run independently.
/// This class waits for a tick, runs one round, records its result, and retries after exceptions;
/// round operations are idempotent, while `LOGICAL_ERROR` exceptions remain visible in the log.
///
/// The background and heartbeat workers are `ThreadFromGlobalPool` instances. The background worker
/// therefore has the attached `ThreadStatus` required by `ProfileEventsScope` for per-round
/// `ProfileEvents` deltas. A manual round runs on the caller's thread; if that thread has no
/// `ThreadStatus`, the per-round delta is omitted rather than making the GC round fail.
class CasGcScheduler
{
public:
    CasGcScheduler(
        Cas::PoolPtr store_,
        std::chrono::seconds interval_,
        const String & log_name,
        String disk_name_,
        GcRoundLogger logger_ = {});
    ~CasGcScheduler();

    /// Starts the periodic round and heartbeat workers. Calling `start` more than once while the
    /// scheduler is running is a no-op; after `stop`, it may be started again on the SAME instance --
    /// the persistent `gc` observer and `gc_id` are preserved, so the lease's observation-window
    /// protocol continues across a stop/start (`SYSTEM CAS GC START`).
    void start();

    /// Stops both workers, wakes them if they are waiting, and joins them before returning. It is
    /// safe to call `stop` when the scheduler is not running and from the destructor. It also clears
    /// the in-process `i_am_leader` hint (after the joins), so a stopped scheduler reports it no longer
    /// leads GC; the durable `gc/state` lease is untouched (`SYSTEM CAS GC STOP`).
    void stop();

    /// Wake the periodic worker so it starts the next ordinary round promptly. This does not create a
    /// second execution path: the same worker, lease protocol, and `gc_round_mutex` remain the sole
    /// scheduled-round authority. Requests coalesce into one boolean while a round is pending.
    void requestRoundSoon();

    /// Test/diagnostics hook: run ONE round synchronously on the caller's thread. Returns the round
    /// report so the SYSTEM command / tests can inspect it. Emits a Start + Finish record.
    Cas::RoundReport runOneRoundNow(GcRoundLogRecord::Trigger trigger = GcRoundLogRecord::Trigger::Manual);

    /// Returns per-disk GC health for `system.cas_mounts`. The fields describing
    /// rounds snapshot this scheduler's state, while `wedged_namespace_count` is read live from the
    /// store's ref lanes; keeping the state here avoids process-global gauges colliding across disks.
    struct GcHealth
    {
        bool is_leader = false;
        bool ever_succeeded = false;
        Int64 pending_reclaim = 0;             /// cumulative condemned - executed deletes (this process)
        UInt64 last_success_age_seconds = 0;   /// seconds since the last led round (0 if never)
        UInt64 wedged_namespace_count = 0;
    };
    /// Takes a consistent-enough atomic snapshot for diagnostics. The returned counters are local
    /// health indicators rather than durable GC state, and the wedged-namespace count is queried
    /// directly from the store.
    GcHealth gcHealth() const;

    /// Whether GC is quiescent right now: TRUE iff NO round is currently in flight on this scheduler
    /// (`round_in_flight` is held for the WHOLE round body via `SCOPE_EXIT` in `runRoundLogged`, on both the
    /// success and the exception path). Used by the `SYSTEM CAS FORGET` / `GC STOP` tests to
    /// prove the scheduler's worker threads were joined — no round can be mid-flight once `stop()` returned.
    bool isQuiescent() const { return !round_in_flight.load(std::memory_order_acquire); }

    /// Test seam: force the in-flight-round flag `isQuiescent` reads, so a test can drive
    /// "running round => not quiescent" without spinning up a real round against a live backend.
    void setRoundInFlightForTest(bool v) { round_in_flight.store(v, std::memory_order_release); }

    /// Test seam (rev.7 §3 [C1]): block up to `timeout` for BOTH the pacing and heartbeat loops to have
    /// SELF-EXITED via the terminal-lifecycle check — a `Vanished` pool or a published FORGET intent — as
    /// opposed to exiting through `stop()`'s `stopping` flag. Returns false on timeout. Predicate-based
    /// wait (no sleeps); the loops set their flag under `terminal_exit_mutex` before notifying, so there is
    /// no lost-wakeup window. Lets a test prove the self-exit path fired without relying on any wall-clock
    /// delay.
    bool waitForTerminalSelfExitForTest(std::chrono::milliseconds timeout);

private:
    /// Waits for the configured interval, runs scheduled rounds while the scheduler is active, and
    /// logs exceptions before continuing with the next tick. The round lock serializes this worker
    /// with `runOneRoundNow` because the persistent `gc` object is not thread-safe.
    void loop();

    /// While this scheduler believes it owns the lease, periodically advances the advisory
    /// heartbeat independently of round progress. The cadence is shorter than the lease
    /// observation window, so a long round does not look like a dead leader to another scheduler.
    /// Heartbeat failures are advisory and are retried on the next cadence.
    void heartbeatLoop();

    /// Runs synchronously when `Cas::Gc` acquires or renews the lease, before the round's potentially
    /// long fold begins. It marks this scheduler as the heartbeat owner and sends the first pulse
    /// immediately; otherwise a new leader's first round could appear inactive until it returned.
    /// The same hook is used by scheduled and manual rounds so both acquisition paths are protected.
    void onLeaseAcquired();

    /// Run one round through the full logging path (Start record, ProfileEventsScope, Finish
    /// record). Used by BOTH loop() and runOneRoundNow. Logging is best-effort - the logger sink
    /// never throws into the round. Rethrows a round exception (after emitting an Aborted Finish).
    /// `allow_steal` is forwarded to `Cas::Gc::runRegularRound` verbatim (see its doc comment).
    Cas::RoundReport runRoundLogged(Cas::Gc & round_gc, GcRoundLogRecord::Trigger trigger,
                                     std::function<void()> on_lease_acquired = {}, bool allow_steal = true);

    const Cas::PoolPtr store;
    const std::chrono::seconds interval;
    const std::chrono::milliseconds hb_interval;   /// advisory heartbeat cadence, interval / 4 with a 50 ms minimum
    const LoggerPtr log;
    const UInt128 gc_id;
    const String disk_name;
    const GcRoundLogger logger;

    /// One persistent Gc for BOTH loop() and runOneRoundNow: the lease's observation-window steal
    /// protocol REQUIRES a stable observer (it compares the lease across consecutive runRegularRound
    /// calls of the same instance). A throwaway per call could never recover a dead-incumbent lease.
    Cas::Gc gc;
    /// Serializes the manual round against the background round so the two never touch the single
    /// (not-thread-safe) `gc` concurrently. Distinct from `mutex`: the loop releases `mutex` before
    /// the round so stop()/heartbeatLoop are not blocked, so the round cannot hold `mutex`.
    std::mutex gc_round_mutex;

    std::mutex mutex;
    std::condition_variable wake;
    bool stopping = false;
    bool round_requested = false;   /// guarded by `mutex`; coalesced external wake request
    ThreadFromGlobalPool thread;
    /// Set by the round worker and read by the heartbeat worker. It is only an in-process hint: the
    /// durable lease remains the authority, and a failed round clears the hint before retrying.
    std::atomic<bool> i_am_leader{false};
    ThreadFromGlobalPool hb_thread;

    /// Set true for the whole body of one round (`runRoundLogged`, held across the `gc_round_mutex`
    /// critical section a scheduled or manual round runs under) and cleared when it returns, on the
    /// success AND exception paths. Read by `isQuiescent` (the FORGET / GC-STOP join-completion signal).
    std::atomic<bool> round_in_flight{false};

    /// rev.7 §3 [C1] test-observation seam: set (under `terminal_exit_mutex`) by `loop`/`heartbeatLoop`
    /// respectively when they SELF-EXIT via the terminal-lifecycle check, NOT when `stop()` flips
    /// `stopping`. `waitForTerminalSelfExitForTest` waits on `terminal_exit_cv` for BOTH, so a test proves
    /// the self-exit path fired without any sleep. Purely diagnostic; production behavior never reads them.
    std::atomic<bool> loop_exited_on_terminal_for_test{false};
    std::atomic<bool> hb_exited_on_terminal_for_test{false};
    std::mutex terminal_exit_mutex;
    std::condition_variable terminal_exit_cv;

    /// Cumulative condemned entries minus exact-token deletes completed by this scheduler while it
    /// led. It is an approximate health gauge, not durable GC state.
    std::atomic<Int64> pending_reclaim{0};
    /// Steady-clock timestamp of the last round that acquired the lease; zero means never.
    std::atomic<UInt64> last_success_ms{0};
};

}
