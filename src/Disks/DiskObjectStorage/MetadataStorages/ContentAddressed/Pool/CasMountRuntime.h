#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Common/ThreadPool.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>

namespace DB::Cas
{

class PartWriteTxn;
using PartWriteTxnPtr = std::shared_ptr<PartWriteTxn>;

/// The pool-level lifecycle condition (rev.7 §1) a `Pool` moves through as its shared backing changes
/// underfoot. It is distinct from the storage-level `Constructing/Started/ShutDown` lifecycle (a null
/// published pool -- before `startup`/after `shutdown`) the metadata storage tracks. Ordering of the
/// enumerators is not significant; membership tests do the work.
///   - `Live`             — the steady state; the mount lease is (or was last) held.
///   - `TransientNotLive` — the lease was lost; access is uncertain and a self-remount retries. The §2
///                          `Present`+identity-match recovery rule fires only from here (or `Live`).
///   - `IdentityLost`     — the pool sentinels are authoritatively absent (both KeyAbsent):
///                          fail-loud and TERMINAL (rev.8). The remount/GC observer threads self-exit;
///                          matching-sentinel reappearance does NOT auto-revive it ([D3]); recovery is a
///                          restart or `SYSTEM CAS FORGET`.
///   - `Vanished*`        — fully terminal truth: the data root was replaced by a foreign pool, or the
///                          disk was decommissioned by `FORGET`. Store-class access fails loud from here.
enum class PoolLifecycle : uint8_t
{
    Live,
    TransientNotLive,
    IdentityLost,
    VanishedReplaced,
    VanishedForgotten,
};

/// Configuration owned by `CasMountRuntime`. `PoolConfig::mountConfig` projects the flat pool settings
/// into this value, keeping the pool's existing configuration interface unchanged while allowing this
/// lower-layer header to describe its own dependencies.
struct MountConfig
{
    std::chrono::milliseconds mount_lease_ttl_ms{30000};
    std::chrono::milliseconds mount_renew_period{10000};
    /// When false, tests drive `renewWatermarkOnce` explicitly. In production this flag enables both
    /// the merged mount-lease/build-watermark heartbeat and self-remount recovery.
    bool background_watermark = false;
    std::function<uint64_t()> boot_ms_fn = {};
    std::function<void(uint64_t)> wait_sleep_fn = {};
};

/// Local, in-memory write fence. It is deliberately not checked by reading the object store for every
/// write: the `MountLeaseKeeper` is the sole lease reader/renewer. A successful renewal translates the
/// durable `expires_at_ms` into `deadline_boot_ms`; a foreign owner, newer `writer_epoch`, or failed
/// renewal latches `lost`. Mutable operations are allowed only while the latch is clear and the local
/// deadline has not passed. The `writer_epoch` is the durable fencing token.
///
/// The fence uses `CLOCK_BOOTTIME`, not `CLOCK_MONOTONIC`: monotonic time does not advance while a VM is
/// suspended, so a resumed sleeper would compute the same "not yet expired" verdict it had before the nap
/// even though wall time (and the GC leader's fence-out) moved far ahead — it could mutate the shared state
/// under a live writer.
/// `CLOCK_BOOTTIME` includes suspend time, so a resumed sleeper sees its fence expired.
/// Container pause is already safe under either clock (the process is frozen, so no local check runs).
struct MountFence
{
    UInt128 server_uuid{};
    uint64_t writer_epoch = 0;
    /// Until something arms a real lease deadline, the permissive default allows mutations. UINT64_MAX =
    /// unarmed (never expires); otherwise a CLOCK_BOOTTIME-milliseconds instant.
    std::atomic<uint64_t> deadline_boot_ms{std::numeric_limits<uint64_t>::max()};
    std::atomic<bool> lost{false};
};

/// Owns the live writer-incarnation mechanics shared by the pool's mount and recovery orchestration:
/// the `MountLeaseKeeper`, local `MountFence`, build watermark and in-flight build registry,
/// `live_writer_epoch`, unclean-boundary marker, and self-remount thread. `Pool` retains the higher-level
/// claim/recovery sequence and its `remount_mutex`; in particular, the runtime does not acquire or own
/// the ref-ledger locks. The runtime receives its backend, layout, configuration, event sink, request
/// budget, and a callback that performs one pool-level remount attempt, so it has no `Pool` back-reference.
/// `Pool` delegates preserve the existing callers and test seams.
class CasMountRuntime
{
public:
    CasMountRuntime(
        BackendPtr backend_ptr_,
        const Layout & layout_,
        MountConfig config_,
        String server_root_id_,
        const CasEventSink & event_sink_,
        CasRequestBudget cas_request_budget_,
        /// One pool-level recovery attempt. The callback captures the owning `Pool` and is invoked only
        /// after construction, from the recovery thread.
        std::function<bool()> remount_attempt_);

    /// ---- per-server watermark and identity ----
    /// `process_epoch` is random and nonzero for this pool incarnation. GC compares it for equality,
    /// never ordering; a different value means that the previous writer incarnation is no longer live.
    uint64_t epoch() const { return process_epoch.load(std::memory_order_acquire); }
    uint64_t writerEpoch() const { return process_epoch.load(std::memory_order_acquire); }
    /// The GC floor: the oldest in-flight build_seq, or next_build_seq when no build is active (so a
    /// quiescent server's watermark floor advances to the next-to-be-allocated seq). Locks builds_mutex.
    uint64_t minActive();
    /// Test/assertion accessor for the next-to-allocate build_seq under the lock.
    uint64_t peekNextBuildSeq();
    /// Renew the merged mount heartbeat once, including its build-watermark floor. A read-only runtime
    /// has no keeper and fails with a logical exception rather than fabricating a heartbeat.
    void renewWatermarkOnce();

    /// ---- local write fence ----
    /// Return whether a mutable operation may start under the locally observed lease state.
    bool mayMutate() const;
    /// Permanently latch the local fence as lost for this runtime incarnation.
    void tripMountLost();
    /// Publish the BOOTTIME deadline from a successful lease renewal.
    void setMountDeadline(uint64_t deadline_boot_ms);
    /// Arm a new lease incarnation and clear any loss latched for the prior incarnation.
    void armMountFence(UInt128 server_uuid, uint64_t writer_epoch, uint64_t deadline_boot_ms);
    /// Test-only interposition at the publication boundary between the re-armed generation and the
    /// live fence. A caller admitted from this hook must be refused: the old generation is already
    /// dead, while the new generation is not live until `lost` is cleared.
    void setArmMountFenceInterpositionHookForTest(std::function<void()> hook)
    {
        arm_mount_fence_interposition_hook_for_test = std::move(hook);
    }
    /// The fence clock: `CLOCK_BOOTTIME` in milliseconds (includes VM-suspend time, unlike
    /// CLOCK_MONOTONIC — see `MountFence`). Consults the injected `config.boot_ms_fn` if set (tests),
    /// otherwise `bootMs`.
    uint64_t bootMsNow() const;
    /// The real boot clock: `CLOCK_BOOTTIME` in milliseconds. Static so tests can compose it.
    static uint64_t bootMs();

    /// ---- fence-generation admission (rev.7 [C2]/[D1]) ----
    /// Bumped by EVERY `tripMountLost` (a fence loss) and EVERY `armMountFence` (a re-arm -- a fresh
    /// lease incarnation, e.g. after a self-remount). A durable-effect caller captures this value once
    /// at admission and compares it again immediately before its durable backend call: a DIFFERENT
    /// value means the lease incarnation moved from under it since admission -- even when the fence
    /// happens to be live again under a brand-new incarnation, the caller's write is stale and must not
    /// land. See `checkFenceOrThrow`.
    uint64_t fenceGeneration() const { return fence_generation.load(std::memory_order_acquire); }

    /// Fence-generation admission check for every durable CAS/PUT/DELETE (the plain-object surface,
    /// staging-buffer finalize): the caller captures `fenceGeneration()` once at admission and passes it
    /// back here immediately before its durable backend call -- and again before EVERY conditional-retry
    /// iteration, not just the first attempt. Throws the typed transient refusal
    /// (`throwCasTransientUnavailable`) when the fence is not currently held or the generation moved since
    /// admission; the caller's write must never reach the backend in either case.
    void checkFenceOrThrow(uint64_t admitted_generation) const;

    /// ---- pool lifecycle condition (rev.7 §1, spec §§1-3); enum at namespace scope below ----
    /// Atomic read of the current lifecycle (acquire).
    PoolLifecycle lifecycle() const { return pool_lifecycle.load(std::memory_order_acquire); }
    /// Whether the pool has reached one of the two fully-terminal `Vanished` values
    /// (`VanishedReplaced` / `VanishedForgotten`).
    bool isVanished() const;
    /// Whether the terminal-intent latch (`vanished_intent`) is published — set by a natural
    /// `enterVanished`, OR EARLY (spec §5 step 1) by FORGET's `publishVanishedIntent`, and NEVER by the
    /// non-absorbing `IdentityLost` ([C1]). This is the EARLIEST terminal signal: it can already be true
    /// while the state is still pre-terminal (mid-FORGET). Consulted alongside `isVanished()` by every
    /// background worker that must self-exit the moment the pool is (being driven) terminal — the keeper
    /// callback (`scheduleRemount`), the remount loop, and the GC scheduler.
    bool vanishedIntentPublished() const { return vanished_intent.load(std::memory_order_acquire); }

    /// Non-terminal lease-loss transition: `Live -> TransientNotLive`. Idempotent and lock-free; a
    /// compare-exchange FROM `Live` only, so it never downgrades a terminal state. `tripMountLost`
    /// calls this (the lease-loss primitive), and the remount loop's identity gate calls it as its
    /// first step so a direct/forced remount attempt has a valid non-terminal predecessor state.
    void noteLeaseLost();
    /// Non-terminal recovery transition: `TransientNotLive -> Live`. Called after a self-remount
    /// reclaimed a fresh incarnation. A compare-exchange FROM `TransientNotLive` only, so it NEVER
    /// revives `IdentityLost`/`Vanished` ([D3]).
    void noteRemounted();

    /// One-way terminal transition to `IdentityLost`, from `TransientNotLive` only (a compare-exchange
    /// FROM `TransientNotLive`, so it is idempotent and cannot fire from `Live`/`Vanished`). On the
    /// transition it emits ONE WARN and one `CASIdentityLost` ProfileEvent. rev.8: `IdentityLost` is a
    /// fail-loud TERMINAL state — `remountTerminal()` reports it, so the remount observer thread self-exits
    /// (and the GC scheduler self-exits, through `Pool`) at its next boundary; there is no demoted observer.
    /// It deliberately does NOT publish the `vanished_intent` latch (which is reserved for the `Vanished*`
    /// idempotency/FORGET protocol); `remountTerminal()` widens the observer-exit boundary to include it.
    /// Must be called under the caller's remount serialization (Pool::remount_mutex).
    void enterIdentityLost();
    /// Test seam: force the lifecycle condition directly to `lc`, bypassing the natural transition
    /// preconditions (used by the operation-gate tests to pin each class × state cell without driving a
    /// full remount/erase sequence). For a `Vanished*` value it also latches `vanished_intent`, so the
    /// forced state is indistinguishable from a naturally-reached one. Never used in production.
    void setLifecycleForTest(PoolLifecycle lc);

    /// Publish the terminal-intent latch (`vanished_intent`) WITHOUT settling the lifecycle state. This is
    /// spec §5 step 1 of `SYSTEM CAS FORGET`: publishing the latch FIRST makes the keeper
    /// callback stop arming remounts and the remount loop bail at its next step boundary, so FORGET's
    /// subsequent thread joins are bounded to one step + one backend timeout. The state store + WARN happen
    /// later, in `enterVanished` at step 6. Idempotent; lock-free (a single release store). A natural
    /// terminal transition does NOT call this — its `enterVanished` publishes the latch itself.
    void publishVanishedIntent();

    /// One-way transition to a fully-terminal `Vanished` value (spec §3). Publishes the terminal-intent
    /// latch (so the keeper stops scheduling remounts and the remount loop exits at its next step
    /// boundary) if it is not already published, records `reason`, stores the state, then emits ONE WARN +
    /// one `CASDataRootVanished` ProfileEvent. Idempotent: the first terminal STATE transition wins (a
    /// dedicated latch keyed separately from `vanished_intent`, because FORGET publishes that intent latch
    /// early at step 1). `which` MUST be one of the two `Vanished*` values (`VanishedReplaced` or
    /// `VanishedForgotten`). `reason` is retained and
    /// surfaced verbatim in the `VanishedForgotten` [D5] error message (see `vanishedReason`). Threads exit
    /// their own loops; the joins happen in `~Pool` for a natural transition, or synchronously in
    /// `Pool::forgetDisk` for FORGET. Must be called under the caller's remount serialization
    /// (Pool::remount_mutex).
    void enterVanished(PoolLifecycle which, const String & reason);

    /// The reason string recorded by the winning `enterVanished`, or empty when none has run (a
    /// forced-for-test terminal state, or a non-terminal pool). `Pool::throwIfLifecycleTerminal` reads it
    /// to build the `VanishedForgotten` [D5] message (which carries the operator's decommission timestamp
    /// authored by `forgetDisk`). Safe to read only AFTER observing a terminal state via `lifecycle()`
    /// (acquire): the reason is written once, before the state's release-store, so a reader that
    /// acquire-observes the terminal state also observes the reason (release/acquire handoff).
    const String & vanishedReason() const { return vanished_reason; }

    /// Wall-clock second (`system_clock`, seconds since epoch) at which the pool ENTERED its current
    /// non-`Live` lifecycle state, or 0 while `Live`. This is the `since` the non-gated
    /// `system.cas_mounts` lifecycle snapshot (spec §7) reports. Written (release) at each
    /// lifecycle edge — `noteLeaseLost`/`enterIdentityLost`/`enterVanished` set it to now, `noteRemounted`
    /// clears it to 0 — and by `setLifecycleForTest`, so a forced state carries a `since` indistinguishable
    /// from a naturally-reached one.
    ///
    /// Ordering vs the `pool_lifecycle` transition it accompanies: the TERMINAL edges (`enterVanished`,
    /// `enterIdentityLost`) publish this store BEFORE the state store, so a reader that acquire-observes a
    /// terminal state is guaranteed (release/acquire handoff) to observe this timestamp. The lock-free
    /// lease-loss/remount edges (`noteLeaseLost`, `noteRemounted`) stamp it in the compare-exchange's
    /// SUCCESS branch — after the CAS — because they may run on an already-terminal pool (`noteLeaseLost` is
    /// called before the caller's `isVanished()` gate), where a pre-CAS stamp would clobber the terminal
    /// `since`; a reader may therefore momentarily observe a just-entered `not_live` with `since` not yet
    /// updated, a benign introspection artifact that converges within nanoseconds.
    time_t lifecycleSinceWallS() const
    {
        return static_cast<time_t>(lifecycle_since_wall_s.load(std::memory_order_acquire));
    }

    /// Extends `mayMutate` with a remaining-budget check. A ref-log attempt is refused unless the
    /// current lease has room for its configured timeout and safety margin, so work is not started when
    /// it cannot plausibly finish before the fence expires.
    bool refAppendFenceOk() const;

    /// The `writer_epoch` of the live mount incarnation. Bumped by `tryRemountOnce` (self-remount after a
    /// GC fence-out) — a `PartWriteTxn` minted under an older epoch fails closed on its next step.
    uint64_t liveWriterEpoch() const { return live_writer_epoch.load(std::memory_order_acquire); }

    /// ---- build registry ----
    /// Allocate a strictly-increasing `build_seq` and add it to the active set. A sequence is never
    /// reused or lowered, which lets the GC watermark advance monotonically.
    uint64_t allocateBuildSeq();
    /// Register the in-flight build so `dropNamespace`'s post-durable cancellation can reach it (weak_ptr).
    void registerInflightBuild(uint64_t seq, const PartWriteTxnPtr & build);
    /// Remove a build_seq from the active set + inflight map; idempotent (safe from publish/abandon/dtor).
    void retireBuildSeq(uint64_t seq);
    /// After the namespace-removal transaction is durable, cancel every in-flight build targeting `ns`.
    /// Live shared pointers are collected under `builds_mutex` and cancelled after releasing it, because
    /// cancellation may take a different path and must not run under the registry lock.
    void cancelInflightBuildsForNamespace(const RootNamespace & ns);

    /// ---- process epoch (identity) ----
    /// Mint the random nonzero process identity used by GC's equality check.
    void mintRandomProcessEpoch();
    /// Set `process_epoch` to the durable `writer_epoch`. The caller supplies the memory order because
    /// the initial writable claim and a later self-remount have different publication requirements.
    void setProcessEpoch(uint64_t v, std::memory_order order);
    /// Publish the live-incarnation `live_writer_epoch` with release ordering.
    void setLiveWriterEpoch(uint64_t v);

    /// ---- mount-lease keeper (owned) ----
    /// Construct the `MountLeaseKeeper` adopting (our_uuid, writer_epoch) and wire its fence callbacks
    /// (renew-ok refreshes the fence deadline; on-lost latches the fence + arms a self-remount) plus its
    /// build-watermark `minActive` reader, and event sink. `keeperStart` is separate so pool claim
    /// orchestration can catch `MountFencedException`, discard the keeper, allocate a fresh epoch, and
    /// retry the claim.
    void installKeeper(UInt128 our_uuid, uint64_t writer_epoch, const std::function<uint64_t()> & now_ms);
    /// Adopt the already-claimed mount slot; on return the adoption is durable.
    void keeperStart();
    /// Force one fresh conditional lease write on the already-adopted slot (fails closed, like any
    /// other `renewOnce`, if the slot changed hands underfoot). Used to re-anchor the write-fence
    /// arm after a materialization grace long enough to have consumed the lease TTL.
    void keeperRenewOnce();
    /// Discard a keeper after a refused adoption so the caller can retry with a fresh epoch.
    void keeperReset();
    /// Start periodic lease and watermark renewal.
    void keeperStartBackground(std::chrono::milliseconds period);
    /// Stop periodic renewal; safe to call more than once.
    void keeperStopBackground();
    bool hasKeeper() const { return static_cast<bool>(mount_keeper); }


    /// ---- self-remount recovery ----
    /// On a lost lease, arm a recovery thread when background operation is enabled. It retries the
    /// pool-level remount callback with exponential backoff until success or teardown.
    void scheduleRemount();
    /// Test seam: drive the arm/refuse path directly. Returns true iff a recovery thread is armed after.
    bool scheduleRemountForTest();
    /// Test seam: latch the shutdown gate without joining or otherwise tearing down the runtime.
    void beginShutdownForTest();
    /// Return how many times `scheduleRemount` was entered, including calls refused by the background
    /// setting. This is useful for testing the keeper's loss callback without starting a real recovery.
    uint64_t scheduleRemountCallCountForTest() const
    {
        return schedule_remount_calls_for_test.load(std::memory_order_relaxed);
    }

    /// ---- teardown ----
    /// Stop and join the self-remount thread before retiring the keeper; otherwise it could recreate the
    /// keeper while teardown is in progress.
    void stopRemountThread();
    /// Retire the merged heartbeat. When `drained` is true, publish the clean farewell; otherwise stop
    /// background renewal without writing a terminal marker, because unresolved writes must not be
    /// certified as clean. Finish with a second recovery-thread join to close the final callback window.
    void finishTeardown(bool drained);

    /// Sleep through the injected test hook when present; otherwise use the production thread sleep.
    /// `Pool` claim observation and materialization grace waits share this seam so tests control both.
    void waitSleep(uint64_t ms) const;

    /// Forward keeper events to the injected sink. The sink is held by reference so it observes the
    /// owning pool's current event routing for the runtime's entire lifetime.
    void emitEvent(CasEvent && e) const { if (event_sink) event_sink(std::move(e)); }

private:
    /// TRUE once the pool has reached — or is being driven toward — a state on which the self-remount
    /// observer thread must stop: a published terminal `Vanished` intent (`vanished_intent` — set early by
    /// FORGET, or by a natural `enterVanished`, and already subsuming every settled `Vanished*` state since
    /// it is published before the state store) OR `IdentityLost` (rev.8: a fail-loud TERMINAL state — no
    /// demoted observer; recovery is restart or FORGET). Consulted by `scheduleRemount` before arming and by
    /// the remount loop at every step boundary. (The GC scheduler applies the same three-way test through
    /// `Pool`, spec §9 rev.8 item 8.)
    bool remountTerminal() const
    {
        return vanished_intent.load(std::memory_order_acquire)
            || lifecycle() == PoolLifecycle::IdentityLost;
    }

    /// ---- injected environment (no `Pool` back-reference); initialized first, in this order ----
    BackendPtr backend_ptr;
    const Layout & layout;
    MountConfig config;
    String server_root_id;
    const CasEventSink & event_sink;
    CasRequestBudget cas_request_budget;
    std::function<bool()> remount_attempt;

    /// Per-server build watermark. `process_epoch` is a random
    /// nonzero u64 minted once at open: GC checks it for EQUALITY (an object stamped with a different
    /// epoch is from a dead incarnation), never for ordering. next_build_seq is a strictly-increasing
    /// per-process counter (monotonicity is load-bearing — a seq is never reused or lowered);
    /// active_build_seqs holds the seqs of in-flight builds, so `minActive` yields the GC floor. The floor
    /// is published by the merged `mount_keeper`
    /// beat (there is no standalone watermark object anymore). ATOMIC because a self-remount re-stamps it
    /// (kept equal to `live_writer_epoch`) off the background remount thread while `epoch`/`writerEpoch`
    /// may observe it; the ref-lane hot readers were moved to `liveWriterEpoch`, so this now backs only
    /// the identity accessors.
    std::atomic<uint64_t> process_epoch{0};
    std::mutex builds_mutex;
    uint64_t next_build_seq = 1;
    std::set<uint64_t> active_build_seqs;
    /// In-flight builds keyed by `build_seq`. `dropNamespace` upgrades these weak pointers only after its
    /// removal transaction is durable and cancels those targeting the removed namespace. The wiring owns
    /// the shared pointers, so an expired entry is simply skipped. Guarded by `builds_mutex`.
    std::map<uint64_t, std::weak_ptr<PartWriteTxn>> inflight_builds;

    /// Mount-lease heartbeat. Constructed and started on a writable
    /// open AFTER the owner/epoch/mount startup protocol; renews the mount lease async off the write
    /// path and drives the local write fence (deadline on each successful renew, `tripMountLost` on a
    /// superseded/foreign touch). Teardown stops it, whose `terminate` retires the lease (so a
    /// same-server reopen can immediately reclaim). Null on a read-only open.
    std::unique_ptr<MountLeaseKeeper> mount_keeper;

    std::atomic<uint64_t> live_writer_epoch{0};
    std::mutex remount_thread_mutex;       /// guards the thread handle below
    std::atomic<bool> remount_running{false};
    std::atomic<bool> remount_stop{false};
    std::atomic<bool> remount_shutting_down{false};   /// latched at teardown top; scheduleRemount refuses to re-arm during teardown
    std::condition_variable remount_cv;
    std::mutex remount_cv_mutex;
    ThreadFromGlobalPool remount_thread;
    /// Counted entries into `scheduleRemount`; retained as a test-only observability seam.
    std::atomic<uint64_t> schedule_remount_calls_for_test{0};

    /// Local write fence. The unarmed default (`deadline_boot_ms = UINT64_MAX`, `lost = false`) permits
    /// mutation until a keeper supplies a real lease deadline or reports that the lease was lost. This
    /// is the gate at the ref-append mutation chokepoint.
    MountFence mount_fence;

    /// Fence-generation token (rev.7 [C2]): bumped by `tripMountLost` and `armMountFence`. See
    /// `fenceGeneration`/`checkFenceOrThrow`.
    std::atomic<uint64_t> fence_generation{0};
    std::function<void()> arm_mount_fence_interposition_hook_for_test;

    /// The pool lifecycle condition (rev.7 §1). Starts `Live`. Non-terminal transitions
    /// (`noteLeaseLost`/`noteRemounted`) are lock-free compare-exchanges guarded by their exact
    /// predecessor state; the terminal transitions (`enterIdentityLost`/`enterVanished`) are serialized
    /// by the caller's `Pool::remount_mutex` and made race-safe against the keeper thread's concurrent
    /// `noteLeaseLost` by the compare-exchange/latch discipline in the .cpp.
    std::atomic<PoolLifecycle> pool_lifecycle{PoolLifecycle::Live};
    /// Terminal-intent latch (spec §3), published before the state store — by `enterVanished` for a
    /// natural transition, or EARLY (step 1) by `publishVanishedIntent` for FORGET. Only the fully-terminal
    /// `Vanished*` transition sets it — `IdentityLost` deliberately does NOT (rev.8 folds IdentityLost into
    /// the observer-exit boundary via `remountTerminal()` instead). Consulted (with `IdentityLost`) by
    /// `remountTerminal()`, so a terminal pool's keeper callback never schedules a remount and the remount
    /// loop bails at its next step boundary — no claim/allocate/write after the pool is (being driven) terminal.
    std::atomic<bool> vanished_intent{false};
    /// Idempotency guard for the terminal STATE transition (`enterVanished`'s body). Distinct from
    /// `vanished_intent`: FORGET publishes that intent latch at step 1, so it can no longer serve as the
    /// "state transition already done" flag. The FIRST `enterVanished` to win this exchange stores the
    /// state, records `vanished_reason`, and logs; every later call returns early.
    std::atomic<bool> terminal_state_published{false};
    /// The reason recorded by the winning `enterVanished` (see `vanishedReason`). Written once, BEFORE the
    /// `pool_lifecycle` release-store, and immutable thereafter — so a reader that acquire-observes a
    /// terminal state also observes this string. Empty when no terminal transition has run.
    String vanished_reason;

    /// Wall-clock second at which the current non-`Live` lifecycle state was entered; 0 while `Live` (see
    /// `lifecycleSinceWallS`). Set at every lifecycle edge with a release-store ordered before the
    /// `pool_lifecycle` transition it accompanies.
    std::atomic<int64_t> lifecycle_since_wall_s{0};
};

}
