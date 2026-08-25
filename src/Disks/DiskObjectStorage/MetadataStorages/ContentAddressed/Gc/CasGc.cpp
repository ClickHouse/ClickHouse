#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcPhaseTimer.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasNamespaceJanitor.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcShardPlan.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <unordered_set>
#include <algorithm>
#include <limits>
#include <optional>
#include <set>

namespace ProfileEvents
{
    extern const Event CASGCClampSuppressedPasses;
    extern const Event CASGCDeadPrecommitSkipped;
    extern const Event CASGCRetiredCondemned;
    extern const Event CASGCRetiredSpared;
    extern const Event CASGCRetiredGraduated;
    extern const Event CASGCRetiredRedeleted;
    extern const Event CASGCRetireReplaced;
    extern const Event CASGCCondemnMarkerUnconfirmedCarry;
    extern const Event CASGCHeartbeatFenceOuts;
    extern const Event CASGCMetaWriteAnomaly;
    extern const Event CASGCMetaOps;
    extern const Event CASGCEnumerationPages;
    extern const Event CASGCRefWalkPlansBuilt;
    extern const Event CASGCUnmatchedAdoptedParentLives;
    extern const Event CASGCStuckRemovals;
    extern const Event CASGCNamespaceCleanupLeaks;
    extern const Event CASGCRebuildVirginByEnumeration;
    extern const Event CASGCUnappliedFoldedTransactions;
    extern const Event CASRefGlobalListPages;
    extern const Event CASRefLogBodyGets;
    extern const Event CASRefManifestBodyFoldGets;
    extern const Event CASRefEmittedEdges;
    extern const Event CASRefCleanupObjectsDeleted;
}

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

namespace
{

/// The `on_page_fetched` hook GC passes to every `forEachListedKey`/`recoverRefTable`
/// call it owns (never passed by fsck/offline-repair callers of those shared helpers) -- one increment
/// per physical LIST page, never per listed key.
void onGcEnumerationPage()
{
    ProfileEvents::increment(ProfileEvents::CASGCEnumerationPages);
}

/// Defined below; forward-declared so the post-CAS hand-off delete in `runRegularRound` can
/// reach the same wholesale LIST-delete helper the retention prune uses.
uint64_t deletePrefixWholesale(Backend & backend, const String & prefix, uint64_t bounded_remaining,
                               bool * out_fully_drained = nullptr);

/// The per-hash freshness-meta operations GC schedules on the bounded pool are
/// best-effort/idempotent by design. The meta is only a point-read freshness marker for the writer/
/// promote gate; the ledger retired-set + the exact-token body delete remain the actual safety
/// core. A lost CAS here is never a correctness problem, only a (rare,
/// self-healing) staleness window for the NEXT point-reader — with ONE exception (triage 2026-07-17
/// §3.4): the CONDEMN marker is load-bearing for the delete edge. The exact-token delete argument
/// below assumes the marker was durably written before the delete fires; a swallowed condemn-marker
/// write lets a writer observe absent/Clean meta and adopt the SAME token the graduated entry later
/// deletes (a dangling manifest). Graduation to `delete_pending` is therefore GATED on confirmed
/// durable Condemned evidence for the exact (hash, token) — recorded in-process when the scheduled
/// `writeCondemnedMeta` reports success, or re-established by a synchronous `loadMeta` re-check at
/// graduation time. An unconfirmed entry is CARRIED (fail-safe delay) and its marker write retried;
/// the delete itself and every other meta op stay async/advisory.
///
/// GC freshness meta is ADD-ONLY: GC may publish `Condemned`, and may REMOVE the meta once the exact body
/// token is confirmed deleted/absent (`deleteConfirmedMeta`), but it NEVER transitions `Condemned ->
/// Clean` on a spare. The SOLE `-> Clean` transition is a WRITER that has already displaced the body with
/// a fresh incarnation token (`PartWriteTxn::ensureBlobPresent` publication + metadata reconciliation). Rationale: a deposed leader that cleared a
/// spare's meta then lost its round CAS would leave a durable stray-`Clean` over a still-condemned body;
/// a writer reading `Clean` would reuse the exact condemned token, which a stale pre-CAS exact-token
/// redelete then deletes -- live-blob data loss (INV_NO_LOSS). Removing the clear restores the exact-token
/// delete argument in full: once a hash is `Condemned`, observing `Clean` means EITHER the condemned body
/// is absent OR a writer already changed its incarnation token, so every stale `deleteExact(t1)` finds the
/// body absent or `TokenMismatch`.

/// Write the per-hash meta to Condemned: a blob newly entering the retired set this round (either the
/// fresh zero-in-degree condemn, or a republication-supersede re-condemn of the current token). Absent meta
/// is created fresh; an already-Condemned meta (a racing condemn, or a replay of this same round) is left
/// alone rather than clobbering a possibly-newer condemn_round.
///
/// Returns whether durable Condemned evidence exists after the call: the conditional write committed,
/// or an already-Condemned meta was observed. A lost CAS reports false and writes nothing further (the
/// loser re-reads next time); a thrown backend error propagates (the scheduling wrapper swallows it) —
/// either way the entry stays UNCONFIRMED and the graduation gate carries it (triage §3.4).
bool writeCondemnedMeta(Pool & pool, const BlobRef & ref, uint64_t condemn_round, uint64_t size)
{
    const auto lm = loadMeta(pool.backend(), pool.layout(), ref);
    const BlobMeta desired{.state = MetaState::Condemned, .condemn_round = condemn_round, .size = size};
    if (!lm)
        return putMetaIfAbsent(pool, ref, desired).outcome == CasOverwriteOutcome::Committed;
    if (lm->meta.state != MetaState::Condemned)
        return casMeta(pool, ref, lm->etag, desired).outcome == CasOverwriteOutcome::Committed;
    return true;
}

/// Drop the meta after its body was physically deleted (or already found absent) by the round's
/// exact-token delete. NO tombstone -- an absent meta reads exactly like a Clean one (absent
/// means not condemned"). Idempotent: an already-absent meta, or one a racing writer/GC pass already
/// moved, is a silent no-op.
void deleteConfirmedMeta(Backend & backend, const Layout & layout, const BlobRef & ref)
{
    const auto lm = loadMeta(backend, layout, ref);
    if (!lm)
        return;
    deleteMetaExact(backend, layout, ref, lm->etag);
}

}

std::set<UInt128> RefPlan::lifeIds() const
{
    std::set<UInt128> out;
    for (const auto & [life_id, row] : rows)
        out.insert(life_id);
    return out;
}

std::vector<NamespaceLifeId> RefPlan::lives() const
{
    std::vector<NamespaceLifeId> out;
    out.reserve(rows.size());
    for (const auto & [life_id, row] : rows)
        out.push_back(row.life);
    return out;
}

std::map<UInt128, RefLifeFoldState> RefPlan::parentFoldStates() const
{
    std::map<UInt128, RefLifeFoldState> out;
    for (const auto & [life_id, row] : rows)
        if (row.has_parent_fold_state)
            out.emplace(life_id, row.fold_state);
    return out;
}

std::map<UInt128, RefLifeFoldState> RefPlan::successorFoldStates() const
{
    std::map<UInt128, RefLifeFoldState> out;
    for (const auto & [life_id, row] : rows)
        out.emplace(life_id, row.fold_state);
    return out;
}

size_t RefPlan::changedRows() const
{
    return std::count_if(rows.begin(), rows.end(), [](const auto & item)
    {
        const RefWalkPlanRow & row = item.second;
        return row.tail_observation
            && row.fold_state.coverage.last_folded_ref_id < *row.tail_observation;
    });
}

std::optional<String> stuckRemovalWarning(
    const RefWalkPlanRow & row, uint64_t current_round, uint64_t threshold_rounds,
    const Layout & layout)
{
    if (!row.removal_started_round || row.fold_state.cleanup_evidence)
        return std::nullopt;
    const uint64_t started = *row.removal_started_round;
    if (current_round < started || current_round - started < threshold_rounds)
        return std::nullopt;

    const uint64_t age = current_round - started;
    const std::optional<RefHold> & hold = row.fold_state.coverage.hold;
    if (hold && hold->reason == HoldReason::BodyUndecodable)
        return fmt::format(
            "CAS GC namespace removal is stuck: namespace='{}', life_id={}, removal_started_round={}, "
            "current_round={}, age_rounds={}; cleanup evidence is absent because ref-log body '{}' is unreadable; "
            "restore the exact object or recreate the pool",
            row.life.ns.string(), u128ToHex(row.life.incarnation), started, current_round, age,
            layout.refLogKey(row.life, hold->offending_position));

    return fmt::format(
        "CAS GC namespace removal is stuck: namespace='{}', life_id={}, removal_started_round={}, "
        "current_round={}, age_rounds={}; cleanup evidence is absent because terminal has not folded",
        row.life.ns.string(), u128ToHex(row.life.incarnation), started, current_round, age);
}

RefPlan buildRefWalkPlan(RoundInput && round_input)
{
    ProfileEvents::increment(ProfileEvents::CASGCRefWalkPlansBuilt);
    RefPlan plan{std::move(round_input.ref_scan), std::move(round_input.catalog_cut)};
    const CasRefCatalog::Snapshot & catalog_cut = plan.catalog_cut;
    const RefScanSummary & ref_scan = plan.ref_scan;
    catalog_cut.life_index.throwIfAmbiguous("CAS ref walk plan");

    /// The sole admission loop. Everything below can only find one of these rows.
    for (const CatalogEntry & entry : catalog_cut.catalog.entries)
    {
        if (entry.state == NsState::Creating)
            continue;
        plan.rows.emplace(entry.incarnation, RefWalkPlanRow{
            .life = NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation),
            .fold_state = {},
            .removal_started_round = entry.removal_started_round,
            .has_parent_fold_state = false,
            .listed_hint = false,
            .checkpoint_observation = std::nullopt,
            .tail_observation = std::nullopt});
    }

    for (const auto & [life_id, state] : ref_scan.parent_ref_lives)
    {
        const auto it = plan.rows.find(life_id);
        if (it == plan.rows.end())
        {
            /// The ordinary end of a namespace's life: its removal completed, GC deleted the catalog
            /// row, and the parent seal still carries the fold state of a life the current cut no
            /// longer names. There is nothing to attach it to and nothing for anyone to do, so this
            /// is counted, not narrated -- a per-drop log line would be pure noise, and it could not
            /// discriminate the expected case from an illegitimately vanished row anyway: this site
            /// sees only the absence, never the evidence. The signal lives in
            /// `CASGCUnmatchedAdoptedParentLives` and in the round's own
            /// `walk_plan_dropped_parent_rows` phase metric; proving that a row disappeared WITHOUT a
            /// completed removal belongs to fsck, which can compare against the removal evidence.
            ProfileEvents::increment(ProfileEvents::CASGCUnmatchedAdoptedParentLives);
            ++plan.dropped_parent_rows;
            continue;
        }
        it->second.fold_state = state;
        it->second.has_parent_fold_state = true;
    }
    for (const UInt128 & life_id : ref_scan.listed_lives)
    {
        const auto it = plan.rows.find(life_id);
        if (it == plan.rows.end())
        {
            ++plan.dropped_listed_lives;
            continue;
        }
        it->second.listed_hint = true;
    }
    for (const auto & [life_id, hold] : ref_scan.holds)
    {
        const auto it = plan.rows.find(life_id);
        if (it == plan.rows.end())
        {
            ++plan.dropped_holds;
            continue;
        }
        it->second.fold_state.coverage.classification = 4;
        it->second.fold_state.coverage.hold = hold;
    }
    for (const auto & [life_id, checkpoint] : ref_scan.checkpoint_observations)
    {
        const auto it = plan.rows.find(life_id);
        if (it == plan.rows.end())
        {
            ++plan.dropped_checkpoints;
            continue;
        }
        it->second.checkpoint_observation = checkpoint;
    }
    for (const auto & [life_id, tail] : ref_scan.max_log_by_life)
    {
        const auto it = plan.rows.find(life_id);
        if (it == plan.rows.end())
        {
            ++plan.dropped_tails;
            continue;
        }
        it->second.tail_observation = tail;
    }
    return plan;
}

namespace tests
{

RefPlan buildRefWalkPlanForTest(RefScanSummary ref_scan, CasRefCatalog::Snapshot catalog_cut)
{
    return buildRefWalkPlan(RoundInput{std::move(ref_scan), std::move(catalog_cut)});
}

}

uint64_t retiredLogicalSize(ObjectKind kind, uint64_t object_size, uint64_t blob_header_len)
{
    if (kind != ObjectKind::Blob)
        return object_size;
    if (object_size < blob_header_len)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS gc retire: blob object of {} bytes is smaller than the pool's fixed blob header ({} bytes)",
            object_size, blob_header_len);
    return object_size - blob_header_len;
}

bool shouldDeferRound(size_t changed_shards, bool graduation_due, uint64_t rounds_since_last_fold,
                      uint64_t fold_threshold, uint64_t fold_max_defer_rounds)
{
    if (graduation_due)
        return false;
    if (changed_shards >= fold_threshold)
        return false;
    if (rounds_since_last_fold >= fold_max_defer_rounds)
        return false;
    return true;
}

Gc::Gc(PoolPtr store_, UInt128 gc_id_, std::function<uint64_t()> now_ms_fn_,
       std::function<uint64_t()> mono_ms_fn_, LoggerPtr log_)
    : store(std::move(store_))
    , gc_id(gc_id_)
    , logger(log_ ? std::move(log_) : getLogger("CasGc"))
    , now_ms_fn(std::move(now_ms_fn_))
    , mono_ms_fn(std::move(mono_ms_fn_))
{
    if (!store)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cas::Gc: store must not be null");
    if (gc_id == UInt128(0))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cas::Gc: gc_id must not be 0 (reserved for 'lease never held')");
    if (store->poolConfig().gc_stuck_removal_rounds == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cas::Gc: gc_stuck_removal_rounds must be nonzero");
    if (!now_ms_fn)
        now_ms_fn = []() -> uint64_t
        {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        };
    /// Use `store->bootMsNow()`, not the raw static `Pool::bootMs()`
    /// -- the latter bypasses the Pool's own injectable `config.boot_ms_fn`, so a time-controlled test
    /// that fakes the mount's clock via `boot_ms_fn` (but constructs a `Gc` without an explicit
    /// `mono_ms_fn`) would silently run its GC-side threshold math against the REAL wall clock while the
    /// mount side runs against the fake one -- two desynced clocks passing for the same test.
    /// `bootMsNow()` already falls back to `bootMs()` itself when no `boot_ms_fn` is injected, so
    /// production (no test seam in play) is unaffected.
    if (!mono_ms_fn)
        mono_ms_fn = [s = store]() -> uint64_t { return s->bootMsNow(); };
    /// Build the bounded pool for this round's per-hash freshness-meta writes here (ctor body),
    /// not a member-initializer, so it can safely read `store->poolConfig()` AFTER the null check above.
    const uint64_t configured_pool_size = store->poolConfig().gc_meta_pool_size;
    const size_t pool_size = static_cast<size_t>(std::max<uint64_t>(1, configured_pool_size));
    meta_pool = std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive,
                                             CurrentMetrics::LocalThreadScheduled, pool_size);
}

void Gc::scheduleMetaJob(std::function<void()> job)
{
    /// Wrap once: `run` is safe to invoke either on the pool or inline (the scheduling-failure fallback
    /// below), and NEVER lets an exception escape: a per-hash meta
    /// op is advisory; the ledger + exact-token body delete are the actual safety core).
    /// Capture the logger by value under a distinct name (the pool job may outlive nothing here, but it
    /// must not depend on `this`; the copy also keeps the capture from shadowing the `logger` member).
    /// `completed` is the `meta_pool_wait` phase's only visible signal: that phase's work runs HERE, on
    /// a pool thread, so it contributes nothing to the round thread's ProfileEvents delta. Captured as a
    /// raw pointer to the atomic rather than `this`, keeping `run`'s existing "must not depend on `this`"
    /// property -- the pool is a member of the same `Gc` and is joined by `~Gc` before the atomic dies.
    auto run = [job, job_logger = this->logger, completed = &meta_jobs_completed_]()
    {
        /// Count one per-hash freshness-meta op EXECUTED (attempt, not success) on this
        /// bounded pool. `run` is invoked on the pool thread (the common path below) or inline on the
        /// round's own thread (the scheduling-failure fallback below) -- either way this is pool-scoped
        /// work, so the counter is GLOBAL-only by design.
        ProfileEvents::increment(ProfileEvents::CASGCMetaOps);
        try
        {
            job();
        }
        catch (...)
        {
            ProfileEvents::increment(ProfileEvents::CASGCMetaWriteAnomaly);
            tryLogCurrentException(job_logger,
                "CAS gc: a per-hash freshness-meta op failed on the bounded pool (advisory-only; "
                "never wedges the round)");
        }
        /// After the catch: a job that threw still FINISHED, and `meta_pool_wait` reports drain
        /// progress, not success (the anomaly counter above is what reports failure).
        completed->fetch_add(1, std::memory_order_relaxed);
    };
    meta_jobs_scheduled_.fetch_add(1, std::memory_order_relaxed);
    try
    {
        meta_pool->scheduleOrThrowOnError(run);
    }
    catch (...)
    {
        /// Scheduling itself failed (e.g. resource exhaustion under a mass-DROP burst) -- run inline
        /// rather than silently lose the meta write. `run` still never throws.
        ProfileEvents::increment(ProfileEvents::CASGCMetaWriteAnomaly);
        tryLogCurrentException(logger,
            "CAS gc: meta pool scheduling failed; running the op inline on the round's own thread");
        run();
    }
}

void Gc::scheduleCondemnMarkerWrite(const BlobRef & ref, const Token & token,
                                    uint64_t condemn_round, uint64_t size)
{
    scheduleMetaJob([this, ref, token, condemn_round, size]()
    {
        if (writeCondemnedMeta(*store, ref, condemn_round, size))
            noteCondemnMarkerDurable(ref, token);
        /// A lost CAS / thrown error leaves the (ref, token) UNCONFIRMED: the graduation gate then
        /// carries the entry and retries this write on a later round (fail-safe delay, triage §3.4).
    });
}

void Gc::noteCondemnMarkerDurable(const BlobRef & ref, const Token & token)
{
    std::lock_guard lock(condemn_marker_mutex);
    condemn_markers_confirmed.emplace(ref, token.value);
}

bool Gc::condemnMarkerConfirmedInProcess(const BlobRef & ref, const Token & token)
{
    std::lock_guard lock(condemn_marker_mutex);
    return condemn_markers_confirmed.contains({ref, token.value});
}

void Gc::forgetCondemnMarker(const BlobRef & ref, const Token & token)
{
    std::lock_guard lock(condemn_marker_mutex);
    condemn_markers_confirmed.erase({ref, token.value});
}

void Gc::runNamespaceJanitorPage(
    const GcState & leased_state, bool suppress_destructive, uint64_t cleanup_evidence_rows)
{
    GcPhaseTimer t(phase_sink, "namespace_cleanup");
    t.metric("evidence_rows", cleanup_evidence_rows);
    NamespaceJanitorResult janitor_result;
    try
    {
        Backend & backend = store->backend();
        const Layout & layout = store->layout();
        NamespaceJanitor janitor(backend, layout, 1000);
        const uint64_t admitted_generation = leased_state.lease.seq;
        janitor_result = janitor.runOnePage(suppress_destructive, [&]
        {
            const auto got = backend.get(layout.gcStateKey());
            if (!got)
                return false;
            const GcState current = decodeGcState(got->bytes);
            return current.lease.owner == gc_id && current.lease.seq == admitted_generation;
        });
        for (const String & anomaly : janitor_result.anomalies)
            LOG_WARNING(logger, "CAS namespace janitor: {}", anomaly);
        if (janitor_result.leaked)
            ProfileEvents::increment(ProfileEvents::CASGCNamespaceCleanupLeaks, janitor_result.leaked);
    }
    catch (const std::exception & e)
    {
        LOG_WARNING(logger, "CAS namespace janitor skipped this round: {}", e.what());
    }
    t.metric("janitor_pages", janitor_result.pages);
    t.metric("janitor_keys", janitor_result.keys);
    t.metric("janitor_deleted", janitor_result.deleted);
    t.metric("leaked", janitor_result.leaked);
}

RoundReport Gc::runRegularRound(std::function<void()> on_lease_acquired, bool allow_steal, UniversePolicy policy)
{
    RoundReport report;
    GcState state;
    Token state_token;
    /// PHASE 1/18 `lease`. Also the ONLY phase a `NotALeader` round emits, which is why the phase rows
    /// are correlated by `round_id` and not by the round number a follower never learns.
    {
        GcPhaseTimer t(phase_sink, "lease");
        report.acquired_lease = acquireOrRenewLease(state, state_token, allow_steal);
        t.metric("acquired", report.acquired_lease ? 1 : 0);
        t.metric("steal_allowed", allow_steal ? 1 : 0);
    }
    if (!report.acquired_lease)
        return report;

    /// Baseline for the `meta_pool_wait` phase's job counts (the pool is per-`Gc`, the counters
    /// cumulative), taken before anything in this round can schedule a job.
    const uint64_t meta_jobs_scheduled_at_round_start = meta_jobs_scheduled_.load(std::memory_order_relaxed);
    const uint64_t meta_jobs_completed_at_round_start = meta_jobs_completed_.load(std::memory_order_relaxed);

    /// Fire the acquire-time hook BEFORE the long fold below, not after the round
    /// returns - a new leader's first round could otherwise run for the whole fold with no
    /// heartbeat cover, letting a follower steal deterministically once it freezes (owner, seq)
    /// across two of its own ticks.
    if (on_lease_acquired)
        on_lease_acquired();

    /// ONE-PASS round. There is no crash-resume step anymore: the round commits everything in the
    /// SINGLE gc/state CAS at the end, so a crashed pass leaves only attempt-scoped debris that is
    /// never adopted (retention prunes it), and every destructive PRE-CAS action below is justified by
    /// PREVIOUSLY PUBLISHED durable state only (delete_pending entries), so replay under a fresh
    /// attempt is idempotent.

    const Layout & layout = store->layout();
    Backend & backend = store->backend();
    const uint64_t new_round = state.round + 1;

    /// ONE budget instance for the WHOLE round, threaded into every destructive-or-observability-write
    /// family below: `fold` (blob graduation/redelete, the `GcOutcomes` audit rows, the orphan-manifest
    /// planner), `pruneSupersededGenerations`, the post-CAS hand-off reclaim (its OWN reserve, never
    /// `pruneSupersededGenerations`' shared remainder), the post-CAS manifest-body cleanup, and
    /// `cleanupRefObjects`. A cap is therefore cumulative over the round, never reset between families or
    /// shards. See `GcRoundWorkBudget`'s own comment for the fail-closed contract each family applies on
    /// exhaustion.
    GcRoundWorkBudget round_work_budget;
    round_work_budget.max_graduations = store->poolConfig().gc_round_graduation_budget;
    round_work_budget.max_redeletes = store->poolConfig().gc_round_redelete_budget;
    round_work_budget.max_sweep_namespaces = store->poolConfig().gc_round_sweep_namespace_budget;
    round_work_budget.max_sweep_recovery_ops = store->poolConfig().gc_round_sweep_recovery_op_budget;
    round_work_budget.max_ref_cleanup_objects = store->poolConfig().gc_round_ref_cleanup_budget;
    round_work_budget.max_prefix_wholesale_objects = store->poolConfig().gc_round_prefix_wholesale_budget;
    round_work_budget.max_handoff_prefix_wholesale_objects = store->poolConfig().gc_round_handoff_prefix_wholesale_budget;
    round_work_budget.max_outcome_entries = store->poolConfig().gc_round_outcome_entry_budget;

    /// The helping barrier precedes heartbeat work, DEFER, the hot stream LIST, and every successor
    /// artifact. A deferred invocation therefore cannot leave a row the adopted parent already proved
    /// complete, and a folding invocation takes its catalog cut only after the deletion settles.
    {
        GcPhaseTimer t(phase_sink, "pre_fold_ref_drain");
        const CatalogLifecycleReconcileResult drain_result = drainCompletedRemoving(state);
        for (const NamespaceLifeId & retired_life : drain_result.retired_lives)
            store->invalidateRemovedCatalogLife(retired_life);
        if (drain_result.authority_status != AuthorityStatus::Authoritative
            || drain_result.catalog_resolution != CatalogResolution::DrainComplete)
            throwCasWriteRetryLater("CAS GC pre-fold drain lost authority before the catalog settled");
        t.metric("deleted", drain_result.deleted);
    }

    /// Token-guarded fence-out of dead mounts (liveness only — graduation itself paces on GC
    /// rounds via `new_round`, not on heartbeat acks). Fencing no longer trusts a predecessor's stamped
    /// `expires_at_ms` against our wall clock — it
    /// fences ONLY once `mount_obs` has watched the mount's write-token hold unchanged for the full
    /// threshold on THIS leader's own monotonic clock (mirrors `claimMountAwaitingExpiry`'s identical
    /// `TTL + Drift` threshold for a mount's own reopen).
    const uint64_t ttl_ms = static_cast<uint64_t>(store->poolConfig().mount_lease_ttl_ms.count());
    /// The formula is shared with `claimMountAwaitingExpiry` via
    /// `mountObservationThresholdMs` -- see its doc comment (CasServerRoot.h).
    const uint64_t stable_threshold_ms = mountObservationThresholdMs(
        ttl_ms, static_cast<uint64_t>(store->poolConfig().mount_renew_period.count()));

    /// PHASE 3/18 `heartbeat_floor`: one LIST of `gc/server-roots/`, one GET per mount slot, and a fence
    /// PUT per newly-fenced mount.
    {
        GcPhaseTimer t(phase_sink, "heartbeat_floor");
        const HeartbeatFloor floor = computeHeartbeatFloor(backend, layout, now_ms_fn(), mono_ms_fn(),
                                                           stable_threshold_ms, mount_obs);
        report.fence_outs = floor.fenced_now;
        if (floor.fenced_now > 0)
            ProfileEvents::increment(ProfileEvents::CASGCHeartbeatFenceOuts, floor.fenced_now);

        /// GcFenceOut audit row per expired mount fenced-out this round: the round latched a fence-out to
        /// re-arm a sleeper's write fence (its held token is now invalid). One row per srid so the log
        /// reconstructs which mount was reclaimed.
        for (const String & srid : floor.fenced_srids)
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::GcFenceOut;
                e.object_kind = CasEventObjectKind::Snap;
                e.round = new_round;
                e.gen = state.snap_generation;
                e.outcome = "fenced";
                e.reason = "expired mount lease past skew margin; token-guarded fence-out re-arms the write "
                           "fence (prevents a resumed sleeper from mutating)";
                e.detail = {{"server_root_id", srid}};
            });

        /// Emit the round's heartbeat classification (what mounts are live/terminated/fenced this round).
        EventEmitter{*store}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::GcFence;
            e.object_kind = CasEventObjectKind::Snap;
            e.round = new_round;
            e.gen = state.snap_generation;
            e.outcome = "floor";
            e.reason = "R1: heartbeat classification (live/terminated/fenced mounts)";
            e.detail = {{"live", std::to_string(floor.live)},
                        {"terminated", std::to_string(floor.terminated)},
                        {"fenced_now", std::to_string(floor.fenced_now)},
                        {"already_fenced", std::to_string(floor.already_fenced)}};
        });

        t.metric("live", floor.live);
        t.metric("terminated", floor.terminated);
        t.metric("fenced_now", floor.fenced_now);
        t.metric("already_fenced", floor.already_fenced);
    }

    /// Decide DEFER vs FOLD from cheap pre-fold signals.
    /// A DEFER round re-adopts the sealed generation — no fold, no delete, no gc/state write — so a
    /// slow idle/small-delta round no longer rebuilds the whole in-degree snapshot. Safety: a due
    /// graduation forces a FOLD (graduationDue), so no destructive decision runs on a stale snapshot.
    ///
    /// `listRefPrefix` is the round's one full enumeration of `cas/ns/stream/`. Its result is retained
    /// (rather than discarded once the defer decision is taken) because `fold` regroups the very same
    /// keys instead of listing the prefix again. A deferred round simply drops it.
    ///
    /// PHASE 4/18 `defer_decision`. `ref_scan` OUTLIVES the timer because the fold consumes it, and
    /// `report.deferred` is set INSIDE the scope so the row already reflects the verdict when the
    /// timer's destructor fires on the deferred round's early return. This phase also performs TWO of
    /// the round's reads of the adopted fold seal (`graduationDue` and `listRefPrefix` each read the
    /// same key) -- see `fold_seal_reads` below.
    std::optional<RefPlan> walk_plan;
    bool defer_round = false;
    {
        GcPhaseTimer t(phase_sink, "defer_decision");
        const bool graduation_due = graduationDue(state, new_round);
        walk_plan.emplace(buildRefWalkPlan(listRefPrefix(state)));
        reportStuckRemovals(*walk_plan, state.round);
        const RefScanSummary & ref_scan = walk_plan->refScan();
        const size_t changed = walk_plan->changedRows();
        defer_round = shouldDeferRound(changed, graduation_due, rounds_since_last_fold_,
                                       store->poolConfig().gc_fold_threshold,
                                       store->poolConfig().gc_fold_max_defer_rounds);
        uint64_t ref_log_keys = 0;
        for (const auto & [scanned_life, ids] : ref_scan.logs_by_life)
            ref_log_keys += ids.size();
        t.metric("changed_shards", changed);
        t.metric("namespaces_seen", ref_scan.max_log_by_life.size());
        t.metric("ref_log_keys_listed", ref_log_keys);
        t.metric("ref_keys_listed", ref_scan.keys.size());
        t.metric("graduation_due", graduation_due ? 1 : 0);
        t.metric("dead_life_debris", ref_scan.dead_life_debris);
        t.metric("walk_plan_builds", 1);
        t.metric("walk_plan_rows", walk_plan->size());
        t.metric("walk_plan_dropped_parent_rows", walk_plan->droppedParentRows());
        t.metric("walk_plan_dropped_listed_lives", walk_plan->droppedListedLives());
        t.metric("walk_plan_dropped_tails", walk_plan->droppedTails());
        t.metric("deferred", defer_round ? 1 : 0);
        /// The number of consecutive rounds already deferred BEFORE this one (this round's own verdict is
        /// `deferred` above), so the pair reads unambiguously against `gc_fold_max_defer_rounds`.
        t.metric("rounds_deferred_before", rounds_since_last_fold_);
        /// `graduationDue` and `listRefPrefix` each GET the adopted fold seal at the SAME
        /// (generation, attempt). Recorded, not fixed -- see the `fold_seal_read` phase, which records
        /// the other duplicate pair; the round GETs that one key FIVE times on a folding round.
        t.metric("fold_seal_reads", 2);

        if (defer_round)
        {
            ++rounds_since_last_fold_;
            report.deferred = true;
            /// A DEFER round mints no new round -- unlike the fold path below (CasGc.cpp:642), which sets
            /// `report.round = state.round` only AFTER the round's single `gc/state` CAS has committed
            /// `next.round = new_round` and `state` was reassigned to that committed `next` (so on that
            /// path `state.round` reads the FRESH round number). Here the round CAS never runs, so `state`
            /// is still the round that was already adopted BEFORE this round started: `state.round` is the
            /// honest, already-durable round number, while `new_round` (`state.round + 1`) would report a
            /// round that never actually happened. Use `state.round` so `RoundReport::round` and the
            /// `system.cas_gc_log` row it feeds never print a fabricated
            /// round number on a deferred round.
            report.round = state.round;
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::GcFence;   /// reuse the Snap round-event channel; outcome = "deferred"
                e.object_kind = CasEventObjectKind::Snap;
                e.round = state.round;
                e.gen = state.snap_generation;
                e.outcome = "deferred";
                e.reason = "skip-unchanged: no changed shard reached the fold threshold and no graduation "
                           "is due; re-adopting the sealed generation (snapshot rebuild elided)";
                e.detail = {{"changed_shards", std::to_string(changed)},
                            {"rounds_since_last_fold", std::to_string(rounds_since_last_fold_)}};
            });
            /// Return after the timer scope so the independently timed janitor phase is not nested
            /// inside `defer_decision`.
        }
        else
            rounds_since_last_fold_ = 0;   /// this round folds
    }

    if (defer_round)
    {
        /// DEFER has no `FoldResult`, hence no complete global destructive verdict. The janitor still
        /// takes its bounded page and catalog cut, but suppression keeps both deletes and valid-page
        /// cursor progress at the same position for the bounded forced fold to retry.
        runNamespaceJanitorPage(state, /*suppress_destructive=*/true, /*cleanup_evidence_rows=*/0);
        return report;   /// no fold, no pre-CAS deletes, no gc/state CAS — sealed generation stays pinned
    }

    /// Emit that the round's single pass begins.
    EventEmitter{*store}.emit([&](CasEvent & e)
    {
        e.type = CasEventType::GcFoldBegin;
        e.object_kind = CasEventObjectKind::Snap;
        e.round = state.round;
        e.gen = state.snap_generation;
        e.reason = "R2: one-pass fold (edges x deltas x retired) into a new durable generation";
    });

    /// Capture the PARENT seal's run refs BEFORE fold mutates
    /// `state.snap_generation`/`snap_attempt` in-memory (CasGc.cpp:838). We compare these against the
    /// NEW seal's refs post-CAS to detect a ref that moved OFF an already-pruned generation (the
    /// wholesale prune skipped it while it was still referenced and its cursor advanced past it), and
    /// hand-off delete that generation's now-unreferenced leftover. Absent parent seal => empty.
    ///
    /// PHASE 5/18 `parent_seal_read`: the round's THIRD GET of the adopted fold seal (`graduationDue`
    /// and `listRefPrefix` already read it in `defer_decision`, and `fold` reads it twice more). One
    /// small GET, given its own row rather than left untimed, because "the same key, five times a round"
    /// is only actionable if each read is attributable to a phase.
    std::vector<RunRef> parent_seal_runs;
    {
        GcPhaseTimer t(phase_sink, "parent_seal_read");
        if (const auto parent_seal = readFoldSeal(state.snap_generation, state.snap_attempt))
            parent_seal_runs = parent_seal->blob_target_runs;
        t.metric("parent_runs", parent_seal_runs.size());
    }

    /// The pass performs discovery, windowing, and the three-cursor merge (spare / graduate / condemn).
    /// It emits phases 5..10 of its own.
    FoldResult folded = fold(state, state_token, report, new_round, *walk_plan, policy, round_work_budget);

    /// THE ROUND'S DESTRUCTIVE GATE, read once, here, and consulted at EVERY destructive site below.
    /// It is available this early because `fold` computes it (see `FoldResult::suppress_destructive`),
    /// and it has to be: the first destructive site of the post-CAS tail is the hand-off reclaim, which
    /// used to run before this value was ever read.
    const bool suppress_destructive = folded.suppress_destructive;

    EventEmitter{*store}.emit([&](CasEvent & e)
    {
        e.type = CasEventType::GcFoldEnd;
        e.object_kind = CasEventObjectKind::Snap;
        e.round = state.round;
        e.gen = state.snap_generation;
        e.outcome = "ok";
        e.reason = "R2 complete";
        e.detail = {{"shards", std::to_string(folded.root_shards.size())},
                    {"anomalies", std::to_string(report.anomalies.size())}};
    });

    const uint64_t generation = state.snap_generation;   /// set in-memory by fold; committed below
    const uint64_t attempt = state.snap_attempt;

    /// PRE-CAS deletes affect ONLY entries the PREVIOUS pass published as delete_pending (justified by
    /// durable state and safe at any leader staleness), plus outcome bookkeeping for
    /// every settled entry. THE SINGLE CONTENT-DELETE SITE.
    ///
    /// PHASE 11/18 `pending_deletes`, covering both the exact-token delete loop and the outcome-log
    /// writes it feeds. Held in an `optional` rather than a `{ }` scope purely so this long, delicate
    /// block is not reindented wholesale; `reset()` below is what emits the row, and an exception
    /// escaping before it still emits from the destructor.
    std::optional<GcPhaseTimer> pending_deletes_timer;
    pending_deletes_timer.emplace(phase_sink, "pending_deletes");
    const uint64_t redeleted_before = report.redeleted;
    const uint64_t graduated_before = report.graduated;
    std::map<uint64_t, OutcomeLog> outcomes;
    for (uint64_t shard = 0; shard < folded.retired_merge.size(); ++shard)
    {
        RetiredMergeResult & merge = folded.retired_merge[shard];
        /// The gate, stated at the site, and scoped to the DELETES alone: the spare / graduate / replace
        /// bookkeeping below is not destructive and must still run on a suppressed round (a suppressed
        /// round still condemns, spares and carries -- only irreversible work stops). A suppressed pass
        /// produces an EMPTY `redelete` by construction (`settleEntry` carries every pending entry
        /// unchanged instead of promoting it), so this loop would already do nothing -- but "would
        /// already do nothing" is a property of another file, and the content-delete site does not
        /// delegate its own gate. If the two ever disagree, the round deletes nothing rather than
        /// deleting on a frontier it cannot prove.
        ///
        /// WHAT THIS GUARD DOES AND DOES NOT COVER, measured rather than assumed: it stops the delete
        /// I/O, and nothing else. `settleEntry`'s gate is the primary one because promoting an entry to
        /// `redelete` also DROPS it from `still_retired` -- so a build with only this guard performs no
        /// delete and still loses the entry from the pipeline, leaking the blob instead of reclaiming
        /// it. Do not read this as a licence to relax the merge-side gate.
        static const std::vector<RetiredEntry> kNothingToDelete;
        const std::vector<RetiredEntry> & redelete_now =
            suppress_destructive ? kNothingToDelete : merge.redelete;
        for (const RetiredEntry & entry : redelete_now)
        {
            DeleteOutcome del = backend.deleteExact(layout.blobKey(entry.ref), entry.token);
            if (del.created_delete_marker)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "CAS gc: delete of blob {} created a delete marker — versioning is enabled "
                    "on the pool (mis-provisioned; the capability probe must reject this)", blobIdOf(entry.ref));

            /// A RustFS quirk: a conditional delete (`If-Match`) against an ABSENT
            /// object can answer HTTP 412 (precondition failed) instead of 404 — we map that 412 to
            /// TokenMismatch. Backend-agnostically disambiguate here: a genuine TokenMismatch means the
            /// object exists under a different (fresh) token; if a follow-up HEAD shows the object is
            /// gone, the "mismatch" was actually the object being absent — treat it as Absent (NotFound)
            /// end-to-end so the `.meta` cleanup below still runs.
            bool absent_on_mismatch_quirk = false;
            if (del.kind == DeleteOutcome::Kind::TokenMismatch)
            {
                const HeadResult head = backend.head(layout.blobKey(entry.ref));
                if (!head.exists)
                {
                    del.kind = DeleteOutcome::Kind::NotFound;
                    absent_on_mismatch_quirk = true;
                }
            }

            const DeleteClass del_class = classifyDeleteOutcome(del);
            const OutcomeKind outcome_kind = del_class == DeleteClass::Deleted ? OutcomeKind::Deleted
                                            : del_class == DeleteClass::Absent ? OutcomeKind::Absent
                                                                                : OutcomeKind::Replaced;
            OutcomeEntry outcome{.kind = entry.kind, .ref = entry.ref, .token = entry.token, .outcome = outcome_kind};
            const String del_outcome{deleteClassName(del_class)};
            /// The single content-delete site is attributable per row. TokenMismatch (a writer
            /// recreated the incarnation) is terminal-OK: the fresh incarnation is a live object.
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::BlobDelete;
                e.object_kind = CasEventObjectKind::Blob;
                e.object_hash = blobIdOf(entry.ref);
                e.token = entry.token.value;
                e.round = new_round;
                e.gen = generation;
                e.outcome = del_outcome;
                e.reason = absent_on_mismatch_quirk
                    ? "delete_pending published by a prior pass; exact-token delete (pre-CAS) "
                      "(delete returned token-mismatch but the object is absent — backend 412-on-absent quirk)"
                    : "delete_pending published by a prior pass; exact-token delete (pre-CAS)";
                e.detail = {{"condemn_round", std::to_string(entry.condemn_round)},
                            {"key", layout.blobKey(entry.ref)}};
            });
            /// The audit row is observability only -- the delete above already executed regardless of
            /// this cap. Skipping it here bounds the per-shard `GcOutcomes` body without skipping or
            /// deferring any destructive work.
            if (round_work_budget.outcomeEntryAvailable())
            {
                outcomes[shard].entries.push_back(std::move(outcome));
                ++round_work_budget.outcome_entries_used;
            }
            ++report.redeleted;
            ProfileEvents::increment(ProfileEvents::CASGCRetiredRedeleted);
            /// Drop the per-hash meta only on Deleted/NotFound — a Replaced (TokenMismatch) outcome
            /// means a writer already resurrected a fresh incarnation at this hash (INV-1), and that
            /// writer's own republication path already flipped the meta back to Clean; blindly deleting here
            /// would race that legitimate Clean write for no reason (the meta is advisory, but there is no
            /// reason to touch it on that path at all).
            if (del_class == DeleteClass::Deleted || del_class == DeleteClass::Absent)
            {
                const BlobRef ref = entry.ref;
                scheduleMetaJob([this, ref]() { deleteConfirmedMeta(store->backend(), store->layout(), ref); });
            }
            /// The entry left the pipeline — drop its in-process condemn-marker confirmation.
            forgetCondemnMarker(entry.ref, entry.token);
        }
        for (const RetiredEntry & entry : merge.spared)
        {
            /// A fresh dedup-adopt raced the condemn (see the matching CasGcFold Debug log emitted
            /// during the merge, which increments CASGCRetiredSparedByReref) -- not an ack-floor
            /// violation, so this is Debug, not a page-worthy Warning.
            if (entry.delete_pending)
                LOG_DEBUG(logger,
                    "CAS gc: delete_pending blob {} (condemned at round {}, this round {}) recovered "
                    "in-degree -- a fresh dedup-adopt raced the condemn; spared (never a fail-closed delete)",
                    blobIdOf(entry.ref), entry.condemn_round, new_round);
            /// Emit the spare verdict — a publish re-pinned the candidate before graduation.
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::GcRecheckVerdict;
                e.object_kind = CasEventObjectKind::Blob;
                e.object_hash = blobIdOf(entry.ref);
                e.token = entry.token.value;
                e.round = new_round;
                e.gen = generation;
                e.outcome = "spared";
                e.reason = "in-degree recovered in the pass merge; entry dropped";
            });
            /// The audit row is observability only -- `settleEntry` already unconditionally spared this
            /// entry (INV_NO_LOSS: recovery wins past any budget), so this cap can never re-condemn it;
            /// it only bounds whether the decision gets a `GcOutcomes` row.
            if (round_work_budget.outcomeEntryAvailable())
            {
                outcomes[shard].entries.push_back(OutcomeEntry{.kind = entry.kind, .ref = entry.ref,
                                                               .token = entry.token, .outcome = OutcomeKind::Spared});
                ++round_work_budget.outcome_entries_used;
            }
            ProfileEvents::increment(ProfileEvents::CASGCRetiredSpared);
            /// A spare does NOT touch the
            /// meta. GC freshness meta is add-only — GC never publishes `Clean`. The in-degree recovered,
            /// but the meta stays `Condemned` (conservative marker) until a WRITER displaces the body with
            /// a fresh incarnation token (unconditional publication + metadata reconciliation in
            /// `PartWriteTxn::ensureBlobPresent`) — the SOLE
            /// `Condemned -> Clean` transition. Clearing here on a
            /// deposed leader that then lost its round CAS would strand a stray-`Clean` over a still-live
            /// condemned token and lose the reuse to a stale exact-token redelete (INV_NO_LOSS); see
            /// The next `putBlob` self-heals
            /// the marker: `PartWriteTxn::ensureBlobPresent` refuses adoption on `Condemned` and
            /// publishes the writer's source under a fresh envelope.
            /// The entry left the pipeline — drop its in-process condemn-marker confirmation.
            forgetCondemnMarker(entry.ref, entry.token);
        }
        for (const RetiredEntry & entry : merge.graduated)
        {
            ++report.graduated;
            ProfileEvents::increment(ProfileEvents::CASGCRetiredGraduated);
            /// Floor-passed — republished pending; the NEXT pass executes the delete.
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::GcRecheckVerdict;
                e.object_kind = CasEventObjectKind::Blob;
                e.object_hash = blobIdOf(entry.ref);
                e.token = entry.token.value;
                e.round = new_round;
                e.gen = generation;
                e.outcome = "pending";
                e.reason = "condemn_round < current_round; published delete_pending (two-phase graduation)";
                e.detail = {{"condemn_round", std::to_string(entry.condemn_round)}};
            });
        }
        for (const ReplacedEntry & replaced : merge.replaced)
        {
            const RetiredEntry & entry = replaced.fresh;
            ProfileEvents::increment(ProfileEvents::CASGCRetireReplaced);
            /// RESURRECT-REUPLOAD-ORPHAN: the current object token differed from a stale retired entry;
            /// the fold superseded that entry and re-condemned the current token in the same window.
            /// `detail["superseded_token"]` carries the STALE token the supersede
            /// dropped — `entry.token` above is only the fresh CURRENT token, and without the old one
            /// this event cannot tell an operator WHICH incarnation was replaced.
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::BlobRetireReplaced;
                e.object_kind = CasEventObjectKind::Blob;
                e.object_hash = blobIdOf(entry.ref);
                e.token = entry.token.value;
                e.round = new_round;
                e.gen = generation;
                e.outcome = "replaced";
                e.reason = "current object token differs from the retired entry — republication replaced the "
                           "incarnation; superseded the stale entry and re-condemned the current token";
                e.detail = {{"superseded_token", replaced.old_token.value}};
            });
            /// The supersede is ALSO a blob entering the retired set fresh (a re-condemn of the
            /// CURRENT token) — write the meta Condemned exactly like a fresh `head_blob` condemn would,
            /// so a NEXT writer's point-read gate sees it (and the graduation gate gets its (hash, token)
            /// confirmation on success). `peek_head` itself stays side-effect-free (it runs once per
            /// closed candidate, not just on a real supersede — see its own comment). The SUPERSEDED
            /// (stale) token's in-process confirmation is dropped — that entry left the pipeline.
            scheduleCondemnMarkerWrite(entry.ref, entry.token, entry.condemn_round, entry.size);
            forgetCondemnMarker(entry.ref, replaced.old_token);
        }
    }

    /// Outcome logs: write-once + byte-adopt (observation-bearing HEAD tokens — never the
    /// deterministic-artifact path). Tally the report from the FINAL durable logs.
    for (auto & [shard, log] : outcomes)
    {
        const String key = layout.outcomesKey(generation, attempt, new_round, shard);
        const String body = sealObject(FormatId::GcOutcomes, encodeOutcomeLog(log));
        if (backend.putIfAbsent(key, body).outcome == PutOutcome::PreconditionFailed)
        {
            const auto existing = backend.get(key);
            if (!existing)
                throw Exception(ErrorCodes::ABORTED,
                    "CAS gc: outcome log at {} vanished between putIfAbsent and read", key);
            if (existing->bytes != body)
            {
                try { log = decodeOutcomeLog(openObject(FormatId::GcOutcomes, existing->bytes)); }
                catch (const Exception & e)
                {
                    throw Exception(ErrorCodes::ABORTED,
                        "CAS gc: undecodable outcome log at {} cannot be adopted: {}", key, e.message());
                }
            }
        }
        for (const OutcomeEntry & o : log.entries)
        {
            switch (o.outcome)
            {
                case OutcomeKind::Deleted: ++report.deleted; break;
                case OutcomeKind::Absent: ++report.absent; break;
                case OutcomeKind::Replaced: ++report.replaced; break;
                case OutcomeKind::Spared: ++report.spared; break;
            }
        }
    }
    pending_deletes_timer->metric("redeleted", report.redeleted - redeleted_before);
    pending_deletes_timer->metric("graduated", report.graduated - graduated_before);
    pending_deletes_timer->metric("deleted", report.deleted);
    pending_deletes_timer->metric("absent", report.absent);
    pending_deletes_timer->metric("replaced", report.replaced);
    pending_deletes_timer->metric("spared", report.spared);
    pending_deletes_timer->metric("outcome_logs_written", outcomes.size());
    pending_deletes_timer.reset();   /// emits the `pending_deletes` row

    /// Wait for the round's whole batch of per-hash freshness-meta writes (condemned during the
    /// fold above, spared/redeleted-confirmed during R3 above) BEFORE the round's retired-list publish and
    /// its single gc/state CAS below — the writer's meta point-read gate must see this round's condemns
    /// durable no later than the ledger it is paired with. `wait()` never throws here: every scheduled job
    /// already caught its own exception (see `scheduleMetaJob`).
    ///
    /// PHASE 12/18 `meta_pool_wait`, AND THE ONE HONEST GAP IN THIS INSTRUMENTATION: the work being
    /// waited on runs on `meta_pool` threads, so none of it appears in this thread's `ProfileEvents`
    /// delta and the row's `ProfileEvents` map is EMPTY BY CONSTRUCTION. That is not a phase with no
    /// cost -- it is a phase whose cost this mechanism cannot see, so it carries explicit job counts
    /// instead: read `jobs_scheduled` / `jobs_completed` next to the duration to tell "the queue was
    /// deep" from "the endpoint was slow". `jobs_completed` is sampled BEFORE the wait deliberately --
    /// after it, it would always equal `jobs_scheduled` and say nothing.
    {
        GcPhaseTimer t(phase_sink, "meta_pool_wait");
        const uint64_t scheduled = meta_jobs_scheduled_.load(std::memory_order_relaxed)
            - meta_jobs_scheduled_at_round_start;
        const uint64_t completed_on_entry = meta_jobs_completed_.load(std::memory_order_relaxed)
            - meta_jobs_completed_at_round_start;
        meta_pool->wait();
        t.metric("jobs_scheduled", scheduled);
        t.metric("jobs_completed_on_entry", completed_on_entry);
        t.metric("jobs_completed", meta_jobs_completed_.load(std::memory_order_relaxed)
                                   - meta_jobs_completed_at_round_start);
    }

    /// Retired-in-snapshot — there is NO separate retired-list object to publish anymore. The
    /// round's surviving condemned entries were already sealed as `kCondemned` rows inside the fold's
    /// `blob_target_runs` (durable before this CAS, via `putDeterministicArtifact`), and the per-shard
    /// `condemned_summary` the seal carries makes the next round's graduation/carry decisions zero-I/O.

    /// The SINGLE round CAS publishes the round, adopted (generation, attempt), and retention cursor.
    ///
    /// PHASE 13/18 `round_commit`. It deliberately covers BOTH the retention prune (heavy: LISTs and
    /// wholesale deletes, bounded at 64 generations a round) and the single `gc/state` CAS (trivial),
    /// because the prune's writes are only safe as a pre-CAS action and splitting the two would suggest
    /// they are independently retryable. `generations_visited` vs the prune's actual deletes is what
    /// separates the two costs inside the row.
    std::optional<GcPhaseTimer> round_commit_timer;
    round_commit_timer.emplace(phase_sink, "round_commit");
    const String manifest_sweep_cursor_before = state.manifest_sweep_cursor;
    GcState next = state;
    next.round = new_round;
    if (!suppress_destructive && store->poolConfig().manifest_sweep_list_budget_keys > 0)
        next.manifest_sweep_cursor = folded.orphan_sweep.next_cursor;
    /// The generations the adopted seal's runs physically live in (reference-parent carry can point a
    /// current shard's run back at an older generation's key). Retention must never reclaim these.
    std::set<uint64_t> referenced_generations;
    for (const RunRef & r : folded.fold_seal.blob_target_runs)
        referenced_generations.insert(r.generation);
    /// ALSO protect every generation the PARENT (currently-adopted, pre-fold) seal references
    /// (`parent_seal_runs`, captured above): this prune runs BEFORE the round's own gc/state CAS below, so
    /// a losing leader must not destroy what the winning leader's already-adopted seal still points at —
    /// pre-CAS destructive actions may only rely on PREVIOUSLY PUBLISHED state (triage #5).
    for (const RunRef & r : parent_seal_runs)
        referenced_generations.insert(r.generation);
    /// Retention floor uses THIS round's (post-fold) `generation`, so `gc_snapshot_generations_to_keep`
    /// keeps exactly that many generations back from the current one. If this round's `gc/state` CAS
    /// then LOSES, the prune reclaimed one generation deeper than the durably-adopted generation would
    /// imply -- an accepted forensics-window slack, not a data-loss risk: every still-reachable
    /// run/blob is independently protected via `referenced_generations` (captured pre-fold above).
    const uint64_t pruned_through_before = state.snap_pruned_through;
    pruneSupersededGenerations(generation, attempt, next, referenced_generations, suppress_destructive,
                               round_work_budget);
    round_commit_timer->metric("generations_visited", next.snap_pruned_through - pruned_through_before);
    round_commit_timer->metric("pruned_through", next.snap_pruned_through);
    round_commit_timer->metric("generations_referenced", referenced_generations.size());
    const CasResult res = backend.casPut(layout.gcStateKey(), encodeGcState(next), state_token);
    if (res.outcome != CasOutcome::Committed)
        throw Exception(ErrorCodes::ABORTED,
            "CAS gc round: gc/state moved during the round (another leader advanced it); retry next round");
    state = std::move(next);
    state_token = res.token;
    report.round = state.round;
    round_commit_timer->metric("round", report.round);
    round_commit_timer->metric("generation", generation);
    round_commit_timer.reset();   /// emits the `round_commit` row

    /// Task 7: the retire pipeline's REMAINING sizes, read from the seal this round's CAS just
    /// committed (`folded.fold_seal.condemned_summary` is TOTAL over every gc-shard -- see its own doc
    /// comment in `CasFoldSealFormat.h`). Zero-shard pools (never folded) leave these at 0.
    for (const auto & [shard, summary] : folded.fold_seal.condemned_summary)
    {
        report.pending_retired += summary.pending_total;
        report.pending_candidates += summary.condemned_total - summary.pending_total;
        report.pending_condemned += summary.condemned_total;
    }

    /// Post-CAS reference-parent HAND-OFF DELETE. `pruneSupersededGenerations` SKIPS a
    /// generation the live seal still references AND advances `snap_pruned_through` PAST it
    /// (CasGc.cpp:1066 computes the cursor as `g - 1` after the loop increments `g` over every skipped
    /// generation). So once a skipped generation is behind the cursor, the wholesale prune NEVER revisits
    /// it — a ref that later moves off it would strand that generation's WHOLE prefix (fold seal, retired/
    /// outcomes sets, all shards' runs), not just the single carried run object. Reclaim it HERE, now that
    /// the ref has moved: for every parent ref whose generation is already pruned-through and whose
    /// generation NO new live ref still references, wholesale-delete that generation's prefix — the exact
    /// reclaimer the normal prune would have used, deferred until the ref finally moved off. Best-effort:
    /// a crash between the CAS and here leaks the prefix to fsck (single-crash window, no permanent leak —
    /// but note the cursor already advanced, so a plain retry will NOT re-attempt it; fsck is the backstop).
    ///
    /// PHASE 14/18 `handoff_reclaim`.
    {
        GcPhaseTimer t(phase_sink, "handoff_reclaim");
        uint64_t objects_reclaimed = 0;
        std::set<uint64_t> new_referenced_generations;
        for (const RunRef & r : folded.fold_seal.blob_target_runs)
            new_referenced_generations.insert(r.generation);

        std::set<uint64_t> handed_off;   /// dedupe: multiple parent refs can share one generation
        /// GATED like every other destructive site, and it is also the FIRST destructive site of the
        /// post-CAS tail -- which is why the gate is read before the tail begins rather than partway
        /// down it.
        ///
        /// UNLIKE EVERY OTHER GATED SITE, SUPPRESSION HERE LOSES THE WORK RATHER THAN POSTPONING IT.
        /// The hand-off is a one-shot DIFFERENCE between the parent seal's runs and the new seal's, and
        /// a suppressed round still FOLDS -- only the irreversible half stops -- so the ref moves off
        /// the old generation on this very round and the next round's parent seal no longer names it.
        /// Nothing revisits it: `snap_pruned_through` is already past that generation and the wholesale
        /// prune only walks forward. The prefix is left to `fsck`, which is the same outcome this site
        /// already documents for a crash in this window (see the PHASE 14/18 comment above). Bounded --
        /// one small run per shard per occurrence of a suppressed round that also folded a delta -- and
        /// not a correctness problem, but it is the one place where the gate costs something permanent,
        /// so it is asserted rather than left to be discovered.
        static const std::vector<RunRef> kNoRuns;
        const std::vector<RunRef> & handoff_candidates =
            suppress_destructive ? kNoRuns : parent_seal_runs;
        for (const RunRef & old_ref : handoff_candidates)
        {
            /// Only generations the wholesale prune already passed AND that no live ref still pins.
            if (old_ref.generation > state.snap_pruned_through)
                continue;   /// not yet pruned-through: the normal prune will reclaim it when it ages out
            if (new_referenced_generations.contains(old_ref.generation))
                continue;   /// still referenced by a (possibly different-shard) live ref: keep it
            if (!handed_off.insert(old_ref.generation).second)
                continue;   /// already reclaimed this round via another shard's ref
            /// `bounded_remaining` draws from the hand-off's OWN reserve, never `UINT64_MAX` and never
            /// `pruneSupersededGenerations`' shared remainder: this hand-off is a ONE-SHOT event (see the
            /// PHASE 14/18 comment above) -- a generation this call reclaims only PARTIALLY is left
            /// exactly like a crash in this window already is -- to `fsck`, never revisited by a later
            /// round's hand-off (the parent-seal difference that triggers it does not recur once the ref
            /// has moved). The prune, by contrast, safely retries an under-served generation next round
            /// via its cursor, so sharing one pool would let a prune-heavy round strand this one-shot
            /// reclaim at zero every time; the separate reserve makes that impossible.
            const uint64_t remaining = round_work_budget.handoffPrefixWholesaleRemaining();
            if (remaining == 0)
                break;
            const uint64_t reclaimed = deletePrefixWholesale(
                backend, layout.gcGenPrefix(old_ref.generation), remaining);
            round_work_budget.handoff_prefix_wholesale_objects_used += reclaimed;
            objects_reclaimed += reclaimed;
            LOG_TRACE(logger,
                "CAS GC hand-off: generation {} moved out of the live seal below the retention cursor "
                "({} objects) — post-CAS wholesale reclaim (the prune had skipped it while referenced)",
                old_ref.generation, reclaimed);
        }
        t.metric("generations_reclaimed", handed_off.size());
        t.metric("objects_reclaimed", objects_reclaimed);
        t.metric("suppressed", suppress_destructive ? 1 : 0);
    }

    /// Post-CAS: owner-removed manifest bodies — deleted ONLY now, after their decrements were
    /// ADOPTED by the round CAS (delete-after-sealed-decrements). NOT durable across rounds: the
    /// ref-log intake cursor that discovered each `-1` edge is committed by THIS round's CAS above,
    /// so a log already folded is never re-visited and never re-populates `mf_cleanup`. This phase is
    /// deliberately unbudgeted: a cap would leave a declined entry unreachable from any live ref AND
    /// never re-derived by this pipeline, converting a bounded burst into a permanent leak. It drains
    /// the whole of `folded.mf_cleanup` every round it runs; only a crash (or the destructive-suppression
    /// gate below) leaves an entry for the orphan-manifest sweep to reclaim later.
    ///
    /// PHASE 15/18 `manifest_deletes`.
    {
        GcPhaseTimer t(phase_sink, "manifest_deletes");
        const uint64_t manifests_deleted_before = report.manifests_deleted;
        /// GATED. A manifest body is content the ref graph still describes until its decrements are both
        /// sealed AND taken on a round that could prove its frontier -- an unprovable round's `-1` may
        /// itself be the observation that is missing an owner elsewhere, so deleting the body on it is
        /// exactly the irreversible step the gate exists to withhold.
        static const std::map<ManifestId, Token> kNoManifestCleanup;
        const std::map<ManifestId, Token> & mf_cleanup_now =
            suppress_destructive ? kNoManifestCleanup : folded.mf_cleanup;
        uint64_t attempted = 0;
        for (const auto & [id, token] : mf_cleanup_now)
        {
            ++attempted;
            const DeleteOutcome mdel = backend.deleteExact(layout.manifestKey(id), token);   /// NotFound/TokenMismatch tolerated
            const DeleteClass mdel_class = classifyDeleteOutcome(mdel);
            if (mdel_class == DeleteClass::Deleted)
                ++report.manifests_deleted;
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::ManifestDelete;
                e.namespace_ = id.root_namespace.string();
                e.object_kind = CasEventObjectKind::Manifest;
                e.object_hash = manifestRefDebugString(id.ref);
                e.token = token.value;
                e.round = new_round;
                e.gen = generation;
                e.outcome = String{deleteClassName(mdel_class)};
                e.reason = "owner-removed manifest body; exact-token delete after decrements adopted";
            });
        }
        t.metric("attempted", attempted);
        t.metric("deleted", report.manifests_deleted - manifests_deleted_before);
        t.metric("suppressed", suppress_destructive ? 1 : 0);
    }

    /// Removal completion has no physical pass. The terminal fold placed positive evidence in the
    /// life row; a later invocation's catalog-only pre-fold drain owns lifecycle deletion, while the
    /// perpetual janitor owns dead-life bytes.
    uint64_t cleanup_evidence_rows = 0;
    for (const auto & [life_id, ref_life_state] : folded.fold_seal.ref_lives)
        cleanup_evidence_rows += ref_life_state.cleanup_evidence ? 1 : 0;
    runNamespaceJanitorPage(state, suppress_destructive, cleanup_evidence_rows);
    /// PHASE 17/18 `ref_object_cleanup`. Emitted even when the whole pass is skipped (`trim_enabled` is
    /// a test seam, `suppressed` gates the deletes), because "this phase did nothing and why" is exactly
    /// what a reader of a round that reclaimed nothing needs to see.
    {
        GcPhaseTimer t(phase_sink, "ref_object_cleanup");
        if (trim_enabled)
            cleanupRefObjects(folded, state.lease, suppress_destructive, round_work_budget);
        t.metric("suppressed", suppress_destructive ? 1 : 0);
        t.metric("trim_enabled", trim_enabled ? 1 : 0);
        t.metric("namespaces_planned", folded.ref_tables.size());
    }

    /// Bounded orphan-manifest backstop. The fold already exact-read each candidate, retired its exact
    /// source edges into the adopted runs and placed cursor progress in the SAME `gc/state` CAS above.
    /// Only this post-CAS tail may delete candidate bodies.
    /// PHASE 18/18 `orphan_sweep`.
    {
        GcPhaseTimer t(phase_sink, "orphan_sweep");
        ManifestSweepResult & sweep = folded.orphan_sweep;
        for (const ManifestSweepResult::Nomination & nomination : sweep.nominations)
        {
            const DeleteOutcome outcome = backend.deleteExact(nomination.key, nomination.token);
            const DeleteClass outcome_class = classifyDeleteOutcome(outcome);
            EventEmitter{*store}.emit([&](CasEvent & e)
            {
                e.type = CasEventType::ManifestDelete;
                e.namespace_ = nomination.id.root_namespace.string();
                e.object_kind = CasEventObjectKind::Manifest;
                e.object_hash = nomination.key;
                e.token = nomination.token.value;
                e.round = new_round;
                e.gen = generation;
                e.outcome = String{deleteClassName(outcome_class)};
                e.reason = "orphan-manifest sweep: source edges retired and adopted before exact-token delete";
            });
            if (outcome.kind == DeleteOutcome::Kind::TokenMismatch)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS orphan sweep: manifest key {} changed token after exact GET; immutable manifest "
                    "identity suffered illegal ABA, retained replacement", nomination.key);
            if (outcome_class == DeleteClass::Deleted)
                ++sweep.deleted;
            else
                ++sweep.skipped;
        }
        reportSweepRetention(sweep);
        t.metric("cursor_advanced", state.manifest_sweep_cursor != manifest_sweep_cursor_before ? 1 : 0);
        t.metric("list_budget_keys", store->poolConfig().manifest_sweep_list_budget_keys);
        t.metric("suppressed", suppress_destructive ? 1 : 0);
        t.metric("listed", sweep.listed);
        t.metric("deleted", sweep.deleted);
        t.metric("skipped", sweep.skipped);
        t.metric("undecodable", sweep.undecodable);
        /// THE §6 PREMISE'S SHARE OF `skipped`, BY REASON CLASS. Rule (1) is satisfiable only for a
        /// closed-and-folded epoch, so a pass in which everything examined was RETAINED is an ordinary
        /// outcome, and without these numbers it is indistinguishable on the row from a pass that found
        /// nothing to do.
        /// A row where `deleted` is 0 and all four are 0 means the sweep genuinely had no candidates.
        t.metric("retained_no_coverage", sweep.retained_no_coverage);
        t.metric("retained_hold", sweep.retained_hold);
        t.metric("retained_unconsumed_seal", sweep.retained_unconsumed_seal);
        t.metric("retained_tail_removal", sweep.retained_tail_removal);
    }

    return report;
}

void Gc::reportStuckRemovals(const RefPlan & plan, uint64_t current_round)
{
    for (const UInt128 & life_id : plan.lifeIds())
    {
        const auto warning = stuckRemovalWarning(
            plan.row(life_id), current_round, store->poolConfig().gc_stuck_removal_rounds,
            store->layout());
        if (!warning)
            continue;
        ProfileEvents::increment(ProfileEvents::CASGCStuckRemovals);
        LOG_WARNING(logger, "{}", *warning);
    }
}

bool Gc::foldManifestEdges(const ManifestId & id, int sign, std::vector<BlobDelta> & deltas,
                           std::map<ManifestId, Token> & mf_cleanup, uint32_t txn_ordinal)
{
    Backend & backend = store->backend();
    const Layout & layout = store->layout();

    const String key = layout.manifestKey(id);
    /// ONE ROUND TRIP PER EDGE. The GET alone carries the absence signal a HEAD would have carried, so
    /// the HEAD that used to precede it bought nothing and cost a second serial round trip on the
    /// hottest read path of the round (one per manifest edge, on every folded log). `!got` is the SAME
    /// absent outcome the missing HEAD used to produce -- record-and-continue, and the caller decides
    /// what an absent body means for that edge (a missing-body precommit is a barrier; a committed one
    /// fails closed). Never a throw: a 404 during the fold is an observation, not an error.
    const auto got = backend.get(key);
    if (!got)
        return false;   /// absent body: caller decides (missing-body precommit OK; committed => fail closed)
    ProfileEvents::increment(ProfileEvents::CASRefManifestBodyFoldGets);   /// one body GET per manifest fold

    const PartManifest body = decodePartManifest(openObject(FormatId::PartManifest, got->bytes));
    if (!refMatchesBody(id.ref, body))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS gc fold: manifest body ref mismatch at {} (refMatchesBody fail-closed)", key);
    if (!manifestNamespaceMatches(id.root_namespace, body))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS gc fold: manifest body namespace mismatch at {} (manifestNamespaceMatches fail-closed)", key);
    /// The manifest-wide `blob_hash_len` foreign-width gate is GONE (the field
    /// itself was deleted — entries now carry their own per-entry algo/width). `decodePartManifest`
    /// already fail-closes on an algo byte this BUILD does not know (`blobHashAlgoName` throws
    /// CORRUPTED_DATA there) -- but a known algo may still not be ADMITTED to THIS pool yet (a stale
    /// in-memory `admitted_algos` cache reading a manifest another node already admitted a new algo
    /// for). Per-entry admission validation refreshes on miss BEFORE
    /// failing closed, so a genuinely fresh admission is never mistaken for corruption.
    for (const ManifestEntry & entry : body.entries)
        if (entry.placement == EntryPlacement::Blob && !store->isAlgoAdmitted(entry.ref.algo))
        {
            const std::vector<uint8_t> refreshed = store->refreshAdmittedAlgos();
            if (!store->isAlgoAdmitted(entry.ref.algo))
            {
                String names;
                for (size_t i = 0; i < refreshed.size(); ++i)
                {
                    if (i != 0)
                        names += ", ";
                    names += blobHashAlgoName(static_cast<BlobHashAlgo>(refreshed[i]));
                }
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS gc fold: manifest entry algo {} not admitted to this pool (algos_used {{{}}})",
                    blobHashAlgoName(entry.ref.algo), names);
            }
        }

    for (const ManifestEntry & entry : body.entries)
        if (entry.placement == EntryPlacement::Blob)
        {
            /// The fold settles the FULL `BlobRef` pair natively -- no bare-digest bridge remains.
            deltas.push_back(BlobDelta{
                .ref = entry.ref,
                .source_id = sourceEdgeId(id, entry.path),
                .remove = (sign < 0),
                .txn_ordinal = txn_ordinal});
            /// A folded owner edge over this blob (the manifest-model analog of the old
            /// `RootAdd`). +1 = the manifest's owner activated this blob's reference; -1 =
            /// the owner was removed, dropping the reference. Reconstructs WHY a blob's in-degree moved.
            EventEmitter{*store}.emit([&](CasEvent & ev)
            {
                ev.type = sign > 0 ? CasEventType::RootAdd : CasEventType::RootRemove;
                ev.namespace_ = id.root_namespace.string();
                ev.object_kind = CasEventObjectKind::Blob;
                ev.object_hash = blobIdOf(entry.ref);
                ev.outcome = sign > 0 ? "edge_added" : "edge_removed";
                ev.reason = sign > 0
                    ? "fold: manifest owner activated; +1 blob edge"
                    : "fold: manifest owner removed; -1 blob edge";
                ev.detail = {{"manifest_ref_instance", manifestRefDebugString(id.ref)},
                             {"path", entry.path}};
            });
        }

    if (sign < 0)
        mf_cleanup.emplace(id, got->token);   /// owner removed: defer exact-token body delete to recheck
    return true;
}

Gc::CheckpointWitnesses Gc::readCheckpointWitnesses(const std::map<String, RefTableListing> & ref_tables,
                                                    const CasRefCatalog::Snapshot & catalog_cut)
{
    /// Read the checkpoint of every namespace in the round's catalog cut, every namespace `ref_tables`
    /// names, PLUS every namespace a HELD row in `parent_cursors` names. A catalog-only namespace is the
    /// one whose second witness matters MOST: a genuinely empty listing cannot distinguish a namespace
    /// that has no records from one whose records the same enumeration missed.
    ///
    /// EXACT KEY, ALWAYS -- never `RefTableListing::has_ckpt`. Skipping the read because the listing did
    /// not show a `_ckpt` would make the second witness a function of the first, which is precisely the
    /// dependency it exists to break: the listing is a SNAPSHOT, and a `_ckpt` that became durable after
    /// the enumeration is exactly the one whose namespace has records the same enumeration also missed.
    Backend & backend = store->backend();
    const Layout & layout = store->layout();

    std::set<String> witness_namespaces;
    for (const CatalogEntry & entry : catalog_cut.catalog.entries)
        if (entry.state == NsState::Live || entry.state == NsState::Removing)
            witness_namespaces.insert(entry.ns.string());
    for (const auto & [ns_str, listing] : ref_tables)
        witness_namespaces.insert(ns_str);

    CheckpointWitnesses out;
    for (const String & ns_str : witness_namespaces)
    {
        const RootNamespace ns{ns_str};
        /// Review C3: use the SAME complete catalog cut the round's walk resolved, never an independent
        /// catalog re-read. A namespace absent from the cut, or present only as a non-walkable
        /// `Creating` row, has no admitted witness key to read this round.
        const auto entry_it = std::lower_bound(
            catalog_cut.catalog.entries.begin(), catalog_cut.catalog.entries.end(), ns,
            [](const CatalogEntry & entry, const RootNamespace & needle) { return entry.ns < needle; });
        if (entry_it == catalog_cut.catalog.entries.end() || entry_it->ns != ns
            || (entry_it->state != NsState::Live && entry_it->state != NsState::Removing))
            continue;
        const String ckpt_key = layout.refCkptKey(NamespaceLifeId::fromCatalogEntry(entry_it->ns, entry_it->incarnation));
        /// THE GET AND THE DECODE ARE SPLIT HERE, rather than taken together through `readCkpt`, so the
        /// catch below can scope to the DECODE ALONE. Wrapping the read too would turn a transport
        /// failure -- which says nothing about this object and everything about the round's ability to
        /// read anything -- into a per-namespace hold, silently narrowing a pool-wide outage to one
        /// namespace. A backend throw still propagates and fails the round, exactly as it always did.
        const std::optional<GetResult> got = backend.get(ckpt_key);
        /// ABSENT IS NORMAL AND IS NOT A WITNESS: a namespace has no `_ckpt` until its first snapshot
        /// publication commits, and one that 404s mid-round is a namespace being reclaimed. Neither says
        /// anything about which ids exist, so neither may hold the walk -- and neither may throw
        /// (a GC fold never fails a round on a 404).
        if (!got)
            continue;

        RefCkpt ckpt;
        try
        {
            /// Materialized read, then decode (`readCkpt`'s rule): the object is MUTABLE, so the body
            /// must be fixed before it is parsed.
            ckpt = decodeRefCkpt(got->bytes);
        }
        catch (const Exception & e)
        {
            /// PER-NAMESPACE, NEVER ROUND-WIDE (spec §5: every per-namespace failure is a clamp or a
            /// hold). This object belongs to exactly one namespace, so it can never be grounds for
            /// refusing to fold another one -- and refusing the whole round is what a single unreadable
            /// 4 KiB object used to do, stopping every namespace's cursor, seal and cleanup for as long
            /// as it stayed unrepaired. It is recorded, named, and left to the walk to hold.
            ///
            /// NAME THE OBJECT. The decode's own message says what is wrong with the bytes and nothing
            /// about WHICH bytes, so without the key an operator cannot find the object to repair.
            out.undecodable.emplace(ns_str, ckpt_key + ": " + e.message());
            continue;
        }
        /// A checkpoint without `checkpoint_snapshot_id` is silent rather than empty: the object exists
        /// because some OTHER field (`life_epoch`, `last_epoch_seal`) was published into it first.
        if (ckpt.checkpoint_snapshot_id)
            out.witnesses.emplace(ns_str, *ckpt.checkpoint_snapshot_id);
        if (ckpt.life_epoch)
            out.life_epochs.emplace(ns_str, *ckpt.life_epoch);
        out.recovery_checkpoints.emplace(ns_str, std::move(ckpt));
    }
    return out;
}

std::optional<std::pair<uint64_t, uint64_t>> Gc::newestFoldSealRef()
{
    Backend & backend = store->backend();
    const Layout & layout = store->layout();
    const String gen_prefix = layout.gcGenPrefix(0);
    const String top = gen_prefix.substr(0, gen_prefix.size() - 2);   /// ".../gc/gen/"

    /// THE WIDE ENUMERATION IS A HINT HERE TOO, exactly as it is in the ref walk. Trusting it for
    /// NEWEST-ness would reopen the same hole one layer up: an enumeration that omits the true newest
    /// seal hands back an older one, and every hold detected since that older seal is silently lost --
    /// an under-carry, which is the failure this whole path exists to prevent.
    std::set<uint64_t> listed_generations;
    bool listed_anything = false;
    std::optional<std::pair<uint64_t, uint64_t>> newest;
    forEachListedKey(backend, top, [&](const ListedKey & k)
    {
        listed_anything = true;
        const size_t from = top.size();
        const size_t gen_end = k.key.find('/', from);
        if (gen_end == String::npos)
            return;
        uint64_t generation = 0;
        try
        {
            generation = std::stoull(k.key.substr(from, gen_end - from));
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            return;   /// foreign key shape under `gc/gen` is debris, not a generation number
        }
        listed_generations.insert(generation);
    }, 1000, onGcEnumerationPage);
    const uint64_t listed_max_generation = listed_generations.empty() ? 0 : *listed_generations.rbegin();

    /// STEP DOWN THROUGH THE GENERATIONS THE LISTING ITSELF REPORTED until one carries a seal. The
    /// newest generation routinely exists WITHOUT one: a round writes its runs during the reduce phase
    /// and its fold seal only at phase 10/18, so an ordinary crash in between leaves exactly that
    /// shape. Stopping at the maximum would then refuse a pool whose holds are sitting readable one
    /// generation down -- turning a plain crash into "recreate the pool".
    ///
    /// Stepping down costs no trust that has not already been spent: these are the generations the wide
    /// listing reported, and its maximum -- which the probes above are checking -- is one of them. What
    /// is NOT weakened is the refusal above: a seal found ABOVE the maximum stays terminal, because
    /// that is the listing being caught in a lie rather than merely being incomplete about seals.
    ///
    /// "Never step PAST an unreadable seal" falls out of returning the FIRST generation that carries
    /// one: the caller decodes it and refuses if it cannot, so an undecodable seal ends the search
    /// instead of being skipped over in favour of an older, readable one.
    for (auto it = listed_generations.rbegin(); it != listed_generations.rend(); ++it)
    {
        if (const auto probe = probeGenerationForSeal(*it); probe.seal_attempt)
        {
            newest = std::make_pair(*it, *probe.seal_attempt);
            break;
        }
    }

    /// DETECTION, NOT PROOF -- and labelled as such deliberately, in the same spirit as probe A. Two
    /// NARROW single-generation probes above the wide listing's maximum ask whether that maximum was a
    /// lie. The generation half of the question is arithmetic (generations are dense in minting: a fold
    /// takes `snap_generation + 1`, a rebuild `max_gen + 1`), but the attempt half is not and cannot be
    /// made so -- `attempt` is `lease.seq`, a global counter that advances on EVERY round including
    /// deferred ones, so consecutive generations carry attempts separated by unbounded gaps and there is
    /// no `attempt + 1` to point-read. So the step is an enumeration WITHIN ONE DIRECTORY: strictly
    /// narrower than the pool-wide listing whose maximum it is checking, and honestly not the exact
    /// read the ref walk gets.
    ///
    /// A seal found above the maximum means the wide listing lied about the very thing this path is
    /// deciding, so the answer is REFUSAL, not adoption of the newer seal: a store that misreports its
    /// own enumeration DURING DISASTER RECOVERY does not get a second guess, and silently adopting
    /// whatever the second query returned would just move the trust one query along.
    static constexpr uint64_t kProbeGenerationsAbove = 2;
    for (uint64_t above = 1; above <= kProbeGenerationsAbove; ++above)
    {
        const uint64_t generation = listed_max_generation + above;
        const GenerationSealProbe probe = probeGenerationForSeal(generation);
        if (!probe.seal_attempt)
            continue;
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS GC rebuild: a fold seal exists at generation {} (attempt {}) while the pool-wide "
            "enumeration of {} reported nothing above generation {}. The enumeration this rebuild "
            "would have taken its baseline from is demonstrably incomplete, so the holds it carries "
            "cannot be trusted to be all of them. GC refuses to rebuild; this pool must be recreated.",
            generation, *probe.seal_attempt, top, listed_max_generation);
    }

    if (newest)
        return newest;

    /// THE VIRGIN VERDICT, and everything it rests on. `gc/state` is already known absent or
    /// unreadable (the caller's precondition); the wide listing found nothing; and one narrow probe of
    /// generation 1 -- the first generation any pool would ever mint -- also finds nothing. That is
    /// three pieces of ENUMERATION evidence and no point read, because the seal key's attempt component
    /// has no arithmetic successor to probe.
    ///
    /// NAMED RESIDUAL, and the generation-1 probe NARROWS it rather than closing it. On a pool that has
    /// been pruned, generation 1 LEGITIMATELY does not exist -- `pruneSupersededGenerations` deletes
    /// whole old generation prefixes once they age past `gc_snapshot_generations_to_keep` -- so an empty
    /// generation-1 probe proves nothing there. A total enumeration blackout on a lived-in, pruned pool
    /// therefore still reads virgin here, and grants it a clean slate with no holds. What the probe
    /// does buy is the un-pruned case: a young pool whose seals the wide listing hid is caught.
    ///
    /// No closure exists in the current key shapes, because a fold seal cannot be point-read from its
    /// generation alone. The fix is a derivable per-generation marker that CAN be point-read, and it
    /// would survive the blackout precisely because it needs no enumeration (see the report and
    /// `docs/superpowers/cas/BACKLOG.md`). Anything the listing DOES show above its own maximum is
    /// caught by the refusal above instead.
    const GenerationSealProbe genesis = probeGenerationForSeal(1);
    if (listed_anything || genesis.generation_exists)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS GC rebuild: the pool-wide enumeration of {} yielded no fold seal, yet {} -- so the "
            "pool is NOT provably new and its holds cannot be enumerated. GC refuses to rebuild; this "
            "pool must be recreated.",
            top,
            listed_anything ? "that enumeration did return objects under it"
                            : "a narrow probe of generation 1 found objects the wide listing omitted");

    ProfileEvents::increment(ProfileEvents::CASGCRebuildVirginByEnumeration);
    LOG_WARNING(logger,
        "CAS GC rebuild PROCEEDING AS NEVER-SEALED: no fold seal was found by the broad listing of {} "
        "or by the generation-1 probe, and gc/state is absent or unreadable, so NO durable hold is "
        "carried forward. IMPLICATION: if this pool HAS sealed and then pruned, generation 1 is gone "
        "legitimately and this verdict rests on a total enumeration blackout -- holds may be lost and "
        "GC may reclaim blobs a held namespace still protects. The verdict rests on ENUMERATION ALONE: "
        "no point read can prove it, because a fold seal key needs an attempt component that is a "
        "lease sequence number. Verify with the store operator that the object listing is complete "
        "before trusting this rebuild.",
        top);
    return std::nullopt;
}

Gc::GenerationSealProbe Gc::probeGenerationForSeal(uint64_t generation)
{
    Backend & backend = store->backend();
    const Layout & layout = store->layout();

    GenerationSealProbe probe;
    forEachListedKey(backend, layout.gcGenPrefix(generation), [&](const ListedKey & k)
    {
        probe.generation_exists = true;   /// ANY object proves this generation was minted
        /// Parse a candidate attempt out of the path and then PROVE it by rebuilding the key: only a
        /// string `foldSealKey` itself would have produced is a fold seal. Everything else under a
        /// generation -- run objects, outcome sets, debris of a lost era -- must not get to decide
        /// which baseline this pool's holds are read from.
        static constexpr std::string_view kAttempt = "/attempt/";
        const size_t a_begin = k.key.find(kAttempt);
        if (a_begin == String::npos)
            return;
        const size_t a_from = a_begin + kAttempt.size();
        const size_t a_end = k.key.find('/', a_from);
        if (a_end == String::npos)
            return;
        uint64_t attempt = 0;
        try
        {
            attempt = std::stoull(k.key.substr(a_from, a_end - a_from));
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            return;   /// foreign key shape is debris, not an attempt
        }
        if (layout.foldSealKey(generation, attempt) != k.key)
            return;
        if (!probe.seal_attempt || *probe.seal_attempt < attempt)
            probe.seal_attempt = attempt;
    }, 1000, onGcEnumerationPage);
    return probe;
}

uint64_t Gc::FoldResult::FrontierDeficit::total() const
{
    return checkpoint_unusable + checkpoint_frontier_empty + committed_below_cursor
        + held + probe_budget + fold_aborted + unattributed;
}

String Gc::FoldResult::FrontierDeficit::describe() const
{
    String out;
    const auto add = [&](const char * name, uint64_t count)
    {
        if (count == 0)
            return;
        if (!out.empty())
            out += ", ";
        out += fmt::format("{}={}", name, count);
    };
    add("checkpoint_unusable", checkpoint_unusable);
    add("checkpoint_frontier_empty", checkpoint_frontier_empty);
    add("committed_below_cursor", committed_below_cursor);
    add("held", held);
    add("probe_budget", probe_budget);
    add("fold_aborted", fold_aborted);
    add("unattributed", unattributed);
    return out;
}

void Gc::FoldResult::FrontierDeficit::count(FrontierUnproven reason)
{
    switch (reason)
    {
        case FrontierUnproven::Proven: return;
        case FrontierUnproven::CheckpointUnusable: ++checkpoint_unusable; return;
        case FrontierUnproven::CheckpointFrontierEmpty: ++checkpoint_frontier_empty; return;
        case FrontierUnproven::CommittedBelowCursor: ++committed_below_cursor; return;
        case FrontierUnproven::Held: ++held; return;
        case FrontierUnproven::Unattributed: ++unattributed; return;
    }
}

Gc::FoldResult Gc::fold(GcState & state, Token & /*state_token*/, RoundReport & report,
                        uint64_t current_round, const RefPlan & walk_plan, UniversePolicy policy,
                        GcRoundWorkBudget & work_budget)
{
    Backend & backend = store->backend();
    const Layout & layout = store->layout();
    FoldResult result;

    /// 1. Group the round's one enumeration of `cas/ns/stream/` (taken before the defer decision) into
    /// per-table immutable-object listings. That single enumeration serves the defer signal, the
    /// ref-log intake, and ref-object cleanup planning alike.
    ///
    /// THE ROUND LISTS THIS PREFIX ONCE. The intake does not need a second opinion about the listing
    /// because it does not consult the listing for completeness at all -- it walks by exact key from
    /// the cursor.
    ///
    /// PHASE 6/18 `fold_ref_group`: the strict regrouping -- what this round will fold, decided before it
    /// reads a single body. No I/O: the keys are already in hand.
    std::optional<GcPhaseTimer> ref_list_timer;
    ref_list_timer.emplace(phase_sink, "fold_ref_group");
    const RefScanSummary & ref_scan = walk_plan.refScan();
    const std::vector<String> & ref_object_keys = ref_scan.keys;

    /// Stage B (spec INV-3): the round's ONE catalog `GET`. `live_incarnation` names, for every
    /// namespace the catalog admits as `Live`/`Removing`, the ONE incarnation the fold may act on --
    /// `Creating` is excluded, matching `discoverUniverse` (no publication can exist yet). This is what
    /// makes the fold catalog-authoritative rather than LIST-authoritative: the pool-wide ref LIST
    /// remains the round's intra-namespace hint (what a namespace the catalog already named has
    /// listed), never the source of WHICH namespaces exist. Reused below for the catalog-only walk
    /// targets, so the round pays this GET once.
    const CasRefCatalog::Snapshot & catalog_snapshot = walk_plan.catalogCut();
    std::map<String, UInt128> live_incarnation;
    /// Final review F1: a `Creating` life IS named by the catalog -- `live_incarnation` excludes it
    /// only because it is not yet WALKABLE (spec §3, no publication can exist), not because the
    /// namespace is unaccounted. `completeCreation` publishes `_ckpt` (step 2) strictly BEFORE the
    /// `Creating -> Live` CAS (step 3), so every ordinary namespace creation has a real window --
    /// crash-stalled or merely mid-flight -- where a `Creating` entry's own `_ckpt` is durable and
    /// listed while the entry itself is absent from `live_incarnation`. R10's un-cataloged anomaly
    /// below must tell that apart from genuine "nothing in the catalog names this at all" debris, or
    /// an ordinary or stalled creation suppresses the whole round's reclamation until someone
    /// recreates the exact name and drives `reconcileStaleCreator` -- unbounded in the stalled case.
    for (const NamespaceLifeId & life : walk_plan.lives())
        live_incarnation.emplace(life.ns.string(), life.incarnation);
    /// Carry the complete cut on the result (review C3) so every later consumer this round --
    /// `cleanupRefObjects` and terminal-evidence attribution -- retains both the chosen incarnation and
    /// the lifecycle/absence distinction instead of re-reading or reducing the catalog independently.
    result.catalog_cut = catalog_snapshot;
    /// THE POSITIVE EMPTY-UNIVERSE PROOF (see the destructive gate below). `token` is guaranteed by
    /// `CasRefCatalog::read` on every operational path -- absence there is `CORRUPTED_DATA`, never an
    /// empty snapshot -- but the check stays here so this fails closed if a bootstrap/test snapshot
    /// ever reaches this line. `entries` (not `live_incarnation`, which drops `Creating`) is the right
    /// source: a catalog holding only `Creating` rows must NOT read as an empty universe, and `entries`
    /// is the one view that still carries those rows.
    result.catalog_cut_proved_empty = catalog_snapshot.token.has_value() && catalog_snapshot.catalog.entries.empty();

    /// A malformed ref-object key or namespace aborts ref folding for the whole round: the
    /// round produces no ref delta, advances no cursor, and authorizes no destructive work -- recorded as
    /// an anomaly (which drives `suppress_destructive`), never a throw that wedges the round.
    std::map<String, RefTableListing> ref_tables;
    bool ref_folding_aborted = false;
    try
    {
        const auto physical_tables = groupRefKeys(layout, ref_object_keys);
        for (const auto & [life_id, listing] : physical_tables)
        {
            const auto life = catalog_snapshot.life_index.resolve(life_id);
            if (!life)
                continue;   /// absent from the post-LIST cut: inert dead-life debris
            const auto live_it = live_incarnation.find(life->ns.string());
            if (live_it != live_incarnation.end() && live_it->second == life_id)
                ref_tables.emplace(life->ns.string(), listing);
        }
    }
    catch (const Exception & e)
    {
        ref_folding_aborted = true;
        report.recordAnomaly(RootNamespace{}, 0, ManifestId{},
                             "malformed ref-object key: ref folding aborted this round");
        LOG_WARNING(logger,
                    "CAS GC ref intake: {} -- aborting ref folding for the round", e.message());
    }
    for (const auto & [ns_str, listing] : ref_tables)
        result.root_shards.emplace_back(RootNamespace{ns_str}, 0);

    ref_list_timer->metric("ref_keys_listed", ref_object_keys.size());
    ref_list_timer->metric("namespaces_seen", ref_tables.size());
    /// The round's only remaining whole-round ref abort: a key attributable to no namespace. Reported on
    /// every round, healthy or not — a column that is always 0 is what makes the one round where it is
    /// not stand out.
    ref_list_timer->metric("ref_folding_aborted", ref_folding_aborted ? 1 : 0);
    ref_list_timer.reset();   /// emits the `fold_ref_group` row

    /// Parent cursors — the per-(ns,shard) cursors a prior round sealed. `listRefPrefix` read them from
    /// the fold seal at the adopted (snap_generation, snap_attempt) before it built this round's one
    /// walk plan (the fold seal IS the coverage record).
    /// Absent => fresh pool (cursor 0). A folded event must never be re-folded from 0 (that double-counts
    /// blob in-degree => silent over-pin/leak).
    /// A live `gc/state` whose adopted
    /// fold seal OBJECT is MISSING is corrupt bookkeeping, never an empty baseline — treating it as
    /// empty would re-fold only journal tails and mass-condemn everything the lost snapshot
    /// protected. NOTE the distinction from a PRESENT seal with empty `ref_lives` (a legitimate
    /// empty-universe generation) — the audit keys on object absence, not coverage emptiness.
    ///
    /// PHASE 7/18 `fold_seal_read`. The scope reaches down to `discover_ref_seal` below, because that is
    /// a SECOND GET of the SAME key at the SAME (generation, attempt) -- the two reads belong on one row
    /// or the redundancy is invisible. Everything between them is I/O-free (a `resize`, three lambda
    /// DEFINITIONS, and plain assignments), so the duration is honestly the two GETs and their decodes.
    /// Instrumented, NOT fixed: removing the second read is a behaviour change the follow-up study
    /// decides, and the `redundant_reads` metric is the evidence it will need.
    std::optional<GcPhaseTimer> seal_read_timer;
    seal_read_timer.emplace(phase_sink, "fold_seal_read");
    const std::optional<CasFoldSeal> adopted_seal = readFoldSeal(state.snap_generation, state.snap_attempt);
    if (!adopted_seal && state.snap_generation > 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS GC: the adopted fold seal (generation {}, attempt {}) is missing under a live "
            "gc/state — GC bookkeeping is corrupt. GC refuses to run; recover with "
            "SYSTEM CAS GC REBUILD.",
            state.snap_generation, state.snap_attempt);
    /// Every fold decision below starts from the immutable plan, never from the raw scan that supplied
    /// it. The successor seal owns its mutable copy; this separate const snapshot remains the prior
    /// coverage/hold view while the successor earns changes later in this fold.
    const std::map<UInt128, RefLifeFoldState> parent_ref_lives = walk_plan.parentFoldStates();
    const uint64_t dropped_parent_ref_lives = walk_plan.droppedParentRows();
    result.fold_seal.ref_lives = walk_plan.successorFoldStates();

    /// Retired-in-snapshot: the prior generation's condemned entries RIDE the source-edge run as
    /// `kCondemned` sentinel rows, so the round no longer reads any separate retired-list object —
    /// the parent seal's `blob_target_runs` ARE the retired input. The per-gc-shard `condemned_summary`
    /// the seal carries below is distilled from the `still_retired` rows each shard re-emits, making the
    /// next round's `graduationDue` / pure-carry decisions zero-I/O.
    const uint64_t condemn_round = state.round + 1;
    result.retired_merge.resize(state.gc_shards);

    /// Condemn-time observation: ONE HEAD per new zero-transition captures the exact incarnation token
    /// the eventual delete carries (absent => a prior landed delete => nothing to condemn). Emits the
    /// Candidate trail (IndegZero / GcRetireObserve / BlobRetire) exactly where the decision is made.
    const auto head_blob = [&](const BlobRef & ref) -> std::optional<HeadResult>
    {
        EventEmitter{*store}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::IndegZero;
            e.object_kind = CasEventObjectKind::Blob;
            e.object_hash = blobIdOf(ref);
            e.round = condemn_round;
            e.gen = state.snap_generation + 1;
            e.reason = "last folded owner edge dropped; in-degree reached 0";
        });
        const HeadResult observed = backend.head(layout.blobKey(ref));
        EventEmitter{*store}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::GcRetireObserve;
            e.object_kind = CasEventObjectKind::Blob;
            e.object_hash = blobIdOf(ref);
            e.token = observed.exists ? observed.token.value : "";
            e.round = condemn_round;
            e.gen = state.snap_generation + 1;
            e.outcome = observed.exists ? "present" : "absent";
            e.reason = "zero-in-degree candidate; HEAD-observe the current token";
        });
        if (!observed.exists)
            return std::nullopt;
        ++report.candidates;
        ++report.condemned;
        ProfileEvents::increment(ProfileEvents::CASGCRetiredCondemned);
        EventEmitter{*store}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::BlobRetire;
            e.object_kind = CasEventObjectKind::Blob;
            e.object_hash = blobIdOf(ref);
            e.token = observed.token.value;
            e.round = condemn_round;
            e.gen = state.snap_generation + 1;
            e.outcome = "retired";
            e.reason = "condemned zero-in-degree candidate; entering the current retired list";
        });
        HeadResult adjusted = observed;
        adjusted.size = retiredLogicalSize(ObjectKind::Blob, observed.size, store->poolMeta().blob_header_len);
        /// This candidate unconditionally becomes a fresh `RetiredEntry` in `closeBlob` (the ONLY
        /// caller of `head_blob`) whenever this lambda returns a value — so this is exactly the round's
        /// side-effecting condemn site. Write the meta Condemned so the writer's point-read gate
        /// sees it; a successful write records the in-process (hash, token) confirmation the graduation
        /// gate consumes (`scheduleCondemnMarkerWrite` captures everything BY VALUE — never by reference
        /// to `cur_blob`, which the fold's tight streaming loop mutates while the job is queued).
        scheduleCondemnMarkerWrite(ref, observed.token, condemn_round, adjusted.size);
        return adjusted;
    };

    /// Side-effect-free peek: the fold's republication-supersede branch (inside
    /// `foldDeltasIntoGeneration`) needs the CURRENT token to detect that republication replaced a stale
    /// retired entry, but must NOT emit the fresh-condemn trail or bump `CASGCRetiredCondemned` — that
    /// hook is `head_blob` above, reserved for a genuinely NEW zero-in-degree candidate. A supersede's
    /// own event is `blob_retire_replaced`, emitted once below from `merge.replaced`. Plain HEAD, no
    /// events, no counters.
    const auto peek_head = [&](const BlobRef & ref) -> std::optional<HeadResult>
    {
        HeadResult hr = backend.head(layout.blobKey(ref));
        if (!hr.exists)
            return std::nullopt;
        hr.size = retiredLogicalSize(ObjectKind::Blob, hr.size, store->poolMeta().blob_header_len);
        return hr;
    };

    /// Graduation gate (triage 2026-07-17 §3.4): the merge consults this before publishing an entry
    /// delete_pending. Confirmation sources, in order: the in-process (hash, token) record left by a
    /// successful `writeCondemnedMeta` completion, then ONE synchronous `loadMeta` re-check — a durable
    /// `Condemned` meta observed NOW is sufficient evidence, because a writer that same-token adopted
    /// must have observed a non-Condemned meta EARLIER, its edge (EDGE-BEFORE-OBSERVE) landed before the
    /// meta turned Condemned, and the redelete only fires from a LATER fold whose cut postdates this
    /// round — that fold sees the edge and spares. (`BlobMeta` carries no token, so the re-check is
    /// per-hash by design; the two-phase pipeline + the exact-token delete carry the rest.) No durable
    /// evidence => count the carry, RETRY the marker write (liveness: a swallowed write would otherwise
    /// carry forever), and refuse — never throw (an unreadable meta is missing evidence, not a wedge).
    const auto confirm_condemned_marker = [&](const RetiredEntry & entry) -> bool
    {
        if (condemnMarkerConfirmedInProcess(entry.ref, entry.token))
            return true;
        try
        {
            if (const auto lm = loadMeta(backend, layout, entry.ref); lm && lm->meta.state == MetaState::Condemned)
            {
                noteCondemnMarkerDurable(entry.ref, entry.token);   /// memoize for a round-CAS-abort replay
                return true;
            }
        }
        catch (...)
        {
            ProfileEvents::increment(ProfileEvents::CASGCMetaWriteAnomaly);
            tryLogCurrentException(logger,
                "CAS gc: condemn-marker re-check failed to read the meta (treated as missing evidence; "
                "the entry is carried, never wedges the round)");
        }
        ProfileEvents::increment(ProfileEvents::CASGCCondemnMarkerUnconfirmedCarry);
        /// Accepted race: this retry writes `Condemned` per-hash, with no token check. If a writer
        /// resurrected this exact hash under a FRESH token between the original swallowed write and this
        /// retry, the retry stamps `Condemned` over that writer's live, uncondemned incarnation. This is
        /// never destructive -- the eventual exact-token delete is a no-op against the fresh token
        /// (`DeleteOutcome::TokenMismatch`/`NotFound`) -- worst case the resurrecting writer's later
        /// same-token adopter sees stale `Condemned` metadata and republishes once unnecessarily.
        scheduleCondemnMarkerWrite(entry.ref, entry.token, entry.condemn_round, entry.size);
        return false;
    };

    const uint64_t new_generation = state.snap_generation + 1;
    /// The fold mints THIS round's attempt id from `lease.seq` (the renew/steal paths bump it every
    /// round, so it is a fresh monotonic per-round id). EVERY fold-artifact WRITE below lands under this
    /// attempt; the PARENT-generation READS keep using `state.snap_attempt` (the attempt the prior round
    /// adopted). The fold-adopt CAS #1 then commits `(new_generation, attempt)` together — a deposed
    /// leader's fold lands under its own unadopted attempt and is invisible to every reader.
    const uint64_t attempt = state.lease.seq;
    result.fold_seal.generation = new_generation;
    result.fold_seal.parent_generation = state.snap_generation;

    std::vector<BlobDelta> deltas;
    /// PROBE B2's round-local ledger (see `TxnApplyLedger`). Grows one entry per ref log the intake
    /// opens; the reducers mark `applied` through `&ledger.applied` at the point they consume a delta.
    TxnApplyLedger ledger;
    bool folded_any = false;

    /// PROBE B1 -- intake-layer identity. `logs_applied` counts, at the SINGLE cursor-advance site
    /// below, every log whose whole body folded. At seal time it is compared against a recomputation
    /// from the sealed coverage and the listing. The two are derived differently (a running counter vs
    /// a recomputation), so a control-flow bug that advances a cursor without folding breaks the
    /// equality.
    ///
    /// REACH, stated so this is not over-trusted: the recomputation reads the SAME listing the intake
    /// read, so B1 is BLIND to a record missing from that listing. It is a control-flow assertion, not
    /// a detector for the skipped-transaction defect -- probe A covers the listing, probe B2 covers
    /// everything below the intake.
    uint64_t logs_applied = 0;

    /// The adopted parent seal, read once at the ADOPTED (snap_generation, snap_attempt): it carries the
    /// parent generation's `blob_target_runs` (resolved below into per-gc-shard prior runs) and the parent
    /// `condemned_summary` (the pure-carry decision). A completed round leaves its fold seal there; a fresh
    /// pool has none (empty seal). Under the snapshot+log ref model there is no per-shard token-diff Skip:
    /// the "did this table change" signal is simply whether the global LIST returned any log id above the
    /// table's durable cursor, which the per-table loop below tests directly.
    /// SECOND read of the key `adopted_seal` already holds: same generation, same attempt, same bytes
    /// (nothing between the two touches `state.snap_generation` / `snap_attempt` or writes that key).
    /// One redundant GET per folding round, and the round's FIFTH GET of this one key overall --
    /// `graduationDue` and `listRefPrefix` make two in `defer_decision`, `parent_seal_read` a third,
    /// `adopted_seal` above a fourth. Recorded on this row, not fixed here.
    CasFoldSeal discover_ref_seal;
    if (const auto fold_seal = readFoldSeal(state.snap_generation, state.snap_attempt))
        discover_ref_seal = *fold_seal;
    seal_read_timer->metric("seal_reads", 2);
    seal_read_timer->metric("redundant_reads", 1);
    seal_read_timer->metric("parent_ref_lives", parent_ref_lives.size());
    seal_read_timer->metric("dropped_parent_ref_lives", dropped_parent_ref_lives);
    seal_read_timer->metric("parent_runs", discover_ref_seal.blob_target_runs.size());
    seal_read_timer->metric("parent_cleanup_evidence", std::count_if(
        discover_ref_seal.ref_lives.begin(), discover_ref_seal.ref_lives.end(),
        [](const auto & row) { return row.second.cleanup_evidence.has_value(); }));
    seal_read_timer.reset();   /// emits the `fold_seal_read` row

    /// Each fully-folded `remove_namespace` transaction earns terminal cleanup evidence.
    std::vector<std::pair<RootNamespace, RefTxnId>> new_removals;

    /// 2-3. Ref-log intake, by ARITHMETIC (spec §5). For each table the walk steps
    /// `expected = cursor + 1` WITHIN the cursor's epoch and reads that exact key: under INV-1 the ids of
    /// one `(namespace, writer_epoch)` are dense `1..T`, so the next record's id is computable and the
    /// round never asks the listing what to read. Each body is decoded+validated and every explicit
    /// owner-change folds into `foldManifestEdges` (which reads the manifest body and appends per-blob
    /// `BlobDelta`s to `deltas`). The durable cursor advances per FULLY folded log; a missing manifest body
    /// clamps this table below the log (barrier, re-read next round) while other tables keep folding.
    ///
    /// THE LISTING IS A HINT, and demoting it is the point of this loop. It used to be the source of
    /// truth for which records exist, so a store that omitted a durable key from an enumeration -- the
    /// observed `0x1430c`/`0x1430d` shape -- made the round skip those records' owner edges and then seal
    /// a cursor ABOVE them, which is unrecoverable (a record below the cursor is never re-read). Under
    /// arithmetic intake such an omission is a NON-EVENT: the exact GET finds the record anyway. The hint
    /// keeps exactly two jobs here:
    ///   (a) the genesis start of a never-folded namespace (`cursor == {0,0}` has no arithmetic
    ///       predecessor; Stage B's `_ckpt.life_epoch` is what finally supplies it -- see below), and
    ///   (b) the WITNESS set that makes an absent expected-next decidable:
    ///         absent, no listed id above it  => this namespace's frontier this round (normal end);
    ///         absent, a listed id above it   => impossible under contiguity, so the store is lying or a
    ///                                           durable record was lost: HOLD the namespace at
    ///                                           classification 4 with its cursor unmoved.
    ///
    /// Epochs are crossed only over a consumed `EpochSeal` (INV-2): the seal folds as an applied table
    /// no-op (probe B2 `produced=false`) and the next epoch's start is `{E', 1}`, reached through the
    /// `prev_epoch_seal` back-chain rather than guessed -- so an epoch the hint omits entirely is still
    /// walked, and a crossing with no consumed seal behind it is an impossible shape that holds. Read
    /// `crossFromSeal` for what that is proved FROM: within a round the seal's kind is checked outright,
    /// across rounds it rests on the chain until Task 8 carries the kind in the durable cursor.
    ///
    /// PER-NAMESPACE FAILURES ARE PER-NAMESPACE (spec §5): an unreadable or undecodable body belongs to
    /// exactly one namespace and clamps only it. The whole-round abort survives ONLY for a key that cannot
    /// be attributed to any namespace at all (`groupRefKeys` above), which is why nothing in this loop
    /// sets `ref_folding_aborted` anymore.
    ///
    /// PHASE 8/18 `fold_ref_intake`: one GET per record (always owed -- the round read every body anyway),
    /// plus one exact 404 probe per namespace to prove its frontier, one extra GET per epoch crossed, and
    /// one GET per manifest edge, which on a busy pool is where the round's object-read budget goes.
    /// It also carries probe B1's two numbers -- reported on EVERY healthy round, so
    /// "logs_accounted always equals logs_applied" becomes an observable property of the table rather than
    /// a claim in a comment.
    std::optional<GcPhaseTimer> intake_timer;
    intake_timer.emplace(phase_sink, "fold_ref_intake");
    uint64_t intake_tables_changed = 0;
    uint64_t intake_tables_clamped = 0;
    uint64_t intake_tables_held = 0;
    uint64_t intake_dead_precommits_skipped = 0;
    uint64_t intake_absent_probes = 0;
    uint64_t intake_epoch_crossings = 0;

    /// Probe B1's raw material: per namespace, the CONTIGUOUS runs of ids this round walked, one per epoch
    /// entered, recorded as `[first, last]`. The recomputation below turns them back into a count and
    /// compares it with the counter the advance site incremented -- so a cursor that moved over a position
    /// nothing applied inflates the first number and fails the round closed.
    std::map<String, std::vector<std::pair<RefTxnId, RefTxnId>>> walked_segments;

    /// The round's SECOND witness source, independent of the listing -- see `readCheckpointWitnesses`
    /// for what it decides and why a listing alone cannot decide it. Its `undecodable` half names the
    /// namespaces whose `_ckpt` is present and unreadable; each of those is HELD below, and only those.
    const CheckpointWitnesses checkpoints = readCheckpointWitnesses(ref_tables, catalog_snapshot);
    const std::map<String, RefTxnId> & checkpoint_witness = checkpoints.witnesses;

    /// WHICH NAMESPACES THIS ROUND WALKS -- i.e. THE ROUND'S UNIVERSE, the set the destructive gate owes
    /// a frontier proof for (spec §5; `UniversePolicy` for why only the catalog can bound it): EVERY
    /// `Live`/`Removing` row of this round's frozen catalog cut, and nothing else.
    ///
    /// The listing is a HINT and cannot change that set in either direction. It cannot SHRINK it -- a
    /// store that goes quiet about a namespace, or stops listing one to clear its hold, leaves the
    /// catalog row and therefore the obligation untouched -- and it cannot GROW it either, since a
    /// physical id the catalog does not name is inert debris rather than a namespace to prove.
    ///
    /// A namespace with no hint entry walks against an EMPTY listing: it has no hint witnesses, but it
    /// still reads its expected-next by exact key -- ONE `GET`, whose absence IS the frontier proof and
    /// whose PRESENCE means the namespace was wrongly quiet and gets walked properly this round. A
    /// carried hold additionally supplies the witness that keeps an absent below its position from
    /// reading as a frontier.
    ///
    /// COST: held namespaces are always walked (their retry is a liveness obligation, not a budgeted
    /// nicety); the merely-QUIET ones are bounded by `gc_frontier_probe_budget`, and the ones the budget
    /// does not reach are simply unproven, which suppresses the round's destruction. In a healthy pool
    /// that budget is rarely touched; dead physical lives are excluded by the catalog-built plan.

    /// THE ROUND'S WORK-SET IS FROZEN AT ROUND START, AND THAT IS WHAT MAKES A ROUND END.
    ///
    /// Arithmetic intake reads the next id by exact key, so on its own it walks WHILE RECORDS EXIST --
    /// and a namespace whose writer appends concurrently therefore has no last record to reach. Round
    /// time stopped being `backlog / walker_rate` and became `backlog / (walker_rate - writer_rate)`,
    /// which diverges the moment a writer keeps up with the walker. Measured: zero completed rounds in
    /// 42 minutes on a hot pool. Everything the round paces on rounds then stops too -- the fold seal and
    /// its cursors, the sampled store-quality detector, ref-object cleanup -- so the backlog the round
    /// was falling behind on grows without bound.
    ///
    /// The bound is `_ckpt.committed_through`, snapshotted once per namespace before the walk (see
    /// `readCheckpointWitnesses`) and never re-read within the round: the ceiling test at the top of the
    /// walk refuses every position above it, so the work is fixed at round start whatever the writer
    /// does meanwhile. It is also the AUTHORITY ceiling -- a record above it is durable but is not
    /// logical history yet -- so ONE comparison serves both purposes and there is no second bound that
    /// could drift out of agreement with it. NO NEW PERSISTED STATE: the number is read from an object
    /// the fold reads anyway.
    ///
    /// THE BOUND IS ON FOLDING, NOT ON READING, and that distinction is the whole design.
    ///
    /// The walk still reads its expected-next exactly as before -- one exact `GET` at `cursor + 1` --
    /// because that read IS the frontier proof, and a namespace that stops being read stops being
    /// provable. An unprovable namespace leaves `frontier_complete` false forever, which suppresses every
    /// destructive decision, which stops ref-object cleanup, which means its listing never drains and it
    /// never becomes provable by any other route either. So "skip reading a quiet namespace" is not a
    /// cheaper version of this design; it is a GC that permanently reclaims nothing. The saving it
    /// appears to offer is exactly one `GET` per namespace per round, and that `GET` is the proof.
    ///
    /// The listed tail bounds nothing, but it is still CLASSIFIED for the phase row, in three classes:
    ///   * `tail > cursor` -- the round has records to fold;
    ///   * `tail == cursor` -- the listing shows nothing new, so the walk folds nothing and its single
    ///     read is the frontier probe. On a wide pool this is most namespaces, most rounds;
    ///   * `tail < cursor` -- the listing's greatest id is BELOW a cursor we folded through. Cleanup
    ///     deletes logs from the bottom up, so a listed log below the cursor with none at or above it is
    ///     a stale or lying listing, not a shape cleanup produces. The cursor is the truth (the sampled
    ///     store-quality detector is this class's observer).
    struct WalkTarget
    {
        String ns;
        UInt128 life_id{};
        const RefTableListing * listing;
    };

    static const RefTableListing kNoListing;
    std::vector<WalkTarget> walk_targets;
    /// The three buckets classify the hinted, UNHELD namespaces -- the ones whose tail comparison
    /// decided how much they could fold. A held namespace folds up to a bound its HOLD also determines
    /// and is reported by `tables_held`, so counting it here would make `tails_unchanged` stop meaning
    /// "namespaces with no work this round", which is the number the row exists to publish.
    uint64_t intake_tails_advanced = 0;
    uint64_t intake_tails_unchanged = 0;
    uint64_t intake_tails_below_cursor = 0;
    uint64_t intake_unhinted_held = 0;
    uint64_t intake_unhinted_quiet = 0;
    uint64_t frontier_probe_budget = store->poolConfig().gc_frontier_probe_budget;
    uint64_t intake_unprobed_budget = 0;
    uint64_t intake_catalog_only = 0;
    for (const auto & [catalog_ns_str, catalog_incarnation] : live_incarnation)
    {
        const auto listing_it = ref_tables.find(catalog_ns_str);
        const RefTableListing * listing = listing_it != ref_tables.end() ? &listing_it->second : &kNoListing;
        const auto parent_it = parent_ref_lives.find(catalog_incarnation);
        const RefCoverage * parent_cov = parent_it != parent_ref_lives.end() ? &parent_it->second.coverage : nullptr;

        if (listing_it == ref_tables.end() && parent_cov)
        {
            if (parent_cov->hold)
                ++intake_unhinted_held;
            else if (checkpoints.recovery_checkpoints.contains(catalog_ns_str))
                ++intake_unhinted_quiet;
            else if (frontier_probe_budget == 0)
            {
                ++intake_unprobed_budget;
                continue;
            }
            else
            {
                --frontier_probe_budget;
                ++intake_unhinted_quiet;
            }
        }
        else if (listing_it == ref_tables.end())
            ++intake_catalog_only;

        walk_targets.push_back({catalog_ns_str, catalog_incarnation, listing});
        if (listing->logs.empty() || (parent_cov && parent_cov->hold))
            continue;

        const RefTxnId tail = listing->logs.back();
        const RefTxnId cursor = parent_cov ? parent_cov->last_folded_ref_id : RefTxnId{};
        if (cursor < tail)
            ++intake_tails_advanced;
        else if (cursor == tail)
            ++intake_tails_unchanged;
        else
            ++intake_tails_below_cursor;
    }

    for (const WalkTarget & target : walk_targets)
    {
        const String & ns_str = target.ns;
        const RefTableListing & listing = *target.listing;
        if (ref_folding_aborted)
            break;
        const RootNamespace ns{ns_str};
        /// Every walk target came out of this round's own catalog read, so its incarnation is the REAL
        /// one and the life below is minted from a catalog entry rather than guessed from a key.
        const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, target.life_id);

        /// Parent cursor = the durable last_folded_ref_id this table folded to (absent => {0,0}).
        ///
        /// Id ordering does NOT subsume remove+recreate: under INV-1 (`nextRefTxnId`) ids are derived per
        /// namespace from that table's own state, so a namespace removed and recreated within one writer
        /// epoch restarts at `{E, 1}`, at or below a cursor sealed for the PREVIOUS life -- and the walk
        /// starts at `cursor + 1`, so those edges would never fold and the recreated refs' manifests
        /// would look unreferenced. That is closed STRUCTURALLY, not by comparing ids: this map and the
        /// walk targets are both keyed by CATALOG INCARNATION, so a rebirth is a different key and
        /// inherits no cursor, and a life the catalog no longer names contributes no walk target and
        /// therefore re-carries no cursor into the new seal. Do not "fix" it by comparing ids.
        const auto cursor_it = parent_ref_lives.find(target.life_id);
        const RefTxnId cursor = cursor_it != parent_ref_lives.end()
            ? cursor_it->second.coverage.last_folded_ref_id : RefTxnId{};

        /// Baseline guard: a table with NO sealed cursor whose logs at/below its
        /// newest snapshot have all been cleaned means a prior fold advanced+cleaned them and then gc/state
        /// was lost -- folding from {0,0} would miss those edges and mass-condemn their blobs. Fail closed;
        /// recover with the explicit rebuild. A fresh table (writer already snapshotted, logs still present)
        /// passes because its logs at or below the snapshot survive.
        if (cursor_it == parent_ref_lives.end() && !listing.snapshots.empty()
            && !checkpoints.recovery_checkpoints.contains(ns_str))
        {
            const RefTxnId newest_snapshot = listing.snapshots.back();
            const bool logs_below_snapshot_gone = listing.logs.empty() || newest_snapshot < listing.logs.front();
            if (logs_below_snapshot_gone)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS GC baseline guard: table {} has snapshot {} but no surviving log at or below it and "
                    "no sealed fold cursor -- gc/state was lost after cleaning covered logs. GC refuses to "
                    "run; recover with SYSTEM CAS GC REBUILD.",
                    ns_str, renderRefTxnId(newest_snapshot));
        }

        /// The hold the PARENT seal left on this namespace, if any. It is three things at once: the
        /// position this round must retry by exact key, a durable witness (see `witnessAbove`), and the
        /// hold that rides forward unless this round resolves that position.
        const std::optional<RefHold> carried_hold =
            cursor_it != parent_ref_lives.end() ? cursor_it->second.coverage.hold : std::nullopt;

        RefCoverage cov;
        cov.classification = 0;
        bool table_changed = false;
        /// THE FRONTIER PROOF for this namespace, and there is exactly one thing that establishes it:
        /// the walk read the expected-next position by exact key, found it ABSENT, and no witness put
        /// anything above it. That is the honest end of the record stream. Every other way out of the
        /// loop leaves the namespace unproven -- a hold (this round's or a carried one), and the walk
        /// that never started because a never-folded namespace's hint offered no genesis position AND
        /// no `_ckpt.life_epoch` was on record either (the `expected` initialization below reads
        /// `checkpoints.life_epochs` for exactly this case; a namespace with neither has a genuinely
        /// unknown genesis, so no probe is taken, nothing is proved, and fail-closed says unproven).
        bool frontier_proven = false;
        /// Which exit left this namespace unproven, for the round's deficit tally. It starts at
        /// `Unattributed` so that an exit which forgets to name itself is reported as such instead of
        /// being silently absorbed into some other bucket.
        FoldResult::FrontierUnproven unproven_reason = FoldResult::FrontierUnproven::Unattributed;

        /// Use the same frozen catalog row + decoded checkpoint authority as read-only recovery. The
        /// ref LIST is only a bounded-work hint; it may neither choose this life's genesis nor extend
        /// its committed frontier.
        const auto catalog_entry_it = std::lower_bound(
            catalog_snapshot.catalog.entries.begin(), catalog_snapshot.catalog.entries.end(), ns,
            [](const CatalogEntry & entry, const RootNamespace & needle) { return entry.ns < needle; });
        chassert(catalog_entry_it != catalog_snapshot.catalog.entries.end());
        chassert(catalog_entry_it->ns == ns);
        chassert(catalog_entry_it->incarnation == target.life_id);
        std::optional<RefCkpt> checkpoint;
        if (const auto checkpoint_it = checkpoints.recovery_checkpoints.find(ns_str);
            checkpoint_it != checkpoints.recovery_checkpoints.end())
            checkpoint = checkpoint_it->second;

        std::optional<RecoveryGrounding> grounding;
        String checkpoint_failure;
        if (const auto bad_checkpoint = checkpoints.undecodable.find(ns_str);
            bad_checkpoint != checkpoints.undecodable.end())
        {
            checkpoint_failure = bad_checkpoint->second;
        }
        else
        {
            try
            {
                grounding = chooseRecoveryGrounding(std::optional<CatalogEntry>{*catalog_entry_it}, checkpoint);
            }
            catch (const Exception & e)
            {
                checkpoint_failure = e.message();
            }
        }

        /// The hold THIS round detected, at the position it stopped. It IS the clamp signal: there is
        /// no separate boolean that could disagree with it about whether the namespace stopped.
        std::optional<RefHold> fired;
        RefTxnId resolved_through = cursor;   /// advances per fully-folded log; a clamp keeps it below the log

        /// The witness role: the smallest id strictly above `id` that SOMETHING says exists. An absent
        /// expected-next means "frontier" without a witness and "impossible shape" with one, so the
        /// witness set decides whether the walk stops quietly or holds. THREE independent sources, none
        /// of them authoritative alone:
        ///   * the hint, which may omit durable records (that is the whole reason intake is arithmetic);
        ///   * `_ckpt.checkpoint`, the namespace's own durable tail -- a listing is a SNAPSHOT, so a
        ///     record durable after the enumeration is invisible to that round's probes, and this one
        ///     is not (phase-0 model `_fix_ckptwitness`);
        ///   * the CARRIED HOLD's offending position, which is durable proof that a previous round
        ///     reached that position -- with ONE exception, `CheckpointUndecodable`, the only hold
        ///     minted before the walk reads anything: its position is the walk's OWN next position, so
        ///     the strict comparison below never lets it witness against a position this walk goes on
        ///     to read. It weakens nothing, because a hold that proves nothing also claims nothing.
        ///     Under contiguity everything at or below a REACHED position must exist, so an absent
        ///     below it is a gap. This is what makes "retry the exact offending position" work
        ///     for a hold that sits above an epoch boundary: the crossing needs a witness to aim at,
        ///     and without this the walk would stop one position short and never re-read it.
        /// The SMALLEST wins, because the nearest witness is the one that decides same-epoch gap versus
        /// epoch crossing.
        const auto witnessAbove = [&](const RefTxnId & id) -> std::optional<RefTxnId>
        {
            std::optional<RefTxnId> nearest;
            const auto consider = [&](const RefTxnId & w)
            {
                if (id < w && (!nearest || w < *nearest))
                    nearest = w;
            };
            const auto it = std::upper_bound(listing.logs.begin(), listing.logs.end(), id);
            if (it != listing.logs.end())
                consider(*it);
            if (const auto ck = checkpoint_witness.find(ns_str); ck != checkpoint_witness.end())
                consider(ck->second);
            if (grounding && grounding->committed_through)
                consider(*grounding->committed_through);
            if (carried_hold)
                consider(carried_hold->offending_position);
            return nearest;
        };

        /// Record the position an impossible shape was detected at, hold the namespace, and stop walking
        /// it. The reason is a BOUNDED enum because it is persisted in the seal: an operator reading a
        /// held row learns what stopped the namespace and exactly where, without correlating logs.
        const auto hold = [&](const RefTxnId & at, HoldReason reason, const char * message)
        {
            report.recordAnomaly(ns, 0, ManifestId{ns, {}}, message);
            EventEmitter{*store}.emit([&](CasEvent & ev)
            {
                ev.type = CasEventType::GcFoldClamp;
                ev.namespace_ = ns_str;
                ev.object_kind = CasEventObjectKind::Root;
                ev.outcome = "held";
                ev.reason = message;
                ev.detail = {{"expected", renderRefTxnId(at)},
                             {"resolved_through",
                              resolved_through == RefTxnId{} ? "none" : renderRefTxnId(resolved_through)}};
            });
            LOG_ERROR(logger,
                "CAS GC ref intake: namespace {} HELD at {} -- {}. The cursor stays at {} and this "
                "namespace folds nothing further this round.",
                ns_str, renderRefTxnId(at), message,
                resolved_through == RefTxnId{} ? "none" : renderRefTxnId(resolved_through));
            fired = RefHold{.reason = reason, .offending_position = at,
                            .retry_count = 0, .next_retry_round = 0};   /// retry fields filled in at the seal below
        };

        /// Cross into the epoch that follows the one `from_seal` closed. The rule itself is
        /// `crossEpochFromSeal` (Pool/CasRefProtocol.cpp) -- read its doc comment for what the back-chain
        /// proves and why the hint may only NOMINATE the target epoch. It lives there rather than here
        /// because fsck's audit walks the same streams: if the round and the audit could disagree about
        /// when an epoch boundary is proved, they would disagree about which records a cut contains.
        ///
        /// This wrapper adds only the round's own accounting -- the reads the crossing performed land on
        /// the row that spent them, and an undecodable epoch-start record is logged with the key.
        const auto crossFromSeal = [&](const RefTxnId & from_seal, const std::optional<bool> & seal_proven,
                                       const RefTxnId & witness) -> std::optional<RefTxnId>
        {
            const EpochCrossResult crossing =
                crossEpochFromSeal(backend, layout, ns, from_seal, seal_proven, witness, life);
            intake_absent_probes += crossing.absent_probes;   /// a failed crossing pays its reads too
            ProfileEvents::increment(ProfileEvents::CASRefLogBodyGets, crossing.body_gets);
            if (crossing.outcome == EpochCrossOutcome::StartInvalid)
                LOG_WARNING(logger, "CAS GC ref intake: epoch-start log {} invalid: {}",
                            layout.refLogKey(life, crossing.probed), crossing.detail);
            return crossing.proved() ? std::optional<RefTxnId>(crossing.start) : std::nullopt;
        };

        /// The first position is arithmetic from the sealed cursor, or from the exact checkpoint's life
        /// epoch on the first fold. A listing never chooses a logical history start. An unusable
        /// checkpoint still has a canonical retry position once a cursor exists, so its durable hold
        /// remains observable rather than degrading into an anonymous anomaly.
        std::optional<RefTxnId> expected;
        if (cursor != RefTxnId{})
            expected = RefTxnId{cursor.writer_epoch, cursor.ref_sequence + 1};
        else if (grounding)
        {
            expected = RefTxnId{*checkpoint->life_epoch, 1};
        }

        /// An unavailable or invalid `_ckpt` quarantines its own namespace and nothing else (spec §5). The object
        /// belongs to exactly one namespace and both of its consumers are keyed by that namespace --
        /// `witnessAbove` above, and `cleanupRefObjects`' delete boundaries via `result.checkpoints` --
        /// so the damage is confinable, and confining it is the difference between one namespace waiting
        /// for a repair and the whole pool's cursors, seals and cleanup stopping on one 4 KiB object.
        ///
        /// FOLD NOTHING FOR IT, rather than fold and merely refuse the frontier proof. The cursor
        /// advance is the one irreversible thing the walk does (nothing ever re-reads below it), and
        /// this namespace has just proved that a piece of its own durable state cannot be read. Spending
        /// that irreversible step against a namespace in that condition buys reclamation latency and
        /// costs recovery options; the hold costs only the latency.
        ///
        /// The hold sits at the position the walk WOULD have read, which is canonical by construction.
        /// A namespace with no such position is a never-folded one whose listing shows no log -- the
        /// `_ckpt`-only "phantom table" `parseRefCkptKey` deliberately admits is exactly this shape --
        /// and there is no walk to stop; the anomaly alone carries it, because the other consumer is
        /// `cleanupRefObjects`, whose boundaries an ABSENT checkpoint WIDENS, and only the round's
        /// destructive gate keeps the snapshot this unreadable object names from being deleted.
        if (!grounding)
        {
            LOG_WARNING(logger,
                "CAS GC ref intake: namespace {} has no usable checkpoint -- {}. This namespace "
                "folds nothing and reclaims nothing until that object is repaired; every other "
                "namespace folds normally.",
                ns_str, checkpoint_failure);
            if (expected)
                hold(*expected, HoldReason::CheckpointUndecodable,
                     "ref intake: the namespace's `_ckpt` is unavailable or invalid -- its durable "
                     "checkpoint authority cannot be read, so nothing above the cursor is accountable");
            else
                report.recordAnomaly(ns, 0, ManifestId{ns, {}},
                    "ref intake: the catalog life has no usable `_ckpt` (no authoritative walk position)");
            unproven_reason = FoldResult::FrontierUnproven::CheckpointUnusable;
            expected.reset();   /// the cursor rides verbatim into this round's seal
        }

        if (grounding && !grounding->committed_through)
        {
            if (cursor == RefTxnId{})
                frontier_proven = true;
            else
            {
                report.recordAnomaly(ns, 0, ManifestId{ns, {}},
                    "ref intake: an empty checkpoint frontier cannot explain a nonzero sealed cursor");
                unproven_reason = FoldResult::FrontierUnproven::CheckpointFrontierEmpty;
            }
            expected.reset();
        }

        /// Whether the record this round last applied (the one `resolved_through` names) was an
        /// `EpochSeal`. `nullopt` = this round has applied nothing yet, so `resolved_through` is still
        /// the inherited cursor and its kind is not knowable here -- see `crossFromSeal`.
        std::optional<bool> last_applied_is_seal;

        /// Probe B1's per-epoch contiguous run: opened at the first position walked in an epoch, closed
        /// when the walk leaves that epoch or stops.
        std::optional<RefTxnId> segment_first;
        std::vector<std::pair<RefTxnId, RefTxnId>> segments;
        const auto closeSegment = [&]()
        {
            if (segment_first)
                segments.emplace_back(*segment_first, resolved_through);
            segment_first.reset();
        };

        while (expected)
        {
            /// `_ckpt.committed_through` is the inclusive authority ceiling. Stop before reading a
            /// durable but uncommitted `F+1`, so neither it nor a later 404 can advance this cursor or
            /// establish the destructive frontier.
            if (*grounding->committed_through < *expected)
            {
                if (resolved_through == *grounding->committed_through)
                    frontier_proven = true;
                else
                {
                    report.recordAnomaly(ns, 0, ManifestId{ns, {}},
                        "ref intake: checkpoint committed_through precedes the sealed cursor");
                    unproven_reason = FoldResult::FrontierUnproven::CommittedBelowCursor;
                }
                break;
            }

            /// GET + decode the expected record. Absence is the decision point of the whole walk, and
            /// an invalid body is a per-namespace hold: the key belongs to exactly one namespace, so it
            /// can never be grounds for discarding another namespace's fold.
            const auto got = backend.get(layout.refLogKey(life, *expected));
            if (!got)
            {
                ++intake_absent_probes;
                const auto witness = witnessAbove(*expected);
                if (!witness)
                {
                    /// The ceiling test above already refused everything past `committed_through`, so
                    /// this absent position is committed and its record owes us an answer.
                    hold(*expected, HoldReason::GapBelowWitness,
                         "ref intake: a checkpoint-committed ref log is absent -- its authoritative "
                         "frontier cannot be complete");
                    break;
                }

                if (witness->writer_epoch == expected->writer_epoch)
                {
                    hold(*expected, HoldReason::GapBelowWitness,
                         "ref intake: expected next id absent below a same-epoch witness -- "
                         "contiguity says this cannot happen, so a durable record is missing");
                    break;
                }

                const auto crossed = crossFromSeal(resolved_through, last_applied_is_seal, *witness);
                if (!crossed)
                {
                    hold(*expected, HoldReason::UnconsumedSealCrossing,
                         "ref intake: a later epoch's records are reachable but this epoch's "
                         "closing seal was never consumed (or the position they chain from is "
                         "not one) -- the crossing has no proof");
                    break;
                }
                /// Every iteration must move `expected` strictly forward. A crossing that lands back on
                /// the position just read as absent makes no progress and would spin: the epoch-start
                /// record answered a GET inside `crossFromSeal` and then not here, so it is vanishing
                /// under us. Treat it as the impossible shape it is.
                if (!(*expected < *crossed))
                {
                    /// The record at `*crossed` answered the crossing's GET and then stopped answering
                    /// the walk's: an ABOVE-CURSOR object that vanished under us. Nothing may
                    /// legitimately remove one, so this is corruption, and it is the one hold shape no
                    /// amount of waiting clears -- which is exactly why it must be named durably. A
                    /// later round that read the namespace as merely quiet would otherwise grant it a
                    /// frontier proof and license destruction against a cut that is missing records.
                    hold(*expected, HoldReason::WitnessDisappeared,
                         "ref intake: the epoch crossing resolved back to the position that "
                         "just read absent -- the epoch-start record is not stably readable");
                    break;
                }
                ++intake_epoch_crossings;
                closeSegment();
                expected = *crossed;
                continue;
            }
            const RefTxnId log_id = *expected;

            /// Probe B2: open this transaction's ledger entry. A clamped log is opened but never
            /// committed, so it is correctly not reported unapplied.
            const uint32_t txn_ordinal = ledger.open(ns, log_id);

            ProfileEvents::increment(ProfileEvents::CASRefLogBodyGets);   /// one body GET per new log
            RefLogTxn txn;
            std::vector<RefManifestEdge> edges;
            try
            {
                txn = decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns_str, log_id);
                /// Extraction shares the decode try-block: an unrecognized owner_transition shape
                /// (`manifestEdgesOfTxn` -> `classifyOwnerTransitionShape`, Pool/CasRefProtocol.cpp) is
                /// exactly as untrustworthy as an undecodable body -- both mean this log cannot be
                /// folded -- so both get the SAME per-namespace hold below. Do
                /// NOT widen this catch over `foldManifestEdges` -- only intake, not the fold itself,
                /// shares this discipline.
                edges = manifestEdgesOfTxn(txn);
            }
            catch (const Exception & e)
            {
                LOG_WARNING(logger, "CAS GC ref intake: log {} invalid: {}",
                            layout.refLogKey(life, log_id), e.message());
                hold(log_id, HoldReason::BodyUndecodable, "ref log body invalid: namespace held below it");
                break;
            }

            /// Fold every explicit manifest edge of the log. A transaction applies
            /// ATOMICALLY ("either the complete transaction applies or none of it applies"): stage this
            /// log's blob deltas and owner-removed manifest cleanup in PER-LOG buffers and merge them into
            /// the round buffers only once the WHOLE log folds. A mid-log clamp DISCARDS the staged buffers
            /// so the cursor stays coherent -- merging a partially folded log's `-1` cleanup would let the
            /// post-CAS body delete remove a body whose edge is still unfolded behind the clamp, and the
            /// re-fold would then clamp on that missing body forever. A missing manifest body is a per-table
            /// CLAMP (barrier), never a round abort: keep the cursor below THIS log and re-read it next
            /// round. A removed precommit whose body never existed emitted no edge -- skip, no clamp.
            std::vector<BlobDelta> log_deltas;
            std::map<ManifestId, Token> log_mf_cleanup;
            for (const RefManifestEdge & edge : edges)
            {
                ProfileEvents::increment(ProfileEvents::CASRefEmittedEdges);   /// one manifest-edge event
                if (foldManifestEdges(edge.manifest_id, edge.change, log_deltas, log_mf_cleanup,
                                      txn_ordinal))
                    continue;

                if (edge.change < 0 && edge.owner_kind == RefOwnerKind::Precommit)
                    continue;   /// removed precommit that never activated: no edge to mirror, no clamp

                /// A `+1` precommit whose body is absent normally holds the fold barrier (the writer may
                /// still be uploading it). But a precommit naming a build PROVABLY DEAD by the durable
                /// watermark floor -- the SAME fact the orphan sweep uses to reclaim the body -- can never
                /// activate, and its body will never return. Skip it (non-activating, advance the log) so
                /// the barrier is not held forever on a body no writer will complete; otherwise this table
                /// clamps every round with no terminal resolution. Fail-closed: no durable watermark (the
                /// build cannot be proven dead) keeps the barrier.
                if (edge.change > 0 && edge.owner_kind == RefOwnerKind::Precommit
                    && prefixEligible(*store, ns,
                           BuildPrefix{.writer_epoch = edge.manifest_id.ref.writer_epoch,
                                       .build_sequence = edge.manifest_id.ref.build_sequence}))
                {
                    ProfileEvents::increment(ProfileEvents::CASGCDeadPrecommitSkipped);
                    ++intake_dead_precommits_skipped;
                    EventEmitter{*store}.emit([&](CasEvent & ev)
                    {
                        ev.type = CasEventType::GcFoldClamp;   /// reuse the fold-decision channel; outcome distinguishes
                        ev.namespace_ = ns_str;
                        ev.object_kind = CasEventObjectKind::Root;
                        ev.object_hash = manifestRefDebugString(edge.manifest_id.ref);
                        ev.outcome = "dead_precommit_skipped";
                        ev.reason = "live precommit body absent AND its build is below the watermark floor "
                                    "(provably dead); skip the non-activating edge instead of clamping forever";
                        ev.detail = {{"log", renderRefTxnId(log_id)}};
                    });
                    continue;
                }

                const char * reason = edge.change > 0
                    ? (edge.owner_kind == RefOwnerKind::Precommit
                           ? "fold barrier: live precommit body not yet present (non-activating)"
                           : "committed/promoted ref names a missing manifest body")
                    : "owner-removal: edge-bearing committed body missing at removal-fold";
                report.recordAnomaly(ns, 0, edge.manifest_id, reason);
                EventEmitter{*store}.emit([&](CasEvent & ev)
                {
                    ev.type = CasEventType::GcFoldClamp;
                    ev.namespace_ = ns_str;
                    ev.object_kind = CasEventObjectKind::Root;
                    ev.object_hash = manifestRefDebugString(edge.manifest_id.ref);
                    ev.outcome = "clamped";
                    ev.reason = reason;
                    ev.detail = {{"log", renderRefTxnId(log_id)},
                                 {"resolved_through",
                                  resolved_through == RefTxnId{} ? "none" : renderRefTxnId(resolved_through)}};
                });
                /// The barrier is a HOLD like any other -- same durable shape, same clearing rule (fold
                /// through `log_id`), and the same suppression consequence. Its reason is the one whose
                /// ordinary cause is benign (a writer that appended its record before finishing the
                /// manifest upload), and naming it durably is what lets an operator tell that apart
                /// from a namespace that has been stuck for hours. The anomaly and event were already
                /// emitted above with the manifest identity, which `hold` cannot carry, so this site
                /// sets the hold directly instead of calling it.
                fired = RefHold{.reason = HoldReason::ManifestBodyMissing, .offending_position = log_id,
                                .retry_count = 0, .next_retry_round = 0};
                break;   /// stop folding this log's edges; the cursor stays at resolved_through (< log_id)
            }

            if (fired)
                break;   /// discard the staged log_deltas / log_mf_cleanup (never merged) and stop this table

            /// The whole log folded: merge its staged transaction into the round buffers.
            if (!log_deltas.empty())
                ledger.markProduced(txn_ordinal);
            ledger.markCommitted(txn_ordinal);
            for (BlobDelta & d : log_deltas)
                deltas.push_back(std::move(d));
            for (const auto & [mid, tok] : log_mf_cleanup)
                result.mf_cleanup.emplace(mid, tok);

            /// A fully-folded `remove_namespace` transaction hands its `{ns, remove_txn_id}` to the
            /// life row's cleanup evidence; its owner-removal edges were folded above.
            if (const auto removal = removalTxnId(txn))
                new_removals.emplace_back(ns, *removal);
            if (!segment_first)
                segment_first = log_id;    /// this epoch's contiguous run opens at the first applied id
            resolved_through = log_id;     /// this log fully folded
            /// The KIND of the record the cursor now sits on, remembered for the crossing below: the
            /// chain check proves the identity of the position an epoch chains from, never that that
            /// position is a seal, and this is the one place the answer is free (the body is decoded and
            /// in hand). See the crossing's own comment for the half of this that cannot be re-checked.
            last_applied_is_seal = refLogTxnIsEpochSeal(txn);
            ++logs_applied;                /// probe B1: the SINGLE cursor-advance site
            table_changed = true;

            if (const std::optional<RefTxnId> next = nextRefLogIdWithinCommittedFrontier(
                    log_id, *last_applied_is_seal, *grounding->committed_through))
            {
                if (*last_applied_is_seal && next->writer_epoch != log_id.writer_epoch)
                {
                    const auto crossed = crossFromSeal(log_id, last_applied_is_seal, *grounding->committed_through);
                    if (!crossed)
                    {
                        hold(*next, HoldReason::UnconsumedSealCrossing,
                             "ref intake: checkpoint frontier cannot prove the successor of the epoch seal "
                             "just consumed");
                        break;
                    }
                    ++intake_epoch_crossings;
                    closeSegment();
                    expected = *crossed;
                }
                else
                    expected = *next;
            }
            else
            {
                frontier_proven = true;
                expected.reset();
            }
        }
        closeSegment();
        if (!segments.empty())
            walked_segments[ns_str] = std::move(segments);

        cov.last_folded_ref_id = resolved_through;

        /// THE CLEARING RULE (spec §5), and the only place it is decided. A hold clears by exactly one
        /// event: this walk RESOLVING the offending position -- folding through it and sealing a cursor
        /// at or above it. It never clears by observing another absent, because an absent is precisely
        /// what a lying store produces and precisely what made the hold necessary; and it never clears
        /// by the hint going quiet, because a quiet hint is not evidence about anything.
        ///
        /// Three cases, total and in order:
        ///   * this round detected a hold -- adopt it. It sits at or above the cursor, so any carried
        ///     hold at a HIGHER position is simply not reached yet and will be re-detected once this
        ///     one clears; a carried hold at a LOWER position was folded through, which is its
        ///     clearance.
        ///   * no new hold, but the walk stopped BELOW a carried hold's position -- it did not resolve
        ///     it, so the hold rides VERBATIM (same reason, same position).
        ///   * otherwise -- either there was no hold, or the walk folded through it. Cleared.
        std::optional<RefHold> effective;
        if (fired)
            effective = fired;
        else if (carried_hold && resolved_through < carried_hold->offending_position)
        {
            effective = carried_hold;
            /// The round produced no anomaly of its own for this namespace (the walk ended quietly),
            /// yet the namespace IS held: record one so this round's destructive work is suppressed on
            /// today's `anomalies`-based rule as well as on the hold set itself. Without it a hold
            /// carried through a quiet round would suppress nothing, which is the hole the carry exists
            /// to close.
            report.recordAnomaly(ns, 0, ManifestId{ns, {}},
                "ref intake: namespace still held below an unresolved position");
            EventEmitter{*store}.emit([&](CasEvent & ev)
            {
                ev.type = CasEventType::GcFoldClamp;
                ev.namespace_ = ns_str;
                ev.object_kind = CasEventObjectKind::Root;
                ev.outcome = "held";
                ev.reason = "carried hold: the offending position did not resolve this round";
                ev.detail = {{"expected", renderRefTxnId(carried_hold->offending_position)},
                             {"resolved_through",
                              resolved_through == RefTxnId{} ? "none" : renderRefTxnId(resolved_through)},
                             {"retry_count", std::to_string(carried_hold->retry_count)}};
            });
        }

        if (effective)
        {
            /// Retry bookkeeping. The count belongs to a POSITION, so it continues only while the hold
            /// stays at the same one; a hold that moved is a different stop and starts over. The count
            /// saturates rather than wrapping -- a wrapped counter would report a namespace stuck for
            /// four billion rounds as freshly held.
            const bool same_position = carried_hold
                && carried_hold->offending_position == effective->offending_position;
            effective->retry_count = same_position && carried_hold->retry_count < UINT32_MAX
                ? carried_hold->retry_count + 1
                : (same_position ? UINT32_MAX : 0);
            effective->next_retry_round = current_round + 1;
            cov.hold = effective;
            cov.classification = 4;
            ++intake_tables_held;
            /// A held namespace is unproven BY DEFINITION -- the hold names a position the walk could
            /// not resolve, so everything at or above it is unaccounted. Stated here rather than left to
            /// the loop's control flow: a carried hold rides forward on a round whose own walk ended
            /// quietly, and that quiet end must not be mistaken for a proof.
            frontier_proven = false;
            unproven_reason = FoldResult::FrontierUnproven::Held;
        }
        else
            cov.classification = table_changed ? 2 : 1;

        result.fold_seal.ref_lives.at(target.life_id).coverage = cov;
        ++result.frontier_namespaces;
        if (frontier_proven)
            ++result.frontier_proven;
        else
            result.frontier_deficit.count(unproven_reason);
        if (table_changed)
        {
            folded_any = true;
            ++intake_tables_changed;
        }
        if (fired)
            ++intake_tables_clamped;
    }

    /// Namespaces the round KNOWS about but never probed, because the frontier-probe budget ran out
    /// before reaching them. Their cursors ride VERBATIM -- dropping a cursor because a round ran out of
    /// budget would hand the next round a namespace to re-fold from `{0,0}`, which is a far worse
    /// outcome than the unproven frontier this already is. They count toward the universe and NOT toward
    /// the proofs, which is what suppresses the round.
    ///
    /// THE DENOMINATOR IS THE SEALED SET, BY CONSTRUCTION. `frontier_namespaces` is incremented by the
    /// number of rows this loop actually ADDED to the seal, never by the count the skip loop predicted.
    /// The two are the same set under the same filters, so they agree -- but agreeing "because both
    /// filters were written the same way" is exactly the kind of coincidence that decays under editing,
    /// and `frontier_namespaces` is the denominator an operator (and the integration test) reads as THE
    /// universe. Deriving it from the seal removes the possibility of publishing a number that
    /// describes a different universe than the round sealed. The `chassert` states the equality so a
    /// future divergence surfaces in debug rather than silently; it is metric integrity, not a safety
    /// gate, because both paths suppress the round either way.
    if (intake_unprobed_budget > 0)
    {
        result.frontier_namespaces += intake_unprobed_budget;
        result.frontier_deficit.probe_budget += intake_unprobed_budget;
        LOG_WARNING(logger,
            "CAS GC ref intake: the frontier-probe budget ({}) ran out with {} known namespace(s) "
            "unprobed; their cursors ride unchanged and ALL destructive work is suppressed this round",
            store->poolConfig().gc_frontier_probe_budget, intake_unprobed_budget);
    }

    /// All-or-nothing: a malformed key/body anywhere aborts the round's ref folding. Discard
    /// every ref delta and cursor advance already accumulated, carry each table's parent cursor verbatim,
    /// and let the recorded anomaly suppress destructive work.
    if (ref_folding_aborted)
    {
        deltas.clear();
        ledger = TxnApplyLedger{};   /// the deltas are gone, so nothing can be unapplied
        result.mf_cleanup.clear();
        folded_any = false;
        /// An abort discards this round's walk, so it discards its proofs with it: nothing this round
        /// observed may be offered as a frontier proof to the destructive gate.
        result.frontier_proven = 0;
        result.frontier_deficit = FoldResult::FrontierDeficit{};
        result.frontier_deficit.fold_aborted = result.frontier_namespaces;
        for (const WalkTarget & target : walk_targets)
        {
            RefCoverage cov;
            cov.classification = 1;
            if (const auto pit = parent_ref_lives.find(target.life_id); pit != parent_ref_lives.end())
            {
                cov.last_folded_ref_id = pit->second.coverage.last_folded_ref_id;
                /// An abort discards this round's work; it does not resolve anything, so a hold it
                /// found in the parent seal rides forward untouched -- not even the retry count moves,
                /// because nothing was retried.
                if (pit->second.coverage.hold)
                {
                    cov.hold = pit->second.coverage.hold;
                    cov.classification = 4;
                }
            }
            RefLifeFoldState & ref_life_state = result.fold_seal.ref_lives.at(target.life_id);
            ref_life_state.coverage = cov;
        }
    }

    /// PROBE B1's recomputation, taken HERE rather than at the seal write: every input it reads
    /// (`walked_segments`, the sealed ref-life coverage) is final as of this line, and nothing
    /// between here and the seal write touches any of them. Computing it inside the intake phase is what
    /// lets the `fold_ref_intake` row carry both numbers; the comparison and its fail-closed throw stay
    /// where they were, just before the seal write. Both stay 0 on a ref-folding abort -- that path
    /// discards every cursor advance and carries the parent cursors, so the identity does not apply.
    ///
    /// It counts the CUT ARITHMETICALLY, not by listed ids. Under arithmetic intake a listed-id count is
    /// not even the right question: a hint hole means a round legitimately applies records the listing
    /// never mentioned, so the old recomputation would report fewer logs than folded and fail every
    /// healthy round on a lying store -- it would have made this task's own fix unshippable.
    ///
    /// BE HONEST ABOUT WHAT IS LEFT. The old formula could disagree with reality because it was derived
    /// from a different source (the listing) than the counter. This one is derived from the runs the
    /// single advance site produced, so for the current code shape it is close to tautological, and B1's
    /// discriminating power went DOWN with this change rather than up. What it still asserts is worth
    /// keeping and is not free: THE SEALED CURSOR IS THE WALK'S CURSOR. The last run of each namespace is
    /// measured against the DURABLE ref-life coverage the next round will trust, not against the
    /// walk's own end, so a cursor sealed from anywhere other than this walk -- a stale carry, a mutated
    /// coverage row, a future edit that advances the cursor away from the advance site -- either fails
    /// the epoch/order check below or lands as a count that no longer matches `logs_applied`.
    logs_accounted_this_round = 0;
    logs_applied_this_round = 0;
    if (!ref_folding_aborted)
    {
        uint64_t logs_accounted = 0;
        for (const auto & [ns_str, segments] : walked_segments)
        {
            const UInt128 life_id = live_incarnation.at(ns_str);
            RefTxnId sealed{};
            if (const auto sit = result.fold_seal.ref_lives.find(life_id);
                sit != result.fold_seal.ref_lives.end())
                sealed = sit->second.coverage.last_folded_ref_id;

            for (size_t i = 0; i < segments.size(); ++i)
            {
                const auto & [first, last] = segments[i];
                /// The final run must end exactly where the seal says this namespace stopped, in the same
                /// epoch. A disagreement is a sealed cursor that did not come from this walk, so it fails
                /// closed here rather than travelling into the next round's baseline.
                const RefTxnId end = i + 1 == segments.size() ? sealed : last;
                if (end.writer_epoch != first.writer_epoch || end < first)
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS GC fold: namespace {} sealed a cursor at {} that does not close the run it "
                        "walked (opened at {}). GC refuses to commit the round; recover with "
                        "SYSTEM CAS GC REBUILD.",
                        ns_str, end == RefTxnId{} ? "none" : renderRefTxnId(end), renderRefTxnId(first));
                logs_accounted += end.ref_sequence - first.ref_sequence + 1;
            }
        }
        logs_accounted_this_round = logs_accounted;
        logs_applied_this_round = logs_applied;
    }

    intake_timer->metric("logs_accounted", logs_accounted_this_round);
    intake_timer->metric("logs_applied", logs_applied_this_round);
    intake_timer->metric("deltas_emitted", deltas.size());
    intake_timer->metric("transactions_opened", ledger.txns.size());
    intake_timer->metric("tables_scanned", ref_tables.size());
    intake_timer->metric("tables_changed", intake_tables_changed);
    intake_timer->metric("tables_clamped", intake_tables_clamped);
    /// `tables_held` counts the coverage rows this round SEALED held -- the ones it detected plus the
    /// ones it carried. It is what `suppress_destructive` keys on, so a round that reclaims nothing has
    /// this column to explain itself. `unhinted_held_walked` is the subset the hint never mentioned:
    /// nonzero means the store stopped listing a namespace that is still held, which is the shape that
    /// used to clear a hold silently.
    intake_timer->metric("tables_held", intake_tables_held);
    intake_timer->metric("unhinted_held_walked", intake_unhinted_held);
    /// THE FROZEN WORK-SET, ON THE ROW THAT PAID FOR IT. `tails_advanced` is how many hinted namespaces
    /// had records to fold this round because their listed tail sat above their cursor -- the round's
    /// real work -- and `tails_unchanged` is how many folded NOTHING and paid only their single frontier
    /// probe, which on a wide pool with a few hot namespaces is most of them and is the number that
    /// explains a short round. `tails_below_cursor` is the anomaly of the three: cleanup deletes logs
    /// from the bottom up, so a listing whose greatest id is below a cursor we folded through is stale or
    /// lying, and a column that is normally 0 is what makes the pool where it is not stand out.
    intake_timer->metric("tails_advanced", intake_tails_advanced);
    intake_timer->metric("tails_unchanged", intake_tails_unchanged);
    intake_timer->metric("tails_below_cursor", intake_tails_below_cursor);
    /// THE FRONTIER OBLIGATION, on the row that explains a round which reclaimed nothing:
    /// `frontier_namespaces` is the round's universe (hint ∪ sealed cursors ∪ catalog `Live`/`Removing`
    /// entries), `frontier_proven` the part of it that reached an honest end-of-stream, and the
    /// remaining columns say where the rest went -- walked because the hint had gone quiet about them,
    /// not walked at all because the probe budget ran out, or walked ONLY because the catalog named a
    /// namespace neither the hint nor a carried cursor did (`catalog_only_walked` -- always 0 until a
    /// namespace is admitted with no listed objects and no sealed cursor yet).
    intake_timer->metric("frontier_namespaces", result.frontier_namespaces);
    intake_timer->metric("frontier_proven", result.frontier_proven);
    /// `catalog_entries` is the hot-scan catalog cut's own row count (every lifecycle state, `Creating`
    /// included), and `catalog_proved_empty` is the derived verdict the destructive gate's non-vacuity
    /// term consults. Together they let an operator tell a proved-empty `0/0` (success) apart from a
    /// `Creating`-only or otherwise unprovable `0/0` (still suppressed) without re-deriving either fact.
    intake_timer->metric("catalog_entries", catalog_snapshot.catalog.entries.size());
    intake_timer->metric("catalog_proved_empty", result.catalog_cut_proved_empty ? 1 : 0);
    intake_timer->metric("unhinted_quiet_walked", intake_unhinted_quiet);
    intake_timer->metric("frontier_unprobed_budget", intake_unprobed_budget);
    intake_timer->metric("catalog_only_walked", intake_catalog_only);
    intake_timer->metric("dead_precommits_skipped", intake_dead_precommits_skipped);
    /// The exact reads arithmetic intake pays that the listing-driven loop did not. `absent_probes`
    /// counts EVERY read that came back absent: the expected-next of each namespace walked (at least one
    /// -- the absent expected-next IS the frontier proof) and the epoch-start read of a crossing that
    /// failed, which a namespace that holds every round pays every round. `epoch_crossings` counts the
    /// successful ones. Both are on the row so the cost shows up where it is spent.
    intake_timer->metric("absent_probes", intake_absent_probes);
    intake_timer->metric("epoch_crossings", intake_epoch_crossings);
    intake_timer->metric("namespace_removals", new_removals.size());
    intake_timer->metric("ref_folding_aborted", ref_folding_aborted ? 1 : 0);
    intake_timer.reset();   /// emits the `fold_ref_intake` row

    result.frontier_unprobed_budget = intake_unprobed_budget;

    /// Reuse the round's single ref LIST for post-CAS ref-object cleanup: one LIST serves
    /// intake AND cleanup planning).
    result.ref_tables = ref_tables;
    /// Same reuse for the checkpoints: the intake walk already paid for them as its second witness, and
    /// the cleanup ranges below are the other consumer of the same fact.
    ///
    /// An UNDECODABLE `_ckpt` contributes no entry here and therefore grants no cleanup authority. The
    /// walk above also held (or recorded an anomaly for) every such namespace, so the round's destructive
    /// gate is shut and `cleanupRefObjects` deletes nothing at all this round. Any future change that
    /// narrows the gate from round-wide to per-namespace must carry this set with it.
    result.checkpoints = checkpoints.recovery_checkpoints;

    /// Folding the terminal record earns positive cleanup evidence directly on the catalog-admitted
    /// life row. It does not claim that any physical debris was removed: `_ckpt`, stream and `_files`
    /// residue belongs to the perpetual janitor, while orphan manifests belong to the manifest sweep.
    /// Consequently namespace removal performs no physical LIST and has no Pending/Completed handshake.
    if (!ref_folding_aborted)
        for (const auto & [rns, remove_txn_id] : new_removals)
        {
            const auto life_it = live_incarnation.find(rns.string());
            if (life_it == live_incarnation.end())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS GC fold: terminal removal for namespace '{}' has no row in the round catalog cut",
                    rns.string());
            auto row_it = result.fold_seal.ref_lives.find(life_it->second);
            if (row_it == result.fold_seal.ref_lives.end())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS GC fold: terminal removal for namespace '{}' has no admitted ref-life row",
                    rns.string());
            row_it->second.cleanup_evidence = RefCleanupEvidence{.remove_txn_id = remove_txn_id};
        }

    /// The parent generation's per-shard run segments, resolved from
    /// the parent fold seal's `blob_target_runs` and grouped by the ref's explicit `shard`. The same seal
    /// `discover_ref_seal` the token-diff already read is the run source — consumers resolve runs THROUGH
    /// refs (a run sealed for the parent generation may physically live under an older generation's key),
    /// never by `blobTargetRunKey` construction.
    std::map<uint64_t, std::vector<RunRef>> parent_runs_by_shard;
    for (const RunRef & r : discover_ref_seal.blob_target_runs)
        parent_runs_by_shard[r.shard].push_back(r);

    /// PURE REF-CARRY: a gc-shard with an EMPTY delta bucket
    /// AND an EMPTY retired input list neither reads nor writes its run — the new fold_seal copies the
    /// parent's `RunRef`s VERBATIM (key/checksum/shard/generation) so the next round resolves them. This
    /// is deterministic (same refs for the same inputs), so seal determinism / crash-replay adoption hold.
    /// An empty delta with a NON-EMPTY retired list still runs the merge: settlement must happen every
    /// pass (carried/graduated/redeleted entries), and that pass reads the run to recompute in-degrees.
    /// Distill one shard's `condemned_summary` entry from the `kCondemned` rows it re-emitted this pass
    /// (`still_retired` mirrors those rows exactly). Folding shards call this; it makes the next
    /// round's `graduationDue` and pure-carry decisions read only the seal, never a run.
    auto summarize = [](const std::vector<RetiredEntry> & still) -> CondemnedSummary
    {
        CondemnedSummary s;
        s.condemned_total = still.size();
        for (const RetiredEntry & e : still)
        {
            if (e.delete_pending)
                ++s.pending_total;
            else
                s.oldest_nonpending_condemn_round =
                    std::min(s.oldest_nonpending_condemn_round, e.condemn_round);
        }
        return s;
    };
    /// The parent seal's summary for a shard, used ONLY for the pure-carry DECISION (condemned_total==0).
    /// Missing => zero: a fresh pool has no parent entry (correct baseline), and a snap_generation>0 seal
    /// that is missing an entry we would pure-carry fails closed inside `carryParentRefs` when it copies.
    auto summaryOfParent = [&](uint64_t shard) -> CondemnedSummary
    {
        const auto it = discover_ref_seal.condemned_summary.find(shard);
        return it != discover_ref_seal.condemned_summary.end() ? it->second : CondemnedSummary{};
    };
    auto carryParentRefs = [&](uint64_t shard)
    {
        const auto it = parent_runs_by_shard.find(shard);
        if (it != parent_runs_by_shard.end())
            for (const RunRef & r : it->second)
                result.fold_seal.blob_target_runs.push_back(r);   /// verbatim: parent key/checksum/gen
        /// Totality: a pure-carry shard settled nothing, so its `condemned_summary` entry is the parent's
        /// VERBATIM. On a fresh pool (no adopted parent seal) it is the explicit zero baseline; otherwise
        /// the parent seal MUST carry the entry (a live seal is total over gc_shards) — a missing entry is
        /// corrupt bookkeeping, never silently treated as zero.
        if (state.snap_generation == 0)
            result.fold_seal.condemned_summary[shard] = CondemnedSummary{};
        else
        {
            const auto sit = discover_ref_seal.condemned_summary.find(shard);
            if (sit == discover_ref_seal.condemned_summary.end())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS gc fold: parent fold seal (generation {}, attempt {}) lacks a condemned_summary "
                    "entry for gc-shard {} — the seal is not total over gc_shards; GC bookkeeping is corrupt",
                    state.snap_generation, state.snap_attempt, shard);
            result.fold_seal.condemned_summary[shard] = sit->second;
        }
    };
    auto priorRunsFor = [&](uint64_t shard) -> const std::vector<RunRef> &
    {
        static const std::vector<RunRef> empty;
        const auto it = parent_runs_by_shard.find(shard);
        return it != parent_runs_by_shard.end() ? it->second : empty;
    };


    /// CLAMP SUPPRESSION: any clamp
    /// this pass means landed-before-cut events may be UNFOLDED behind a clamped cursor — the
    /// floor's "landed before cut => folded before graduation" lemma does not hold, so this pass
    /// must not graduate NOR execute pending deletes (the merge carries everything; condemnation
    /// and sparing continue). Deletes resume on the first clamp-free pass. This is the honest-mode
    /// counterpart of the model's SabotageSkipChangedShard counterexample.
    ///
    /// PHASE 9/18 `fold_reduce`: the prior-run streaming GETs, one HEAD per zero-transition candidate,
    /// and the run PUTs -- the heaviest phase of a folding round on a pool with churn. It also carries
    /// probe B2's verdict (`transactions_unapplied`), which is 0 on every committed round because a nonzero
    /// value throws a few lines below; the row is therefore the forensic record of a round that failed.
    std::optional<GcPhaseTimer> reduce_timer;
    reduce_timer.emplace(phase_sink, "fold_reduce");
    const uint64_t deltas_in = deltas.size();
    const uint64_t condemned_before = report.condemned;
    uint64_t shards_pure_carry = 0;
    /// ============================ THE DESTRUCTIVE GATE ============================
    ///
    /// Computed ONCE, here, from three independent terms, and consulted at every destructive site of
    /// the round (see `FoldResult::suppress_destructive`). It sits at this point in the fold because
    /// every input is final: the coverage rows the seal will carry are written, the anomalies are
    /// recorded, and the frontier tally is closed.
    ///
    /// Term 2 is STRUCTURAL. Today every hold also records an anomaly, so term 1 happens to imply it --
    /// but that is a property of the current code, not the invariant, and a gate that relies on a
    /// coincidence opens the day the coincidence stops holding. The hold SET is the invariant, so the
    /// gate reads the seal it is about to make durable.
    const std::vector<std::pair<UInt128, RefHold>> carried_holds = result.carriedHolds();

    /// Term 3, the universe seam. `Authoritative` means the round's universe is the catalog's own
    /// `Live`/`Removing` set, so the per-namespace proofs decide on their own; `StageA_Suppressed`
    /// refuses outright, which is the posture a test asserting inertness selects. See `UniversePolicy`.
    const bool universe_authoritative = policy == UniversePolicy::Authoritative;
    /// `frontier_proven == frontier_namespaces` is `0 == 0` -- TRUE --
    /// on an empty universe, which is not a proof of anything BY ITSELF: a fresh pool, a damaged
    /// catalog, or a read that legitimately returns nothing all produce zero entries. `frontier_namespaces
    /// > 0` closes that degenerate case for the ordinary, nonempty pool -- a nonzero count still needs
    /// every namespace PROVEN, which the equality above still checks.
    ///
    /// That floor alone is unsound for a pool whose LAST namespace was removed: the catalog then reads
    /// genuinely empty forever, `frontier_namespaces` can never again exceed 0, and the equality's
    /// vacuous truth can never be licensed -- an emptied pool would stop reclaiming permanently. The
    /// floor's job was never "reject `frontier_namespaces == 0`", it was "reject the UNSUPPORTED case of
    /// it". `catalog_cut_proved_empty` is the supported case: the round's own hot-scan catalog cut,
    /// read once and reused (never a second `GET`), decoded successfully, token-bearing, and holding
    /// zero rows of every lifecycle state including `Creating`. Under this pool's protocol every live or
    /// live-precommit edge requires an exact `Live` catalog row (INV-3), so that cut is a positive proof
    /// that no namespace anywhere holds one -- not merely an absence of proof. A catalog holding only
    /// `Creating` rows produces the SAME `frontier_namespaces == 0` but is a birth in progress, not an
    /// empty universe, and `catalog_cut_proved_empty` is false for it (see `entries.empty()` above).
    result.frontier_complete = universe_authoritative
        && result.frontier_proven == result.frontier_namespaces
        && (result.frontier_namespaces > 0 || result.catalog_cut_proved_empty);
    const bool frontier_incomplete = !result.frontier_complete;

    result.suppress_destructive =
        !report.anomalies.empty() || !carried_holds.empty() || frontier_incomplete;
    const bool suppress_destructive = result.suppress_destructive;
    if (suppress_destructive)
    {
        ProfileEvents::increment(ProfileEvents::CASGCClampSuppressedPasses);
        /// LEVEL SPLIT, deliberately. A pass suppressed by an anomaly, a hold, an unproven namespace or
        /// an empty universe has a per-round cause an operator can chase, and that is a WARNING. A pass
        /// suppressed because the CALLER refused to supply a universe carries no such cause -- nothing on
        /// the pool explains it -- so it is reported at Info with the same numbers rather than raising an
        /// alarm nobody can act on.
        ///
        /// An AUTHORITATIVE-but-EMPTY universe that the catalog cut did NOT prove empty
        /// (`frontier_namespaces == 0 && !catalog_cut_proved_empty` -- e.g. a `Creating`-only catalog)
        /// is a per-round cause and is named explicitly here, because the bare equality
        /// `frontier_proven != frontier_namespaces` reads `0 != 0` (false) and would otherwise miss it.
        /// A catalog the cut DID prove empty is not a suppression cause at all: it satisfies
        /// `frontier_complete`, so a round reaching this block with one is suppressed by an anomaly or
        /// a hold, never by the frontier term.
        const bool per_round_cause = !report.anomalies.empty() || !carried_holds.empty()
            || result.frontier_proven != result.frontier_namespaces
            || (universe_authoritative && result.frontier_namespaces == 0 && !result.catalog_cut_proved_empty);
        const char * const universe_note =
            universe_authoritative ? "" : "; the caller supplied no universe";
        /// Names the real reason a `Creating`-only (or otherwise unprovable) catalog is not an empty
        /// universe, so the operator does not read a suppressed round with rows on file as "empty".
        const String catalog_empty_note =
            (universe_authoritative && result.frontier_namespaces == 0 && !result.catalog_cut_proved_empty)
                ? fmt::format("; catalog holds {} row(s), none walkable/provable", catalog_snapshot.catalog.entries.size())
                : String{};
        /// The per-cause breakdown of the unproven namespaces. Without it "N of M proven" names a
        /// deficit but not its cause, and the causes want opposite operator responses.
        const String deficit_note = result.frontier_deficit.total() == 0
            ? String{}
            : fmt::format("; unproven: {}", result.frontier_deficit.describe());
        if (per_round_cause)
            LOG_WARNING(logger,
                "CAS GC fold: destructive work SUPPRESSED this pass — {} anomaly(ies), {} held "
                "namespace(s), frontier {} ({} of {} namespace(s) proven{}{}{}). Graduations and pending "
                "deletes are carried; nothing irreversible runs until a pass that clears all three.",
                report.anomalies.size(), carried_holds.size(),
                result.frontier_complete ? "complete" : "INCOMPLETE",
                result.frontier_proven, result.frontier_namespaces, universe_note, catalog_empty_note,
                deficit_note);
        else
            LOG_INFO(logger,
                "CAS GC fold: destructive work SUPPRESSED this pass — {} anomaly(ies), {} held "
                "namespace(s), frontier {} ({} of {} namespace(s) proven{}{}{}). Graduations and pending "
                "deletes are carried; nothing irreversible runs until a pass that clears all three.",
                report.anomalies.size(), carried_holds.size(),
                result.frontier_complete ? "complete" : "INCOMPLETE",
                result.frontier_proven, result.frontier_namespaces, universe_note, catalog_empty_note,
                deficit_note);
    }

    std::vector<BlobSourceRetirement> orphan_source_retirements;
    if (!suppress_destructive && store->poolConfig().manifest_sweep_list_budget_keys > 0)
    {
        result.orphan_sweep = planManifestCursorPage(
            *store,
            state.manifest_sweep_cursor,
            store->poolConfig().manifest_sweep_list_budget_keys,
            store->poolConfig().manifest_sweep_delete_budget_keys,
            /// The sweep may recover a catalog-named namespace's debris only from the same frozen catalog
            /// cut and `_ckpt` frontier the round's own universe came from -- which is exactly what an
            /// authoritative universe means, and is why this is the gate's term and not a separate one.
            universe_authoritative,
            &work_budget);
        for (const ManifestSweepResult::Nomination & nomination : result.orphan_sweep.nominations)
            orphan_source_retirements.insert(
                orphan_source_retirements.end(),
                nomination.source_retirements.begin(),
                nomination.source_retirements.end());
    }

    if (state.gc_shards == 1)
    {
        /// SINGLE-SHARD PATH (gc_shards == 1). Every blob routes to shard 0, so the entire delta stream
        /// folds into one `blobTargetRunKey(new_generation, 0, 0)` run.
        if (!folded_any && orphan_source_retirements.empty() && summaryOfParent(0).condemned_total == 0)
        {
            /// Pure ref-carry: nothing changed and no condemned entries to settle => zero run I/O. Carry the
            /// parent shard-0 refs + summary into the seal so coverage/resume/graduation stay durable.
            carryParentRefs(0);
            ++shards_pure_carry;
        }
        else
        {
            /// Either a real delta or a non-empty retired input: run the merge (empty deltas still settle
            /// the kCondemned rows riding the parent run). The prior runs are the parent seal's shard-0 refs.
            foldDeltasIntoGeneration(backend, layout, priorRunsFor(0),
                                     new_generation, attempt, /*shard*/0,
                                     std::move(deltas), result.fold_seal.blob_target_runs,
                                     current_round, condemn_round, head_blob, peek_head,
                                     confirm_condemned_marker,
                                     result.retired_merge.data(), suppress_destructive,
                                     &ledger.applied, std::move(orphan_source_retirements),
                                     &work_budget);
            result.fold_seal.condemned_summary[0] = summarize(result.retired_merge[0].still_retired);
        }
    }
    else
    {
        /// SHARDED PATH (gc_shards > 1) — target-sharded reducers. Each blob's
        /// `BlobDelta` carries its full signed edge stream; `blobShard(blob_hash, gc_shards)` partitions
        /// the stream into `gc_shards` disjoint buckets. Each bucket folds via its own `ShardReducer`
        /// into `blobTargetRunKey(new_generation, shard, 0)`. The `RootOwnerEvent`'s paired old/new
        /// bindings produced the `-1`/`+1` deltas above, so a promote that displaces a blob's owner
        /// emits BOTH the `-1` (old binding) and the `+1` (new binding) at the SAME source event. This
        /// is why cross-shard displacement needs no special handling: each delta routes independently and
        /// deterministically to whichever target shard owns its blob; the old/new pair is solved at the
        /// source, not by a cross-shard fixup.
        std::vector<std::vector<BlobDelta>> buckets(state.gc_shards);
        for (BlobDelta & d : deltas)
            buckets[blobShard(d.ref, state.gc_shards)].push_back(std::move(d));
        std::vector<std::vector<BlobSourceRetirement>> retirement_buckets(state.gc_shards);
        for (BlobSourceRetirement & retirement : orphan_source_retirements)
            retirement_buckets[blobShard(retirement.ref, state.gc_shards)].push_back(std::move(retirement));

        for (uint64_t shard = 0; shard < state.gc_shards; ++shard)
        {
            if (buckets[shard].empty() && retirement_buckets[shard].empty()
                && summaryOfParent(shard).condemned_total == 0)
            {
                /// Pure ref-carry for this shard: empty delta + no condemned entries => zero run I/O.
                carryParentRefs(shard);
                ++shards_pure_carry;
                continue;
            }
            /// A reducer owns exactly one disjoint shard. Two replicas may run reducers for DIFFERENT
            /// shards concurrently (CasGcScheduler ownership); their run-key namespaces never collide.
            std::vector<RunRef> shard_runs;
            foldDeltasIntoGeneration(
                backend, layout, priorRunsFor(shard), new_generation, attempt, shard,
                std::move(buckets[shard]), shard_runs,
                current_round, condemn_round, head_blob, peek_head,
                confirm_condemned_marker,
                &result.retired_merge[shard], suppress_destructive,
                &ledger.applied, std::move(retirement_buckets[shard]),
                &work_budget);
            for (RunRef & r : shard_runs)
                result.fold_seal.blob_target_runs.push_back(std::move(r));
            result.fold_seal.condemned_summary[shard] = summarize(result.retired_merge[shard].still_retired);
        }
    }

    /// Aggregate the unmatched-remove signal across every gc-shard this pass touched and log ONCE per
    /// round with the total plus one example, rather than from the hot per-edge inner loop that detects
    /// them (see `foldDeltasIntoGeneration`'s comment) — that loop runs over potentially millions of
    /// rows, so it only counts (`ProfileEvents::CASGCUnmatchedRemoveDeltas`, incremented per occurrence)
    /// and hands back one example; this is the bounded, once-per-round operator-visible trail.
    {
        uint64_t total_unmatched_removes = 0;
        std::optional<UnmatchedRemoveExample> example;
        for (const RetiredMergeResult & merge : result.retired_merge)
        {
            total_unmatched_removes += merge.unmatched_removes;
            if (!example && merge.unmatched_remove_example)
                example = merge.unmatched_remove_example;
        }
        if (total_unmatched_removes > 0 && example)
            LOG_WARNING(logger,
                "CAS GC fold: {} unmatched removal delta(s) this pass (matched no existing source edge; "
                "a harmless per-key no-op by design, since the in-degree model is a set, not a counter — "
                "but a persistent nonzero rate means removal deltas are reaching the reducer without "
                "their matching activation, which is a correctness signal) — example: blob {} source {}",
                total_unmatched_removes, blobIdOf(example->ref), u128ToHex(example->source_id));
        reduce_timer->metric("unmatched_removes", total_unmatched_removes);
    }

    /// PROBE B2's ledger verdict, computed inside the reduce phase so the row carries it; the
    /// fail-closed throw it drives stays below, at its original site before the seal write.
    const std::vector<uint32_t> unapplied_txns = ledger.unapplied();
    transactions_unapplied_this_round = unapplied_txns.size();
    {
        uint64_t graduated = 0;
        uint64_t spared = 0;
        uint64_t redelete_pending = 0;
        for (const RetiredMergeResult & merge : result.retired_merge)
        {
            graduated += merge.graduated.size();
            spared += merge.spared.size();
            redelete_pending += merge.redelete.size();
        }
        reduce_timer->metric("shards_total", state.gc_shards);
        reduce_timer->metric("shards_pure_carry", shards_pure_carry);
        reduce_timer->metric("shards_reduced", state.gc_shards - shards_pure_carry);
        reduce_timer->metric("deltas_in", deltas_in);
        reduce_timer->metric("runs_written", result.fold_seal.blob_target_runs.size());
        reduce_timer->metric("condemned", report.condemned - condemned_before);
        reduce_timer->metric("graduated", graduated);
        reduce_timer->metric("spared", spared);
        reduce_timer->metric("redelete_pending", redelete_pending);
        reduce_timer->metric("suppress_destructive", suppress_destructive ? 1 : 0);
        /// Published separately from `suppress_destructive` so a reader can tell the frontier term apart
        /// from the anomaly and hold terms without re-deriving the formula from the tally.
        reduce_timer->metric("frontier_complete", result.frontier_complete ? 1 : 0);
        reduce_timer->metric("transactions_unapplied", transactions_unapplied_this_round);
    }
    reduce_timer.reset();   /// emits the `fold_reduce` row

    /// The part-manifest cleanup RUN + its fold-seal record are removed: the run
    /// object had no reader — the manifest cleanups execute inline from `result.mf_cleanup` (below /
    /// the recheck path), so the durable bundle was pure dead weight. `result.mf_cleanup` is unchanged.

    /// Write-once CasFoldSeal: its existence marks fold complete. The fold seal is DETERMINISTIC (same
    /// fold inputs => byte-identical seal), so it goes through `putDeterministicArtifact`: a byte-equal
    /// occupant is our own crash/deterministic replay (adopt, no-op); divergent bytes are impossible
    /// under correct operation and fail closed with `CORRUPTED_DATA`. A deposed leader writes under its
    /// own unadopted attempt so it never collides with the adopted seal — the occupant here is only ever
    /// our own prior attempt-scoped write.
    /// PROBE B2's verdict. A committed transaction that produced deltas but whose deltas never
    /// reached a reducer means this round LOST a durable record it had already read and decoded.
    /// Unlike a 404 during a fold (missing evidence, which must never wedge the round), this is proof
    /// of loss, so the round fails CLOSED: nothing is adopted, GC reclaims and deletes nothing, and an
    /// operator has to intervene. Thrown before the seal write, and therefore long before the single
    /// `gc/state` CAS, so the whole round evaporates.
    /// `unapplied_txns` was computed in the reduce phase above (so its row could report it); the verdict
    /// itself is unchanged and still fires here, before the seal write.
    if (!unapplied_txns.empty())
    {
        String detail;
        for (size_t i = 0; i < unapplied_txns.size() && i < 8; ++i)
        {
            if (i != 0)
                detail += ", ";
            detail += ledger.namespaces[unapplied_txns[i]] + "@"
                + renderRefTxnId(ledger.txns[unapplied_txns[i]]);
        }
        ProfileEvents::increment(ProfileEvents::CASGCUnappliedFoldedTransactions, unapplied_txns.size());
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS GC fold: {} ref transaction(s) folded and merged into the round buffers but NONE of "
            "their blob deltas reached a shard reducer ({}{}). The round would have advanced its "
            "cursor past a transaction it never applied. GC refuses to commit the round; recover with "
            "SYSTEM CAS GC REBUILD.",
            unapplied_txns.size(), detail, unapplied_txns.size() > 8 ? ", ..." : "");
    }

    /// PROBE B1's comparison. Both terms were derived at the end of the ref intake (see the
    /// recomputation there, which is also what the `fold_ref_intake` row reports) and are 0 on a
    /// ref-folding abort, where the identity does not apply -- so the inequality below can only fire on
    /// a round that actually folded.
    if (logs_accounted_this_round != logs_applied_this_round)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS GC fold: the round sealed coverage over {} ref log(s) but only {} fully folded -- "
            "a cursor advanced past a log this round never applied. GC refuses to commit the round; "
            "recover with SYSTEM CAS GC REBUILD.",
            logs_accounted_this_round, logs_applied_this_round);

    /// PHASE 10/18 `fold_seal_write`: one PUT (or, on a deterministic replay, a byte-compare GET).
    {
        GcPhaseTimer t(phase_sink, "fold_seal_write");
        validateFoldSealForWrite(result.fold_seal, layout, store->poolConfig().gc_shards);
        const String seal_body = encodeFoldSeal(result.fold_seal);
        t.metric("seal_bytes", seal_body.size());
        t.metric("seal_runs", result.fold_seal.blob_target_runs.size());
        t.metric("seal_ref_lives", result.fold_seal.ref_lives.size());
        t.metric("seal_cleanup_evidence", std::count_if(
            result.fold_seal.ref_lives.begin(), result.fold_seal.ref_lives.end(),
            [](const auto & item) { return item.second.cleanup_evidence.has_value(); }));
        putDeterministicArtifact(backend, layout.foldSealKey(new_generation, attempt), seal_body);
    }

    /// One-pass round: the fold NO LONGER CASes gc/state. (new_generation, attempt) are adopted
    /// in-memory here and committed — together with the round, the retired refs, and the retention
    /// cursor — by the SINGLE round CAS in runRegularRound. A deposed leader's whole pass therefore
    /// evaporates at that one CAS; its attempt-scoped artifacts are never adopted.
    state.snap_generation = new_generation;
    state.snap_attempt = attempt;
    return result;
}

void Gc::reportSweepRetention(const ManifestSweepResult & result)
{
    const auto top = result.topRetainReason();
    if (top.second == 0)
    {
        /// Nothing retained by the premise. Re-arm, so that the next retention -- however far off --
        /// is reported as the change it is rather than swallowed by a repeat counter.
        last_retain_rollup.reset();
        retain_rollup_passes_since_report = 0;
        return;
    }

    const bool changed = !last_retain_rollup || *last_retain_rollup != top;
    if (!changed && ++retain_rollup_passes_since_report < kRetainRollupRepeatPasses)
        return;

    /// INFO, not WARNING: retention is the CORRECT outcome whenever rule (1) is unsatisfiable (it is
    /// satisfiable only for a closed-and-folded epoch), so warning here would alarm on healthy rounds. It is
    /// still the operator's answer to "why is manifest debris not shrinking?", which is why it is not
    /// left at DEBUG with the per-object sentences.
    /// The "X of Y" denominator is the RETAINED total, not `skipped`. `skipped` is a strictly larger
    /// population -- it also counts malformed keys, ineligible prefixes, protected owners and
    /// budget-deferred candidates -- so measuring the top class against it would understate the class's
    /// share of the very number this sentence just reported.
    const uint64_t retained = result.retained_no_coverage + result.retained_hold
        + result.retained_unconsumed_seal + result.retained_tail_removal;
    LOG_INFO(logger,
        "CAS gc orphan sweep: retained {} manifest body(ies) this pass, most of them ({} of {}) for "
        "'{}' -- see the fold seal's coverage for that namespace; deleted {}, listed {}, skipped {}",
        retained, top.second, retained, sweepRetainClassName(top.first),
        result.deleted, result.listed, result.skipped);

    last_retain_rollup = top;
    retain_rollup_passes_since_report = 0;
}

void Gc::cleanupRefObjects(
    const FoldResult & folded, const GcLease & adopted_lease, bool suppress_destructive,
    GcRoundWorkBudget & work_budget)
{
    /// A clamp / ref-folding abort this round may leave landed-before-cut edges unfolded behind the clamp,
    /// so a covered-log cleanup could delete a log whose delta is not yet durable -- defer to a clean pass.
    if (suppress_destructive)
        return;

    Backend & backend = store->backend();
    const Layout & layout = store->layout();
    if (!folded.catalog_cut)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS GC ref cleanup: fold result carries no catalog cut");

    for (const auto & [ns_str, listing] : folded.ref_tables)
    {
        const RootNamespace ns{ns_str};
        /// Review C3: look up the SAME complete cut the round's walk resolved, never re-resolve
        /// independently. A fresh read here could see a namespace dropped and recreated since the
        /// walk and delete the successor's objects using predecessor bounds. An absent or `Creating`
        /// row is skipped rather than mapped to a fabricated key.
        const auto entry_it = std::lower_bound(
            folded.catalog_cut->catalog.entries.begin(), folded.catalog_cut->catalog.entries.end(), ns,
            [](const CatalogEntry & entry, const RootNamespace & needle) { return entry.ns < needle; });
        if (entry_it == folded.catalog_cut->catalog.entries.end() || entry_it->ns != ns
            || (entry_it->state != NsState::Live && entry_it->state != NsState::Removing))
            continue;
        const CatalogEntry & observed_entry = *entry_it;
        const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry_it->ns, entry_it->incarnation);

        /// Current-life ref cleanup is not the dead-life janitor: every irreversible key delete must
        /// still be licensed by the SAME complete catalog observation and GC lease that adopted the
        /// fold. Re-read both after the target HEAD and immediately before `deleteExact`. A moved token,
        /// changed row/life, missing or unreadable authority object, or changed owner/sequence stops the
        /// whole cleanup pass. Continuing with another row/key would turn a refusal into a fallback.
        const auto deleteRefObject = [&](const String & key)
        {
            const HeadResult h = backend.head(key);
            if (!h.exists)
                return true;

            try
            {
                const CasRefCatalog::Snapshot current_catalog = CasRefCatalog::read(backend, layout);
                current_catalog.life_index.throwIfAmbiguous("CAS GC ref cleanup revalidation");
                const auto current_entry_it = std::lower_bound(
                    current_catalog.catalog.entries.begin(), current_catalog.catalog.entries.end(), ns,
                    [](const CatalogEntry & entry, const RootNamespace & needle) { return entry.ns < needle; });
                const std::optional<NamespaceLifeId> current_life
                    = current_catalog.life_index.resolve(life.incarnation);
                if (current_catalog.token != folded.catalog_cut->token
                    || current_entry_it == current_catalog.catalog.entries.end()
                    || current_entry_it->ns != ns || *current_entry_it != observed_entry
                    || !current_life || *current_life != life)
                {
                    LOG_DEBUG(logger,
                        "CAS GC ref cleanup stopped before deleting '{}': catalog observation/life moved",
                        key);
                    return false;
                }

                const auto current_state_object = backend.get(layout.gcStateKey());
                if (!current_state_object)
                {
                    LOG_WARNING(logger,
                        "CAS GC ref cleanup stopped before deleting '{}': mandatory gc/state is absent",
                        key);
                    return false;
                }
                const GcState current_state = decodeGcState(current_state_object->bytes);
                if (current_state.lease.owner != adopted_lease.owner
                    || current_state.lease.seq != adopted_lease.seq)
                {
                    LOG_DEBUG(logger,
                        "CAS GC ref cleanup stopped before deleting '{}': GC fence moved",
                        key);
                    return false;
                }
            }
            catch (const std::exception & e)
            {
                LOG_WARNING(logger,
                    "CAS GC ref cleanup stopped before deleting '{}': authority revalidation failed: {}",
                    key, e.what());
                return false;
            }

            backend.deleteExact(key, h.token);
            ProfileEvents::increment(ProfileEvents::CASRefCleanupObjectsDeleted);   /// cleanup object deletion
            return true;
        };

        const auto row_it = folded.fold_seal.ref_lives.find(entry_it->incarnation);
        const RefTxnId durable_cursor = row_it != folded.fold_seal.ref_lives.end()
            ? row_it->second.coverage.last_folded_ref_id
            : RefTxnId{};

        /// Only a checkpoint-named RECOVERY TRIPLE licenses deletion. A listed snapshot may have landed
        /// before its publisher's checkpoint CAS, so it must never be promoted into cleanup authority.
        /// Validate the exact same-id non-seal `_log` and `_snap` through recovery's one shared helper;
        /// failure is confined to this namespace and leaks its listed objects for a later round.
        std::optional<RefCkpt> checkpoint;
        if (const auto ckit = folded.checkpoints.find(ns_str); ckit != folded.checkpoints.end())
            checkpoint = ckit->second;
        if (!checkpoint || !checkpoint->checkpoint_snapshot_id)
            continue;
        const RefTxnId checkpoint_snapshot_id = *checkpoint->checkpoint_snapshot_id;

        std::optional<RefTxnId> retained_log_proof;
        try
        {
            retained_log_proof = readCheckpointSnapshotBase(backend, layout, life, *checkpoint).predecessor_seal_id;
        }
        catch (const Exception & e)
        {
            LOG_WARNING(logger,
                "CAS GC ref cleanup retained namespace '{}': checkpoint base {} is not a valid recovery triple: {}",
                ns_str, renderRefTxnId(checkpoint_snapshot_id), e.message());
            continue;
        }

        const RefCleanupPlan plan = planRefCleanup(
            listing, durable_cursor, checkpoint_snapshot_id, retained_log_proof);
        for (const RefTxnId & log_id : plan.deletable_logs)
        {
            /// Cumulative per-round cap, never amortized against the per-key fail-close
            /// validation `deleteRefObject` performs (HEAD + catalog re-read + gc/state re-read before
            /// every exact delete stays exactly as expensive per key as before). Exhaustion simply stops
            /// the round's cleanup pass here; `planRefCleanup` recomputes the SAME remaining candidates
            /// from durable state next round, so nothing here needs its own cursor.
            if (!work_budget.refCleanupAvailable())
                return;
            if (!deleteRefObject(layout.refLogKey(life, log_id)))
                return;
            ++work_budget.ref_cleanup_objects_used;
        }
        for (const RefTxnId & snap_id : plan.deletable_snapshots)
        {
            /// Task 5's rule, asserted where it is acted on rather than only where it is computed: the
            /// snapshot the checkpoint names is the one a recovering reader will sample, so it must
            /// survive every cleanup that the same checkpoint authorized.
            chassert(snap_id < checkpoint_snapshot_id);
            if (!work_budget.refCleanupAvailable())
                return;
            if (!deleteRefObject(layout.refSnapshotKey(life, snap_id)))
                return;
            ++work_budget.ref_cleanup_objects_used;
        }
    }
}

namespace
{
/// GC-metadata wholesale delete of every object under `prefix`. Returns the number of objects deleted.
/// `bounded_remaining` caps how many objects this call may delete (0 => stop immediately, deleting none).
///
/// Token source: the in-memory and S3 backends surface a per-key token through `list`
/// (`supportsListTokens()`), so `deleteExact` straight from the listed token; otherwise HEAD first.
///
/// 404 / NotFound is FAIL-OPEN: an object that vanished between LIST and delete (a concurrent crashed
/// attempt, or a racing prune) is already reclaimed — never throw on a benign missing GC-internal object
/// during a prune (it would only wedge GC). A genuine TokenMismatch is
/// likewise tolerated here: the object was rewritten under us (another attempt is live at this key) — the
/// safe direction during a best-effort prune is to leave it for a later round, never to force-delete.
/// `out_fully_drained`, when set, reports whether the WHOLE prefix was exhausted (every listed key
/// visited) rather than the call stopping early because `bounded_remaining` ran out. A
/// caller advancing a monotone cursor past this prefix must consult this: a `false` here means objects
/// remain, and the cursor must stay put so a later round's fresh budget can finish the same prefix
/// instead of stranding the remainder permanently. `bounded_remaining == 0` conservatively reports
/// `false` (nothing was even examined, so completeness cannot be claimed).
uint64_t deletePrefixWholesale(Backend & backend, const String & prefix, uint64_t bounded_remaining,
                               bool * out_fully_drained)
{
    if (out_fully_drained)
        *out_fully_drained = false;
    static constexpr size_t kListPageLimit = 1000;
    uint64_t deleted = 0;
    String cursor;
    while (deleted < bounded_remaining)
    {
        ListPage page = backend.list(prefix, cursor, kListPageLimit);
        /// One page fetched, not one increment per listed key below.
        ProfileEvents::increment(ProfileEvents::CASGCEnumerationPages);
        for (const auto & listed : page.keys)
        {
            if (deleted >= bounded_remaining)
                return deleted;
            if (listed.token.has_value())
            {
                /// deleteExact tolerates NotFound (returns Kind::NotFound) and TokenMismatch — both are
                /// benign here (already gone / rewritten by a live attempt); do not throw.
                backend.deleteExact(listed.key, *listed.token);
            }
            else if (const auto head = backend.head(listed.key); head.exists)
            {
                backend.deleteExact(listed.key, head.token);
            }
            ++deleted;
        }
        if (page.next_cursor.empty())
        {
            if (out_fully_drained)
                *out_fully_drained = true;
            break;
        }
        cursor = page.next_cursor;
    }
    return deleted;
}
}

void Gc::pruneSupersededGenerations(uint64_t adopted_generation, uint64_t attempt, GcState & next,
                                    const std::set<uint64_t> & referenced_generations,
                                    bool suppress_destructive, GcRoundWorkBudget & work_budget)
{
    /// GATED, and `snap_pruned_through` stays where it is. The cursor is a monotone high-water mark the
    /// wholesale prune never revisits, so advancing it over a generation this round declined to delete
    /// would strand that generation's whole prefix with no reclaimer left (the hand-off only covers
    /// generations a LIVE ref moved off, not ones skipped for suppression).
    if (suppress_destructive)
        return;

    const uint64_t keep = store->poolConfig().gc_snapshot_generations_to_keep;
    if (keep == 0)
        return;   /// keep ALL (debug/forensics — replay GC's in-degree view as-of a past round)

    Backend & backend = store->backend();
    const Layout & layout = store->layout();

    static constexpr uint64_t kMaxPrunePerRound = 64;   /// bound the per-round prune burst

    /// (1) WHOLESALE generation-retention (correctness). A single generation may hold artifacts under
    /// MULTIPLE attempts: every round mints a fresh `lease.seq` (= attempt), and a deposed leader writes
    /// its fold_seal/runs/cleanup AND its attempt-scoped retired/outcomes sets under its OWN unadopted
    /// attempt before its CAS fails. The old per-key single-attempt prune (keyed on the final
    /// snap_attempt) therefore leaked every non-adopted attempt's debris. Instead, LIST the whole
    /// `gc/gen/<g>/` prefix and delete every listed object — reclaiming ALL attempts of `g`, including
    /// the retired/ and outcomes/ sets that now live under `gc/gen/<g>/attempt/<a>/`. Bounded per round
    /// by generation count (`kMaxPrunePerRound`) AND by the round's shared object-count work budget;
    /// fail-open on 404. `snap_pruned_through` advances over every generation the loop
    /// FULLY processes this round — ref-retained (skipped) generations count as fully processed (there
    /// is nothing left for THIS loop to do to them), but a generation whose delete the work budget cut
    /// short does NOT: the loop stops there, so the cursor never strands a partially-drained prefix
    /// behind it. It is a monotone high-water cursor, NOT a proof that everything below it is gone.
    if (adopted_generation > keep)
    {
        const uint64_t prune_floor = adopted_generation - keep;   /// prune generations <= prune_floor
        uint64_t g = next.snap_pruned_through + 1;
        uint64_t pruned = 0;
        for (; g <= prune_floor && pruned < kMaxPrunePerRound; ++g, ++pruned)
        {
            /// A generation whose run the LIVE adopted seal still
            /// references (reference-parent carry: an idle shard's current run physically lives at an
            /// older generation's key) must NOT be reclaimed — deleting it would strand the live seal's
            /// ref. Skip its prefix delete; the run stays alive as long as it is referenced. NOTE the
            /// cursor still advances past this skipped generation (see the `g - 1` cursor note above), so
            /// the wholesale prune NEVER revisits it once it is behind the cursor. LEAK-FREEDOM therefore
            /// rests on the post-CAS hand-off in `runRegularRound`: the round that finally moves the
            /// ref OFF this generation (a later delta writes a fresh run) wholesale-deletes this whole
            /// prefix right after its CAS. So every formerly-referenced generation is eventually FULLY
            /// reclaimed — either here (if the ref moved off before the cursor reached it, WholesalePrune*
            /// test) or by the hand-off (if the cursor passed it while still referenced, HandOffDeletes*
            /// test). Until the ref moves it persists safely (bounded: one small run per shard).
            if (referenced_generations.contains(g))
            {
                LOG_TRACE(logger,
                    "CAS GC prune: retaining generation {} — still referenced by the live adopted seal",
                    g);
                continue;
            }
            /// `bounded_remaining` is the round's remainder shared across every PRUNE
            /// `deletePrefixWholesale` call this round (never `UINT64_MAX`; the post-CAS hand-off draws
            /// from its own separate reserve, never this one). A generation whose prefix
            /// this call cannot FULLY drain within the remaining budget must not let the cursor advance
            /// past it -- `snap_pruned_through` is a monotone high-water mark this loop never revisits,
            /// so stranding a partially-drained generation behind it would leak the remainder forever.
            /// Stop the loop here; `g - 1` (the previous, fully-processed generation) is what gets
            /// persisted below.
            const uint64_t remaining = work_budget.prefixWholesaleRemaining();
            if (remaining == 0)
                break;
            bool fully_drained = false;
            const uint64_t reclaimed = deletePrefixWholesale(
                backend, layout.gcGenPrefix(g), remaining, &fully_drained);
            work_budget.prefix_wholesale_objects_used += reclaimed;
            if (!fully_drained)
                break;
        }
        next.snap_pruned_through = g - 1;   /// highest generation FULLY processed this round
    }

    /// (2) NO per-round current-generation attempt-sweep (KISS). A previous revision LISTed the FOLD
    /// generation's `gc/gen/<G_f>/` prefix EVERY completed round to delete non-adopted attempts with
    /// `a < snap_attempt` — debris a deposed leader of the just-completed round left under its own
    /// (unadopted) `lease.seq`. That per-round LIST was steady-state S3 budget spent for the RARE case
    /// of a concurrent-leader collision (the GC-DISCOVERY-LIST-QUADRATIC concern), so it is removed.
    ///
    /// The wholesale generation-retention prune in (1) is now the SOLE reclaimer of ALL attempt debris,
    /// including a deposed leader's: every artifact of generation `g` — across every attempt — lives
    /// under `gc/gen/<g>/`, and the prefix-delete in (1) reclaims the whole subtree once `g` ages past
    /// `keep`. Deposed-leader current-generation debris is therefore BOUNDED space (one collision leaves
    /// at most a handful of small objects per generation) that waits at most `keep` completion-advances
    /// to be reclaimed. This trades ~`keep` rounds of reclaim latency on (rare) concurrent-leader
    /// collisions for eliminating a per-round LIST on the common (single-leader) path. When `keep == 0`
    /// (keep-all / forensics mode) nothing is reclaimed by design — same as before.
    (void)attempt;
}

std::optional<CasFoldSeal> Gc::readFoldSeal(uint64_t generation, uint64_t attempt)
{
    if (const auto got = store->backend().get(store->layout().foldSealKey(generation, attempt)))
        return decodeFoldSeal(
            got->bytes, store->layout(), store->poolConfig().gc_shards, generation);
    return std::nullopt;
}

namespace
{

/// `Layout::parseRefObjectKey` for a key coming from a global `cas/ns/stream/` enumeration, with the one
/// refusal it can raise absorbed into the ordinary "unrecognized" answer.
///
/// A ref object naming no life (the un-incarnated shape) is the single malformed key the parser
/// REFUSES by name instead of classifying as debris, and both global enumerations run OUTSIDE the
/// fold's catch. Letting the refusal escape one of them would not merely lose a round: GC is the only
/// thing that could ever delete the key, so every future round would die on it too, with nothing able
/// to clear it. Absorbed here, the key stays in the enumeration's raw key list, unindexed, exactly
/// like every other malformed shape, and `groupRefKeys` raises it once inside the fold's catch --
/// louder than before (an anomaly plus `suppress_destructive`), and without the wedge.
///
/// Only `CORRUPTED_DATA` is absorbed: any other exception is a real failure of the enumeration itself.
std::optional<ParsedRefObjectKey> parseRefObjectKeyForEnumeration(const Layout & layout, const String & key)
{
    try
    {
        return layout.parseRefObjectKey(key);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::CORRUPTED_DATA)
            throw;
        return std::nullopt;
    }
}

}

std::vector<NamespaceLifeId> Gc::discoverUniverse()
{
    /// One catalog GET replaces the pool-wide `LIST(cas/ns/stream/)` this used to run to
    /// discover WHICH namespaces exist. `Creating` entries are excluded -- spec §3, "no publication can
    /// exist" while a namespace is still being created, so there is nothing here for a discovery path to
    /// walk; `Live` and `Removing` entries are both returned. `fromCatalogEntry` mints each life directly
    /// from the row that is its own authority for both fields, never from a listed key (which could name
    /// a DEAD incarnation of the same namespace name).
    ///
    /// The filter itself lives in `CasRefCatalog::liveUniverse` (review Important C) -- fsck's own
    /// reachability walk needed the identical catalog-authoritative set and is not this class, so the
    /// filter moved to where both can share it rather than grow a second copy that could disagree.
    return CasRefCatalog::liveUniverse(store->backend(), store->layout());
}

bool Gc::graduationDue(const GcState & state, uint64_t current_round)
{
    /// Retired-in-snapshot: the graduation signal is read from the adopted fold seal's per-shard
    /// `condemned_summary` — ZERO backend I/O beyond the single seal read. A summary distilled from this
    /// generation's `kCondemned` rows says, per shard, how many entries are `delete_pending` (a graduation
    /// is already published) and the oldest non-pending condemn round (one crosses the floor once
    /// `condemn_round < current_round`).
    if (state.snap_generation == 0)
        return false;   /// fresh pool: nothing condemned yet, nothing to graduate.

    /// FAIL-CLOSED: a missing / undecodable seal, or a summary that is not TOTAL over gc_shards, is
    /// corrupt GC bookkeeping — force a FOLD so the round's own fail-closed path surfaces it, never a
    /// silent defer (matching the fold's throw-on-missing-adopted-seal treatment).
    std::optional<CasFoldSeal> seal;
    try
    {
        seal = readFoldSeal(state.snap_generation, state.snap_attempt);
    }
    catch (...)
    {
        return true;   /// undecodable seal => fail-closed force-fold
    }
    if (!seal)
        return true;
    for (uint64_t shard = 0; shard < state.gc_shards; ++shard)
    {
        const auto it = seal->condemned_summary.find(shard);
        if (it == seal->condemned_summary.end())
            return true;   /// summary not total over gc_shards => fail-closed force-fold
        if (it->second.pending_total > 0 || it->second.oldest_nonpending_condemn_round < current_round)
            return true;
    }
    return false;
}

RefScanSummary Gc::enumerateRefPrefix()
{
    /// One full enumeration of `cas/ns/stream/`: the raw keys, plus a lenient per-life index of the
    /// Log-kind ids among them. Lenient is deliberate — a malformed key is kept in `keys` and left
    /// unindexed, so the STRICT validation (and the round-abort it can raise) happens exactly once, in
    /// the fold's `groupRefKeys`. That holds for EVERY malformed shape: the one the parser refuses by
    /// name is absorbed per key by `parseRefObjectKeyForEnumeration`, which is what keeps this
    /// enumeration -- which runs before the fold, outside its catch -- unable to wedge the round.
    const Layout & layout = store->layout();
    Backend & backend = store->backend();

    RefScanSummary scan;
    static constexpr size_t kListPageLimit = 1000;
    size_t count_in_page = 0;
    forEachListedKey(backend, layout.casRefsPrefix(), [&](const ListedKey & lk)
    {
        scan.keys.push_back(lk.key);
        const auto parsed = parseRefObjectKeyForEnumeration(layout, lk.key);
        if (parsed)
        {
            scan.listed_lives.insert(parsed->life_id);
            if (parsed->kind == RefObjectKind::Log)
            {
                scan.logs_by_life[parsed->life_id].insert(parsed->txn_id);
                RefTxnId & g = scan.max_log_by_life[parsed->life_id];
                if (g < parsed->txn_id)
                    g = parsed->txn_id;
            }
        }
        if (++count_in_page == kListPageLimit)
        {
            count_in_page = 0;
            ProfileEvents::increment(ProfileEvents::CASRefGlobalListPages);
        }
    }, kListPageLimit, onGcEnumerationPage);
    /// The walk's `backend.list` lands at least once even for an empty/undersized final page --
    /// count it (one increment per physical LIST call).
    if (count_in_page > 0 || scan.keys.empty())
        ProfileEvents::increment(ProfileEvents::CASRefGlobalListPages);
    return scan;
}

RoundInput Gc::listRefPrefix(const GcState & state)
{
    /// The round's ONE hint enumeration, followed by the parent coverage and authoritative catalog cut
    /// needed to build the DEFER/FOLD walk plan. The caller computes the DEFER signal from that frozen
    /// plan. A listed id absent from the later cut is dead, inert debris: it contributes no work and
    /// cannot force DEFER.
    RefScanSummary scan = enumerateRefPrefix();
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(store->backend(), store->layout());
    /// TEST SEAM: see `setPostHotScanCatalogReadHookForTest`. Moved into a local before invoking (the
    /// same reason `create_namespace_step1_pre_read_hook_for_test` is swapped rather than called
    /// directly): a hook that reassigns the member from inside its own body would otherwise reassign
    /// the very `std::function` executing it.
    if (post_hot_scan_catalog_read_hook_for_test)
    {
        std::function<void()> hook_to_run;
        std::swap(hook_to_run, post_hot_scan_catalog_read_hook_for_test);
        hook_to_run();
    }
    catalog_cut.life_index.throwIfAmbiguous("CAS GC hot scan");
    store->reconcileRefCatalogCut(catalog_cut);

    const std::optional<CasFoldSeal> seal = readFoldSeal(state.snap_generation, state.snap_attempt);
    if (!seal && state.snap_generation > 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS GC hot scan: adopted fold seal (generation {}, attempt {}) is missing",
            state.snap_generation, state.snap_attempt);
    if (seal)
        scan.parent_ref_lives = seal->ref_lives;

    for (const NamespaceLifePhysicalId life_id : scan.listed_lives)
        if (!catalog_cut.life_index.resolve(life_id))
            ++scan.dead_life_debris;

    return RoundInput{std::move(scan), catalog_cut};
}

RebuildReport Gc::rebuildBaseline(bool force)
{
    /// The `gc/state` disaster-recovery command.
    /// DRY: the engine is the round's own bricks — one catalog cut for the universe,
    /// foldManifestEdges(+1) for edge emission, foldDeltasIntoGeneration with EMPTY priors
    /// (attempt-iterated for O(budget) memory), computeHeartbeatFloor for the round mint.
    /// Writes ONLY the GC plane; namespace streams/state, manifests, and blobs are read-only inputs;
    /// the rebuild never deletes them.
    RebuildReport rep;
    Backend & backend = store->backend();
    const Layout & layout = store->layout();

    /// Read bookkeeping health before the lease (the lease acquire on an absent state CREATES a
    /// bootstrap body, which must not make scenario (а) look healthy). A generation-0 ref-baseline
    /// check is deliberately postponed to the sole post-LIST work cut below.
    /// The prior seal, when `gc/state` claims one. It is the ONLY place holds live, so the rebuild
    /// either reads it and carries every hold forward, or refuses (see the refusal below).
    std::optional<CasFoldSeal> prior_seal;
    bool healthy = false;
    bool validate_generation_zero_ref_baseline = false;
    {
        const auto got = backend.get(layout.gcStateKey());
        /// The state's own decode stays inside its own try: an undecodable `gc/state` IS scenario (а),
        /// the disaster this command exists for. The prior-seal refusal below must NOT be swallowed by
        /// that catch, so the seal is read outside it.
        std::optional<GcState> decoded;
        if (got)
        {
            try
            {
                decoded = decodeGcState(got->bytes);
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// undecodable state = scenario (а)
            }
        }
        if (decoded)
        {
            const GcState & st = *decoded;
            healthy = true;
            if (st.snap_generation == 0)
            {
                /// This check needs the rebuild universe. Delay it until after lease acquisition, the
                /// zero-mutation generation-0 drain, the completed hot LIST and the sole fresh work cut.
                /// No pre-lease catalog snapshot may become authority for successor construction.
                validate_generation_zero_ref_baseline = true;
            }
            if (st.snap_generation > 0)
            {
                /// THE REFUSAL (spec r9-1). The prior seal is where every hold lives, and a rebuild
                /// rewrites coverage from owner state -- so with no readable seal it would hand back a
                /// baseline that LOOKS proven while silently discarding holds it cannot even enumerate.
                /// The alternative, a "pool-wide hold" on the rebuilt baseline, is not representable:
                /// every hold names a position the fold must fold THROUGH, and a pool-wide one would
                /// need an invented position nothing could ever resolve. So the honest branch is the
                /// already-safe one -- refuse, and name pool recreation. FORCE does not buy past this:
                /// force means "rebuild deliberately", never "drop the holds". It THROWS rather than
                /// returning `rep.refusal` because, unlike every other refusal here, no flag and no
                /// retry makes it succeed.
                std::optional<CasFoldSeal> seal;
                try
                {
                    seal = readFoldSeal(st.snap_generation, st.snap_attempt);
                }
                catch (const Exception & e)
                {
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS GC rebuild: the prior fold seal (generation {}, attempt {}) is UNDECODABLE "
                        "({}), so the holds it carries cannot be read. A rebuild that dropped them would "
                        "bless a baseline whose frontier is unproven. GC refuses to rebuild; this pool "
                        "must be recreated.",
                        st.snap_generation, st.snap_attempt, e.message());
                }
                if (!seal)
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS GC rebuild: the prior fold seal (generation {}, attempt {}) is MISSING under "
                        "a gc/state that claims it, so the holds it carries cannot be read. A rebuild "
                        "that dropped them would bless a baseline whose frontier is unproven. GC refuses "
                        "to rebuild; this pool must be recreated.",
                        st.snap_generation, st.snap_attempt);
                for (const RunRef & r : seal->blob_target_runs)
                    if (!backend.head(r.key).exists)
                        healthy = false;
                prior_seal = std::move(seal);
                rep.adopted_seal_generation = st.snap_generation;
            }
        }

        /// NO ADOPTED BASELINE IS NAMED: `gc/state` is absent, undecodable, or sits at generation 0.
        /// The holds live in the SEAL, not in the pointer to it, so stopping here would make losing the
        /// pointer -- the LESSER corruption -- produce a hold-free baseline, while an unreadable seal
        /// refuses. That asymmetry is inverted, and it matters because holds are not re-derivable:
        /// `WitnessDisappeared` names a record that is gone, so the next walk reads a clean frontier
        /// and hands the namespace exactly the proof the hold exists to deny.
        ///
        /// So find the newest fold seal OBJECT by enumeration and carry ITS holds. This keeps the
        /// pool's disaster recovery intact -- losing `gc/state` on a lived-in pool is the scenario this
        /// command exists for -- while making a hold-free baseline over a pool that had holds
        /// unreachable.
        ///
        /// An unadopted deposed-leader attempt can be the newest object, and carrying its holds
        /// over-holds RATHER THAN under-holds -- but that claim needs its qualifier, because it is not
        /// universal. It holds outside the lying-store corner. The exception is a deposed attempt that
        /// FOLDED THROUGH a position on a read that was transient: its seal records the hold as
        /// cleared, the adopted leader's seal still holds it, and adopting the deposed one loses that
        /// hold. Bounded by the refusal above (a seal above the listing's maximum refuses outright) and
        /// by the fact that both attempts walked the same cursor state.
        if (!decoded || decoded->snap_generation == 0)
        {
            const auto newest = newestFoldSealRef();
            /// Absent here is the VIRGIN verdict, and it is the one path left on which a durable hold
            /// can still be dropped without anything failing. It is REPORTED on the command's own row,
            /// not merely logged: this command is run by hand during a disaster, and its operator is
            /// exactly the person who needs to know the clean slate was inferred rather than proved.
            rep.virgin_by_enumeration = !newest.has_value();
            if (newest)
            {
                std::optional<CasFoldSeal> seal;
                try
                {
                    seal = readFoldSeal(newest->first, newest->second);
                }
                catch (const Exception & e)
                {
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS GC rebuild: gc/state names no adopted baseline and the newest fold seal "
                        "(generation {}, attempt {}) is UNDECODABLE ({}), so the holds this pool carries "
                        "cannot be read. A rebuild that dropped them would bless a baseline whose "
                        "frontier is unproven. GC refuses to rebuild; this pool must be recreated.",
                        newest->first, newest->second, e.message());
                }
                if (!seal)
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS GC rebuild: gc/state names no adopted baseline and the newest fold seal "
                        "(generation {}, attempt {}) vanished between the enumeration and the read, so "
                        "the holds this pool carries cannot be read. GC refuses to rebuild; this pool "
                        "must be recreated.",
                        newest->first, newest->second);
                prior_seal = std::move(seal);
                rep.adopted_seal_generation = newest->first;
            }
            /// else: no fold seal object anywhere. That is the ONE proof that dropping nothing is safe
            /// -- a pool that never sealed a baseline has no hold to lose.
        }
    }
    if (healthy && !force && !validate_generation_zero_ref_baseline)
    {
        rep.refusal = "gc/state and every referenced artifact are healthy — a rebuild would discard "
                      "live bookkeeping; re-run with FORCE to rebuild deliberately";
        return rep;
    }

    /// Lease: single leader vs regular rounds and other rebuilds. On an absent state this CREATES
    /// a lease-bearing bootstrap body whose token anchors our final CAS. allow_steal=false: this is a
    /// manual disaster-recovery command, same reasoning as the manual GC round (runRegularRound's doc
    /// comment) — though it is structurally moot here too (a fresh one-shot `this` with
    /// has_observation==false always takes the non-steal branch on its one and only call), pass it
    /// explicitly rather than rely on that invariant.
    GcState state;
    Token state_token;
    if (!acquireOrRenewLease(state, state_token, /*allow_steal=*/false))
    {
        rep.refusal = "another GC leader holds the lease";
        return rep;
    }

    /// Healthy `FORCE REBUILD` shares the same parent-authorized barrier as an ordinary round. A
    /// damaged-state rebuild has no adopted parent (`snap_generation == 0`) and the barrier performs
    /// zero catalog mutations. Only after that distinction is resolved do we complete the hot LIST and
    /// take the sole fresh work cut.
    const CatalogLifecycleReconcileResult drain_result = drainCompletedRemoving(state);
    for (const NamespaceLifeId & retired_life : drain_result.retired_lives)
        store->invalidateRemovedCatalogLife(retired_life);
    if (drain_result.authority_status != AuthorityStatus::Authoritative
        || drain_result.catalog_resolution != CatalogResolution::DrainComplete)
        throwCasWriteRetryLater("CAS GC rebuild lost authority before the catalog settled");
    const RefScanSummary rebuild_ref_scan = enumerateRefPrefix();
    const CasRefCatalog::Snapshot rebuild_work_catalog_cut = CasRefCatalog::read(backend, layout);

    RefScanSummary rebuild_round_scan = rebuild_ref_scan;
    if (prior_seal)
        rebuild_round_scan.parent_ref_lives = prior_seal->ref_lives;
    const RefPlan rebuild_walk_plan = buildRefWalkPlan(
        RoundInput{std::move(rebuild_round_scan), rebuild_work_catalog_cut});
    const std::vector<NamespaceLifeId> rebuild_walk_universe = rebuild_walk_plan.lives();
    /// The exact checkpoint sample is paired with the same frozen catalog cut that chose the rebuild
    /// universe. `recoverRefTableDetailedFromAuthority` deliberately has no internal catalog or
    /// checkpoint read: a later cut could admit a different life or frontier than the one every other
    /// part of this rebuild is using.
    const CheckpointWitnesses rebuild_checkpoints = readCheckpointWitnesses({}, rebuild_walk_plan.catalogCut());

    if (validate_generation_zero_ref_baseline)
    {
        /// A generation-0 state is healthy only when no table proves that a now-lost cursor cleaned
        /// covered logs. The check consumes the same catalog-built universe as reconstruction; it does
        /// not own an earlier catalog cut or a second admission rule.
        for (const NamespaceLifeId & life : rebuild_walk_universe)
        {
            std::vector<String> table_keys;
            forEachListedKey(backend, layout.namespaceStreamPrefix(life),
                [&](const ListedKey & lk) { table_keys.push_back(lk.key); }, 1000, onGcEnumerationPage);
            std::map<NamespaceLifePhysicalId, RefTableListing> grouped;
            try
            {
                grouped = groupRefKeys(layout, table_keys);
            }
            catch (const Exception & e)
            {
                if (e.code() != ErrorCodes::CORRUPTED_DATA)
                    throw;
                healthy = false;
                break;
            }
            const auto grouped_it = grouped.find(life.incarnation);
            if (grouped_it != grouped.end() && !grouped_it->second.snapshots.empty()
                && (grouped_it->second.logs.empty()
                    || grouped_it->second.snapshots.back() < grouped_it->second.logs.front()))
            {
                healthy = false;
                break;
            }
        }
        if (healthy && !force)
        {
            rep.refusal = "gc/state and every referenced artifact are healthy — a rebuild would discard "
                          "live bookkeeping; re-run with FORCE to rebuild deliberately";
            return rep;
        }
    }

    /// Numbering, part 1: generation above ANY surviving gc/gen prefix (putDeterministicArtifact
    /// must never collide with debris of the lost era).
    uint64_t max_gen = state.snap_generation;
    {
        const String gen_prefix = layout.gcGenPrefix(0);
        const String top = gen_prefix.substr(0, gen_prefix.size() - 2);   /// ".../gc/gen/"
        forEachListedKey(backend, top, [&](const ListedKey & k)
        {
            const size_t from = top.size();
            const size_t slash = k.key.find('/', from);
            if (slash == String::npos)
                return;
            try
            {
                max_gen = std::max(max_gen, static_cast<uint64_t>(std::stoull(k.key.substr(from, slash - from))));
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// Foreign key shape under `gc/gen` is debris, not a numbering input.
            }
        }, 1000, onGcEnumerationPage);
    }
    const uint64_t generation = max_gen + 1;
    const uint64_t budget = rebuild_edge_budget_override ? rebuild_edge_budget_override
                                                         : store->poolConfig().rebuild_edge_budget;

    /// Per-gc-shard attempt-iterated fold state: batch k folds with attempt k and the previous
    /// attempt's runs as priors; the FINAL attempt's runs go into the seal.
    const uint64_t gc_shards = state.gc_shards ? state.gc_shards : store->poolConfig().gc_shards;
    std::vector<std::vector<BlobDelta>> buckets(gc_shards);
    std::vector<std::vector<RunRef>> prior_runs(gc_shards);
    std::vector<uint64_t> attempt_of(gc_shards, 0);
    /// The fold is EDGE-ONLY here: a rebuild condemns nothing (spec §7, and the deletion below), so no
    /// condemn round is stamped and no head source is supplied. `current_round` 0 graduates nothing and
    /// `condemn_round` 0 with an empty `head_blob` mints no `kCondemned` row -- this call is
    /// `foldDeltasIntoGeneration`'s pure edge form.
    auto flush_shard = [&](uint64_t shard)
    {
        if (buckets[shard].empty())
            return;
        std::vector<RunRef> out;
        foldDeltasIntoGeneration(backend, layout, prior_runs[shard], generation, ++attempt_of[shard],
                                 shard, std::move(buckets[shard]), out,
                                 /*current_round*/0, /*condemn_round*/0, /*head_blob*/{},
                                 /*peek_head*/{}, /*confirm_condemned_marker*/{},
                                 /*out_retired*/nullptr, /*suppress_destructive*/false,
                                 /// Probe B2 does not apply to the rebuild: it derives edges from raw
                                 /// owner STATE, not from a stream of ref transactions, so there is no
                                 /// transaction whose deltas could go unapplied and no fold cursor to
                                 /// advance past one. Every delta it emits carries ordinal 0.
                                 /*out_applied_by_txn_ordinal*/nullptr);
        buckets[shard].clear();
        prior_runs[shard] = std::move(out);
    };
    auto route_deltas = [&](std::vector<BlobDelta> & deltas)
    {
        rep.edges += deltas.size();
        for (BlobDelta & d : deltas)
        {
            const uint64_t shard = blobShard(d.ref, gc_shards);
            buckets[shard].push_back(std::move(d));
            if (buckets[shard].size() >= budget)
                flush_shard(shard);
        }
        deltas.clear();
    };

    /// `rebuild_walk_plan` was frozen immediately after the completed hot LIST and sole fresh catalog
    /// cut. Listed ids absent from that later cut are inert dead-life debris and cannot mint work or
    /// refuse reconstruction.
    std::set<String> seen_ns;
    std::set<String> owned_manifest_keys;
    CasFoldSeal seal;
    seal.generation = generation;
    seal.parent_generation = state.snap_generation;
    seal.ref_lives = rebuild_walk_plan.successorFoldStates();
    /// Life ids whose hold this rebuild MINTED (see `minted_here`); they are stamped with the retry
    /// round once it is known, and nothing else is touched.
    std::set<UInt128> minted_hold_lives;
    uint64_t max_fence_round = 0;
    std::map<ManifestId, Token> mf_cleanup_unused;

    for (const NamespaceLifeId & life : rebuild_walk_universe)
    {
        const RootNamespace & ns = life.ns;
        seen_ns.insert(ns.string());
        ++rep.shards;

        /// Recover from the exact catalog row and exact checkpoint paired with this plan. A visible
        /// log above `committed_through` is not logical history yet, and a Live/Removing row without a
        /// readable checkpoint has no bounded recovery frontier; both cases must refuse rather than
        /// letting a stream LIST decide what this baseline protects.
        const auto entry_it = std::lower_bound(
            rebuild_walk_plan.catalogCut().catalog.entries.begin(), rebuild_walk_plan.catalogCut().catalog.entries.end(), ns,
            [](const CatalogEntry & entry, const RootNamespace & needle) { return entry.ns < needle; });
        chassert(entry_it != rebuild_walk_plan.catalogCut().catalog.entries.end());
        chassert(entry_it->ns == ns);
        chassert(entry_it->incarnation == life.incarnation);
        if (const auto bad_checkpoint = rebuild_checkpoints.undecodable.find(ns.string());
            bad_checkpoint != rebuild_checkpoints.undecodable.end())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS GC rebuild: catalog life {} has an undecodable checkpoint: {}", ns.string(), bad_checkpoint->second);
        std::optional<RefCkpt> checkpoint;
        if (const auto checkpoint_it = rebuild_checkpoints.recovery_checkpoints.find(ns.string());
            checkpoint_it != rebuild_checkpoints.recovery_checkpoints.end())
            checkpoint = checkpoint_it->second;
        const RecoveredRefTable recovered = recoverRefTableDetailedFromAuthority(
            backend, layout, *entry_it, checkpoint);
        const RefTableState & st = recovered.state;

        RefCoverage cov;
        cov.classification = 2;   /// Folded (full coverage) unless a bodiless precommit clamps
        cov.last_folded_ref_id = st.getGreatestApplied();
        /// Whether the hold on this row was minted BY THIS REBUILD (and so still owes a retry round)
        /// rather than carried from the prior seal. Tracked explicitly instead of by looking for a
        /// `next_retry_round` of 0: 0 is a perfectly good wire value, and a carried hold that happened
        /// to hold it would have its backoff silently rewritten by the stamping pass.
        bool minted_here = false;

        std::vector<BlobDelta> deltas;

        /// Committed owners: a missing/invalid body under a committed ref is DATA LOSS the rebuild must
        /// not bless (INV_NO_DANGLE) -- refuse.
        for (const auto [ref_name, row] : st.getCommitted())
        {
            const ManifestId id{ns, row.manifest_ref};
            owned_manifest_keys.insert(layout.manifestKey(id));
            if (!foldManifestEdges(id, +1, deltas, mf_cleanup_unused, /*txn_ordinal=*/0))
            {
                rep.refusal = "committed ref '" + ns.string() + "/" + ref_name
                    + "' names a missing or invalid part manifest — that is DATA LOSS the rebuild "
                      "must not bless; run fsck forensics first";
                return rep;
            }
            ++rep.committed_refs;
        }

        /// Live precommits: a present body contributes edges; a bodiless one is non-activating and clamps
        /// (the fold barrier -- the first regular round folds it once the body lands).
        for (const auto & [ref_name, manifest_ref] : st.getPrecommits())
        {
            const ManifestId id{ns, manifest_ref};
            owned_manifest_keys.insert(layout.manifestKey(id));
            if (foldManifestEdges(id, +1, deltas, mf_cleanup_unused, /*txn_ordinal=*/0))
                ++rep.live_precommits;
            else
            {
                /// A bodiless live precommit leaves the rebuilt baseline INCOMPLETE for this namespace:
                /// nothing can enumerate the blobs it pins, so nothing can protect them. That is a
                /// durable hold, not a report field -- and it is the one hold the rebuild has to invent
                /// a position for, because it derives edges from owner STATE and cannot name the log
                /// that introduced the precommit (that log is below the rebuilt cursor and no round
                /// re-reads it). It holds at the FIRST POSITION THE NEXT ROUND WILL READ, which makes a
                /// quiet namespace stay held indefinitely -- the fail-close answer while the body is
                /// still missing -- and clears once the namespace makes durable progress.
                /// RESIDUAL, named rather than hidden: progress unrelated to this precommit also clears
                /// it, and the precommit's edges stay missing until another rebuild.
                cov.classification = 4;   /// Clamped
                cov.hold = RefHold{.reason = HoldReason::ManifestBodyMissing,
                                   .offending_position = RefTxnId{cov.last_folded_ref_id.writer_epoch,
                                                                 cov.last_folded_ref_id.ref_sequence + 1},
                                   .retry_count = 0,
                                   .next_retry_round = 0};   /// stamped below, once `round` is minted
                minted_here = true;
                ++rep.clamped_shards;
            }
        }
        route_deltas(deltas);

        /// HOLDS RIDE THROUGH A REBUILD VERBATIM (spec §5/§7). The rebuild derives coverage from owner
        /// state, which knows nothing about a ref-log position that would not resolve -- so without
        /// this every held row would be overwritten by a clean one and the rebuilt baseline would claim
        /// a frontier proof it does not have. `retry_count` and `next_retry_round` ride unchanged too:
        /// the rebuild retried nothing, so it must not reset the count that says how long the namespace
        /// has been stuck. The ordinary clearing rule then applies from the next round on.
        if (prior_seal)
        {
            const auto pit = prior_seal->ref_lives.find(life.incarnation);
            if (pit != prior_seal->ref_lives.end() && pit->second.coverage.hold)
            {
                cov.classification = 4;
                cov.hold = pit->second.coverage.hold;
                minted_here = false;   /// a carried hold rides VERBATIM; its retry fields are not ours
            }
        }
        if (minted_here)
            minted_hold_lives.insert(life.incarnation);

        RefLifeFoldState & row = seal.ref_lives.at(life.incarnation);
        row.coverage = cov;
    }
    rep.namespaces = seen_ns.size();

    /// Trimmed-but-live precommits: a build alive across trim has NO journal
    /// evidence; its manifests look unowned. Include edges of every manifest that is unowned AND
    /// not provably build-dead (the watermark fact) — over-protect. An unowned manifest that later
    /// dies without journal evidence leaks its edges until a future rebuild (documented, bounded,
    /// fsck-visible); provably-dead ones stay excluded (the orphan sweep owns their bodies).
    for (const String & ns_str : seen_ns)
    {
        const RootNamespace ns{ns_str};
        std::vector<BlobDelta> deltas;
        forEachListedKey(backend, layout.manifestNamespacePrefix(ns), [&](const ListedKey & k)
        {
            if (owned_manifest_keys.contains(k.key))
                return;
            /// The one shared manifest-path parser for the canonical hexadecimal manifest identifier,
            /// also used by fsck's parseBuildPrefix and the orphan sweep's parseListedManifestObject.
            const auto parsed = layout.parseManifestKey(k.key);
            if (!parsed)
                return;   /// foreign key shape — debris
            const ManifestRef & mref = parsed->ref;
            if (prefixEligible(*store, ns, BuildPrefix{mref.writer_epoch, mref.build_sequence}))
                return;   /// provably dead — the orphan sweep's territory, never an edge
            const ManifestId id{ns, mref};
            if (foldManifestEdges(id, +1, deltas, mf_cleanup_unused, /*txn_ordinal=*/0))
            {
                ++rep.unowned_alive_manifests;
                route_deltas(deltas);
            }
            /// A missing/invalid UNOWNED body is debris (no owner claims it) — skip, never refuse.
        }, 1000, onGcEnumerationPage);
    }

    /// A REBUILD CONDEMNS NOTHING (spec §7).
    ///
    /// It used to end here with a LIST of `blobs/`, condemning every listed body its traversal had not
    /// reached ("pipeline blindness repair": the fold discovers candidates by TRANSITIONS to zero, so a
    /// blob whose edges were already gone by rebuild time would have no row and never be reclaimed).
    /// The premise was that a full traversal knows every live blob. It does not. BOTH legs of the
    /// traversal above are listing-driven -- the owner replay reads the ref prefix, the trimmed-but-live
    /// pass reads the manifest prefix -- so a store that omits a durable key from one enumeration hides
    /// a LIVE owner, and this pass would then condemn the very blob that owner pins. That is
    /// r5-finding-4: one lying enumeration, and acked data is scheduled for deletion. Hiding is not
    /// hypothetical here; it is the observed `0x1430c` shape that made every ref walk arithmetic.
    ///
    /// THE NAMED RESIDUAL this leaves (Stage-A staging contract, register R4): a blob whose manifest no
    /// longer exists anywhere is unreclaimable -- nothing can enumerate it safely -- until the
    /// build/upload registry can say which uploads are in flight. It is retention, not loss, and it is
    /// bounded by that registry landing. NO substitute reclamation is added in its place: any cheaper
    /// rule that reclaims from an enumeration is the same vector wearing a different hat, and a
    /// fallback that deletes on incomplete evidence is precisely what "fail closed" forbids.
    ///
    /// Numbering, part 2: the round above every surviving fence/state/generation number.
    const uint64_t round = std::max({max_fence_round, state.round, max_gen}) + 1;

    /// Stamp the retry round on the holds THIS rebuild minted (the bodiless-precommit ones, left unset
    /// because the round was not minted yet). Carried holds are named by no key here and are not
    /// touched: their `next_retry_round` and `retry_count` ride verbatim, since a rebuild retries
    /// nothing.
    for (const UInt128 life_id : minted_hold_lives)
        if (const auto it = seal.ref_lives.find(life_id);
            it != seal.ref_lives.end() && it->second.coverage.hold)
            it->second.coverage.hold->next_retry_round = round + 1;

    for (uint64_t shard = 0; shard < gc_shards; ++shard)
        flush_shard(shard);   /// real-edge rows only: a rebuild condemns nothing, so it seeds nothing

    for (uint64_t shard = 0; shard < gc_shards; ++shard)
        for (const RunRef & r : prior_runs[shard])
            seal.blob_target_runs.push_back(r);

    /// Also fence out any dead mounts as part of the disaster-recovery pass (liveness cleanup; the
    /// returned classification counts are not needed for the round mint — graduation paces on rounds).
    /// Use the same threshold/`mount_obs` as the regular round.
    const uint64_t ttl_ms = static_cast<uint64_t>(store->poolConfig().mount_lease_ttl_ms.count());
    /// Share the identical formula with `claimMountAwaitingExpiry` via
    /// `mountObservationThresholdMs` -- see its doc comment (CasServerRoot.h).
    const uint64_t stable_threshold_ms = mountObservationThresholdMs(
        ttl_ms, static_cast<uint64_t>(store->poolConfig().mount_renew_period.count()));
    computeHeartbeatFloor(backend, layout, now_ms_fn(), mono_ms_fn(), stable_threshold_ms, mount_obs);

    /// Retired-in-snapshot: the rebuilt seal's `condemned_summary` must be TOTAL over gc_shards so a
    /// subsequent regular round reads graduation/carry decisions zero-I/O off it (and its `carryParentRefs`
    /// totality check does not fail closed). Every entry is EMPTY -- a rebuild condemns nothing -- and
    /// the totality is still owed: an ABSENT row and a zero row are different claims, and the round that
    /// reads this seal fails closed on the absent one.
    for (uint64_t shard = 0; shard < gc_shards; ++shard)
        seal.condemned_summary[shard] = CondemnedSummary{};

    /// Seal (deterministic artifact) + the single state CAS. attempt = the max per-shard attempt
    /// (>= 1 so the seal key is stable даже for an empty universe).
    uint64_t seal_attempt = 1;
    for (uint64_t a : attempt_of)
        seal_attempt = std::max(seal_attempt, a);
    validateFoldSealForWrite(seal, layout, gc_shards);
    putDeterministicArtifact(backend, layout.foldSealKey(generation, seal_attempt), encodeFoldSeal(seal));

    GcState next = state;
    next.round = round;
    next.snap_generation = generation;
    next.snap_attempt = seal_attempt;
    /// No retired set to publish: a rebuild condemns nothing (the deletion above), so there is nothing
    /// to retire in the first place. Retired-in-snapshot removed the separate `RetiredSet` object
    /// family independently of that, and the two reasons are stated apart on purpose — a future reader
    /// must not take this line as evidence that REBUILD still produces condemnations somewhere.
    next.manifest_sweep_cursor = "";
    const CasResult res = backend.casPut(layout.gcStateKey(), encodeGcState(next), state_token);
    if (res.outcome != CasOutcome::Committed)
    {
        rep.refusal = "gc/state changed under the rebuild (a competing writer) — re-run";
        return rep;
    }

    rep.performed = true;
    rep.round = round;
    rep.generation = generation;
    EventEmitter{*store}.emit([&](CasEvent & e)
    {
        e.type = CasEventType::GcRebuild;
        e.object_kind = CasEventObjectKind::Snap;
        e.round = round;
        e.gen = generation;
        e.outcome = "performed";
        e.reason = "raw baseline rebuild from owner state (gc/state disaster recovery)";
        e.detail = {{"namespaces", std::to_string(rep.namespaces)},
                    {"shards", std::to_string(rep.shards)},
                    {"committed_refs", std::to_string(rep.committed_refs)},
                    {"live_precommits", std::to_string(rep.live_precommits)},
                    {"unowned_alive_manifests", std::to_string(rep.unowned_alive_manifests)},
                    {"edges", std::to_string(rep.edges)},
                    {"clamped_shards", std::to_string(rep.clamped_shards)},
                    {"force", force ? "1" : "0"}};
    });
    return rep;
}

std::vector<Gc::PreviewEntry> Gc::previewDeletes()
{
    std::vector<PreviewEntry> out;

    const auto state_bytes = store->backend().get(store->layout().gcStateKey());
    if (!state_bytes)
        return out;
    const GcState state = decodeGcState(state_bytes->bytes);

    const Layout & layout = store->layout();
    Backend & backend = store->backend();

    /// Resolve the run objects THROUGH the adopted seal's refs, never by
    /// `blobTargetRunKey` construction: with reference-parent carry a shard's current run may physically
    /// live under an older generation's key, and the seal ref is the only authority for the real key.
    /// Group the adopted `blob_target_runs` by the ref's explicit `shard`. Absent seal => no candidates.
    std::map<uint64_t, std::vector<RunRef>> runs_by_shard;
    if (const auto adopted = readFoldSeal(state.snap_generation, state.snap_attempt))
        for (const RunRef & r : adopted->blob_target_runs)
            runs_by_shard[r.shard].push_back(r);

    /// Scan every blob-target shard (see `retire`): a preview that only looked at shard 0 would miss the
    /// zero-in-degree candidates owned by shards 1..N under `gc_shards > 1`.
    for (uint64_t shard = 0; shard < state.gc_shards; ++shard)
    {
        const auto it = runs_by_shard.find(shard);
        static const std::vector<RunRef> kEmptyRuns;
        const std::vector<RunRef> & shard_runs = it != runs_by_shard.end() ? it->second : kEmptyRuns;
        for (const BlobCandidate & cand : zeroInDegree(backend, shard_runs))
        {
            const HeadResult observed = backend.head(layout.blobKey(cand.ref));
            if (!observed.exists)
                continue;
            PreviewEntry e;
            e.kind = ObjectKind::Blob;
            e.ref = cand.ref;
            e.key = layout.blobKey(cand.ref);
            e.size = observed.size;
            e.reason = "unreachable";
            out.push_back(std::move(e));
        }

        /// Retired-in-snapshot: stream the SAME adopted seal runs and emit every `kCondemned`
        /// sentinel row. The stored token IS the authority — NO HEAD here (a HEAD would defeat the point
        /// and cost I/O). `delete_pending` rows are deleted next fold; the rest await graduation. Preview
        /// stays WRITE-FREE (`openSourceEdgeRun` is a pure reader). Output is a superset of the above.
        for (const RunRef & run : shard_runs)
        {
            SourceEdgeRunView reader = openSourceEdgeRun(backend, run.key);
            String key;
            String payload;
            while (reader.next(key, payload))
            {
                if (payload.empty() || payload[0] != kCondemned)
                    continue;
                BlobRef ref;
                UInt128 source_id;
                SourceEdgeKeyCodec::parse(key, ref, source_id);   // throws CORRUPTED_DATA on a malformed key
                const CondemnedRow row = decodeCondemnedRow(payload);
                PreviewEntry e;
                e.kind = ObjectKind::Blob;
                e.ref = ref;
                e.key = layout.blobKey(ref);
                e.size = row.size;
                e.token = row.token;
                e.condemn_round = row.condemn_round;
                e.reason = row.delete_pending ? "delete_pending" : "awaiting_graduation";
                out.push_back(std::move(e));
            }
            /// Whole-file seal-checksum: verify the drained run before its condemned
            /// rows are trusted in the preview. Fail-closed on mismatch.
            reader.verifyAgainst(run.checksum);
        }
    }
    return out;
}

void Gc::rememberObservation(const GcLease & lease)
{
    has_observation = true;
    last_seen_owner = lease.owner;
    last_seen_seq = lease.seq;
}

void Gc::pulseHeartbeat(Pool & store, UInt128 gc_id)
{
    const String key = store.layout().gcHbKey();
    const auto got = store.backend().get(key);
    GcHeartbeat hb;
    std::optional<Token> expected;
    if (got)
    {
        hb = decodeGcHeartbeat(got->bytes);
        expected = got->token;
    }
    hb.owner = gc_id;
    ++hb.hb_seq;
    store.backend().casPut(key, encodeGcHeartbeat(hb), expected);
}

bool Gc::acquireOrRenewLease(GcState & state, Token & state_token, bool allow_steal)
{
    const String key = store->layout().gcStateKey();

    for (int attempt = 0; attempt < 2; ++attempt)
    {
        const auto got = store->backend().get(key);

        if (!got)
        {
            if (has_observation)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS gc/state vanished after being observed (owner {}, seq {})",
                    u128ToHex(last_seen_owner), last_seen_seq);

            GcState fresh;
            fresh.lease = GcLease{gc_id, 1};
            /// Creation-time only: gc_shards is set ONCE on first-ever acquire; subsequent rounds read
            /// the authoritative value from the persisted GcState (pool is authoritative on reopen).
            /// PoolConfig carries the configured value from the disk XML.
            fresh.gc_shards = store->poolConfig().gc_shards;
            const CasResult acquire_res = store->backend().casPut(key, encodeGcState(fresh), std::nullopt);
            if (acquire_res.outcome == CasOutcome::Committed)
            {
                rememberObservation(fresh.lease);
                state = std::move(fresh);
                state_token = acquire_res.token;
                return true;
            }
            continue;
        }

        GcState current = decodeGcState(got->bytes);
        if (current.gc_shards != store->poolConfig().gc_shards)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS gc/state gc_shards {} disagrees with the pool-authoritative _pool_meta value {}",
                current.gc_shards, store->poolConfig().gc_shards);

        if (current.lease.owner == gc_id)
        {
            GcState next = current;
            ++next.lease.seq;
            const CasResult renew_res = store->backend().casPut(key, encodeGcState(next), got->token);
            if (renew_res.outcome == CasOutcome::Committed)
            {
                rememberObservation(next.lease);
                state = std::move(next);
                state_token = renew_res.token;
                return true;
            }
            continue;
        }

        GcHeartbeat hb;
        if (const auto hb_got = store->backend().get(store->layout().gcHbKey()))
            hb = decodeGcHeartbeat(hb_got->bytes);
        /// Observation-based heartbeat liveness, symmetric with the frozen-lease-tuple check below:
        /// ANY movement of the observed (owner, hb_seq) pair between this contender's two ticks is
        /// proof of life, and `hb_seq` values are comparable only under the SAME remembered hb owner.
        /// Deliberately NOT compared against `current.lease.owner`: a deposed leader's heartbeat
        /// thread keeps pulsing (with `owner = itself`) until its next round resets `i_am_leader`,
        /// and its writes can race out the live new leader's pulses — an hb pair that keeps moving
        /// under the OLD owner's name must still read as "alive", or a live, pulsing new leader gets
        /// its lease stolen. An hb owner change re-arms the window (this tick's pair is remembered
        /// below); a steal happens only once the lease tuple AND the hb pair are both frozen across
        /// a full window.
        const bool hb_alive = has_observation
            && (hb.owner != last_seen_hb_owner || hb.hb_seq > last_seen_hb_seq);

        const bool incumbent_renewed = !has_observation
            || current.lease.owner != last_seen_owner
            || current.lease.seq != last_seen_seq;
        if (incumbent_renewed || hb_alive || !allow_steal)
        {
            /// Only ARM the steal-decision state (last_seen_owner/seq/hb_*) when this call is itself
            /// allowed to act on a frozen tuple - i.e. the loop path. A caller with allow_steal=false
            /// (manual `SYSTEM ... GC`) reads current state for its own acquire/renew/back-off decision
            /// above, but must NOT record this foreign-incumbent observation: doing so would let the
            /// loop's own very next tick treat THIS snapshot as one half of ITS two-observation window,
            /// without the real wall-time gap (>= H) the window's safety argument requires between the
            /// loop's own ticks (a manual command can land microseconds before a scheduled tick). Leaving
            /// last_seen_* untouched here restores the pre-A7 invariant exactly: the frozen-tuple
            /// comparison that can actually trigger a steal is only ever between two LOOP observations,
            /// always >= interval apart.
            if (allow_steal)
            {
                rememberObservation(current.lease);
                last_seen_hb_owner = hb.owner;
                last_seen_hb_seq = hb.hb_seq;
            }
            return false;
        }

        GcState next = current;
        next.lease.owner = gc_id;
        ++next.lease.seq;
        const CasResult steal_res = store->backend().casPut(key, encodeGcState(next), got->token);
        if (steal_res.outcome == CasOutcome::Committed)
        {
            rememberObservation(next.lease);
            state = std::move(next);
            state_token = steal_res.token;
            return true;
        }

        if (const auto reread = store->backend().get(key))
            rememberObservation(decodeGcState(reread->bytes).lease);
        return false;
    }

    return false;
}

CatalogLifecycleReconcileResult Gc::drainCompletedRemoving(const GcState & leased_state)
{
    if (leased_state.snap_generation == 0)
        return {
            .authority_status = AuthorityStatus::Authoritative,
            .catalog_resolution = CatalogResolution::DrainComplete,
            .retired_lives = {},
            .final_catalog_cut = std::nullopt,
            .deleted = 0};

    const std::optional<CasFoldSeal> parent = readFoldSeal(
        leased_state.snap_generation, leased_state.snap_attempt);
    if (!parent)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS GC pre-fold drain: adopted parent seal (generation {}, attempt {}) is missing",
            leased_state.snap_generation, leased_state.snap_attempt);

    Backend & backend = store->backend();
    const Layout & layout = store->layout();
    const uint64_t admitted_generation = leased_state.lease.seq;
    const auto check_fence = [&](uint64_t expected_generation)
    {
        if (expected_generation != admitted_generation)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS GC pre-fold drain: internal leader generation mismatch (expected {}, admitted {})",
                expected_generation, admitted_generation);
        const auto got = backend.get(layout.gcStateKey());
        if (!got)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS GC pre-fold drain: gc/state vanished while checking leader generation {}",
                admitted_generation);
        const GcState current = decodeGcState(got->bytes);
        if (current.lease.owner != gc_id || current.lease.seq != admitted_generation)
            return CasRefCatalog::LeaderFenceStatus::Moved;
        return CasRefCatalog::LeaderFenceStatus::Held;
    };

    return CatalogLifecycleReconciler(
        backend, layout, *parent, admitted_generation, check_fence).reconcile();
}

}
