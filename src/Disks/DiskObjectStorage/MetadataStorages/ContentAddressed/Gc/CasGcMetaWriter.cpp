#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMetaWriter.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>

#include <Common/ProfileEvents.h>

#include <algorithm>

namespace ProfileEvents
{
    extern const Event CASGCMetaOps;
    extern const Event CASGCMetaWriteAnomaly;
}

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB::Cas
{

namespace
{

/// The per-hash freshness-meta operations GC schedules on the bounded pool are
/// best-effort/idempotent by design. The meta is only a point-read freshness marker for the writer/
/// promote gate; the ledger retired-set + the exact-token body delete remain the actual safety
/// core. A lost CAS here is never a correctness problem, only a (rare,
/// self-healing) staleness window for the NEXT point-reader — with ONE exception: the CONDEMN marker
/// is load-bearing for the delete edge. The exact-token delete argument
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
/// either way the entry stays UNCONFIRMED and the graduation gate carries it.
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

GcMetaWriter::GcMetaWriter(PoolPtr store_, LoggerPtr logger_, size_t pool_size)
    : state(std::make_shared<State>())
    , pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive,
           CurrentMetrics::LocalThreadScheduled, std::max<size_t>(1, pool_size))
{
    state->store = std::move(store_);
    state->logger = std::move(logger_);
}

void GcMetaWriter::submit(std::function<void()> op)
{
    /// `run` is safe to invoke either on the pool or inline (the scheduling-failure path below). A
    /// per-hash meta-operation exception is caught because the ledger and the exact-token body
    /// delete are the actual safety core. Pool/framework failures, including a failure while
    /// reporting an operation exception, remain visible to the throwing protocol barrier. The job
    /// captures only `state`, so it stays well-defined however long it outlives the writer that
    /// scheduled it.
    auto run = [op, st = state]()
    {
        ProfileEvents::increment(ProfileEvents::CASGCMetaOps);
        try
        {
            op();
        }
        catch (...)
        {
            ProfileEvents::increment(ProfileEvents::CASGCMetaWriteAnomaly);
            tryLogCurrentException(st->logger,
                "CAS gc: a per-hash freshness-meta op failed on the bounded pool (advisory-only; "
                "never wedges the round)");
        }
        /// A job that threw still FINISHED: this counter reports drain progress, not success.
        st->completed.fetch_add(1, std::memory_order_relaxed);
    };
    state->scheduled.fetch_add(1, std::memory_order_relaxed);
    try
    {
        pool.scheduleOrThrowOnError(run);
    }
    catch (...)
    {
        /// Scheduling itself failed (e.g. resource exhaustion under a mass-DROP burst) -- run inline
        /// rather than silently lose the meta write. The operation exception remains contained;
        /// infrastructure or diagnostic failures still propagate.
        ProfileEvents::increment(ProfileEvents::CASGCMetaWriteAnomaly);
        tryLogCurrentException(state->logger,
            "CAS gc: meta pool scheduling failed; running the op inline on the round's own thread");
        run();
    }
}

void GcMetaWriter::scheduleCondemnMarkerWrite(const BlobRef & ref, const Token & token,
                                              uint64_t condemn_round, uint64_t size)
{
    submit([st = state, ref, token, condemn_round, size]()
    {
        if (writeCondemnedMeta(*st->store, ref, condemn_round, size))
            st->noteCondemnMarkerDurable(ref, token);
    });
}

void GcMetaWriter::scheduleConfirmedMetaDelete(const BlobRef & ref)
{
    submit([st = state, ref]()
    {
        deleteConfirmedMeta(st->store->backend(), st->store->layout(), ref);
    });
}

void GcMetaWriter::drain()
{
    pool.wait();
}

void GcMetaWriter::drainOnExitNoThrow() noexcept
{
    try
    {
        pool.wait();
    }
    catch (...)
    {
        try
        {
            tryLogCurrentException(state->logger,
                "CAS gc: meta pool drain failed during round-exit cleanup");
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Cleanup is `noexcept`; diagnostic logging must not replace the round's exception.
        }
    }
}

uint64_t GcMetaWriter::scheduled() const
{
    return state->scheduled.load(std::memory_order_relaxed);
}

uint64_t GcMetaWriter::completed() const
{
    return state->completed.load(std::memory_order_relaxed);
}

void GcMetaWriter::State::noteCondemnMarkerDurable(const BlobRef & ref, const Token & token)
{
    std::lock_guard lock(condemn_marker_mutex);
    condemn_markers_confirmed.emplace(ref, token.value);
}

bool GcMetaWriter::State::condemnMarkerConfirmedInProcess(const BlobRef & ref, const Token & token)
{
    std::lock_guard lock(condemn_marker_mutex);
    return condemn_markers_confirmed.contains({ref, token.value});
}

void GcMetaWriter::State::forgetCondemnMarker(const BlobRef & ref, const Token & token)
{
    std::lock_guard lock(condemn_marker_mutex);
    condemn_markers_confirmed.erase({ref, token.value});
}

void GcMetaWriter::noteCondemnMarkerDurable(const BlobRef & ref, const Token & token)
{
    state->noteCondemnMarkerDurable(ref, token);
}

bool GcMetaWriter::condemnMarkerConfirmedInProcess(const BlobRef & ref, const Token & token)
{
    return state->condemnMarkerConfirmedInProcess(ref, token);
}

void GcMetaWriter::forgetCondemnMarker(const BlobRef & ref, const Token & token)
{
    state->forgetCondemnMarker(ref, token);
}

}
