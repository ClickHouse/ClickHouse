#include <Coordination/KeeperSessionReadBarrier.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/KeeperSpans.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <algorithm>
#include <chrono>


namespace CurrentMetrics
{
    extern const Metric KeeperPendingSessionWrites;
    extern const Metric KeeperDeferredSessionReads;
}

namespace ProfileEvents
{
    extern const Event KeeperDeferredReads;
    extern const Event KeeperImmediateReads;
}

namespace DB
{

namespace
{

uint64_t monotonicMicroseconds()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

}


KeeperSessionReadBarrier::KeeperSessionReadBarrier(const Settings & settings, LoggerPtr log)
    : settings_(settings)
    , log_(log)
{
}


// ---------------------------------------------------------------------------
// Map-level helpers (acquire/release map_mutex_ only)
// ---------------------------------------------------------------------------

KeeperSessionReadBarrier::SessionStatePtr KeeperSessionReadBarrier::findSession(int64_t session_id) const
{
    std::lock_guard map_lock(map_mutex_);
    auto it = sessions_.find(session_id);
    return it != sessions_.end() ? it->second : nullptr;
}

KeeperSessionReadBarrier::SessionStatePtr KeeperSessionReadBarrier::findOrCreateSession(int64_t session_id)
{
    std::lock_guard map_lock(map_mutex_);
    auto & slot = sessions_[session_id];
    if (!slot)
        slot = std::make_shared<SessionState>();
    return slot;
}

KeeperSessionReadBarrier::SessionStatePtr KeeperSessionReadBarrier::removeSession(int64_t session_id)
{
    std::lock_guard map_lock(map_mutex_);
    auto it = sessions_.find(session_id);
    if (it == sessions_.end())
        return nullptr;
    auto ptr = std::move(it->second);
    sessions_.erase(it);
    return ptr;
}


// ---------------------------------------------------------------------------
// Session-state helpers (caller must hold state.mutex)
// ---------------------------------------------------------------------------

void KeeperSessionReadBarrier::drainAllDeferredReads(SessionState & state, DeferredReadRequests & out)
{
    for (auto & [write_seq, queue] : state.deferred_reads)
    {
        for (auto & entry : queue)
        {
            out.push_back(std::move(entry));
            CurrentMetrics::sub(CurrentMetrics::KeeperDeferredSessionReads);
        }
    }
    state.deferred_reads.clear();
    state.deferred_reads_count = 0;
}

void KeeperSessionReadBarrier::clearPendingWrites(SessionState & state)
{
    if (!state.pending_writes.empty())
    {
        CurrentMetrics::sub(CurrentMetrics::KeeperPendingSessionWrites, state.pending_writes.size());
        state.pending_writes.clear();
    }
}

void KeeperSessionReadBarrier::enterDegradedMode(
    SessionState & state,
    int64_t session_id,
    const CallbackKey & key,
    DeferredReadRequests & out)
{
    state.barrier_degraded = true;
    drainAllDeferredReads(state, out);
    clearPendingWrites(state);
    LOG_ERROR(log_, "Per-session read barrier invariant violation for session {} (callback op {} xid {}), entering degraded mode",
        session_id, Coordination::opNumToString(key.op_num), key.xid);
}


// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void KeeperSessionReadBarrier::registerPendingWrites(const KeeperRequestsForSessions & batch)
{
    for (const auto & req : batch)
    {
        const auto op_num = req.request->getOpNum();
        if (!isPerSessionBarrierOp(op_num) || req.request->isReadRequest())
            continue;

        auto state_ptr = findOrCreateSession(req.session_id);
        std::lock_guard session_lock(state_ptr->mutex);

        if (state_ptr->barrier_degraded)
            continue;

        const uint64_t write_seq = state_ptr->next_write_seq++;
        state_ptr->pending_writes.push_back({write_seq, {op_num, req.request->xid}});
        CurrentMetrics::add(CurrentMetrics::KeeperPendingSessionWrites);

        if (state_ptr->pending_writes.size() > settings_.max_pending_writes_per_session)
        {
            LOG_WARNING(LogFrequencyLimiter(log_, 10), "Session {} exceeded max pending writes limit ({}), current: {}",
                req.session_id, settings_.max_pending_writes_per_session, state_ptr->pending_writes.size());
        }
    }
}

DeferReadResult KeeperSessionReadBarrier::tryDeferRead(const KeeperRequestForSession & request)
{
    auto state_ptr = findSession(request.session_id);
    if (!state_ptr)
    {
        ProfileEvents::increment(ProfileEvents::KeeperImmediateReads);
        return DeferReadResult::Immediate;
    }

    std::lock_guard session_lock(state_ptr->mutex);

    if (state_ptr->barrier_degraded)
        return DeferReadResult::Rejected;

    if (state_ptr->pending_writes.empty())
    {
        ProfileEvents::increment(ProfileEvents::KeeperImmediateReads);
        return DeferReadResult::Immediate;
    }

    if (state_ptr->deferred_reads_count >= settings_.max_deferred_reads_per_session)
        return DeferReadResult::Rejected;

    const uint64_t write_seq = state_ptr->pending_writes.back().write_seq;
    ZooKeeperOpentelemetrySpans::maybeInitialize(
        request.request->spans.read_wait_for_write,
        request.request->tracing_context);
    state_ptr->deferred_reads[write_seq].push_back(DeferredReadRequest{
        .deferred_wait_start_us = monotonicMicroseconds(),
        .request_for_session = request,
    });
    ++state_ptr->deferred_reads_count;

    ProfileEvents::increment(ProfileEvents::KeeperDeferredReads);
    CurrentMetrics::add(CurrentMetrics::KeeperDeferredSessionReads);

    return DeferReadResult::Deferred;
}

bool KeeperSessionReadBarrier::resolveWrite(
    SessionState & state,
    int64_t session_id,
    const CallbackKey & key,
    bool scan_from_back,
    DeferredReadRequests & out)
{
    if (state.barrier_degraded)
        return false;

    auto & pw = state.pending_writes;

    std::deque<PendingWrite>::iterator match_it;
    if (scan_from_back)
    {
        auto rit = std::find_if(pw.rbegin(), pw.rend(),
            [&](const PendingWrite & w) { return w.key == key; });
        if (rit == pw.rend())
        {
            enterDegradedMode(state, session_id, key, out);
            return true;
        }
        match_it = std::next(rit).base();
    }
    else
    {
        match_it = std::find_if(pw.begin(), pw.end(),
            [&](const PendingWrite & w) { return w.key == key; });
        if (match_it == pw.end())
        {
            enterDegradedMode(state, session_id, key, out);
            return true;
        }
    }

    const uint64_t barrier_seq = match_it->write_seq;
    pw.erase(match_it);
    CurrentMetrics::sub(CurrentMetrics::KeeperPendingSessionWrites);

    size_t moved_reads = 0;
    if (auto deferred_it = state.deferred_reads.find(barrier_seq);
        deferred_it != state.deferred_reads.end())
    {
        moved_reads = deferred_it->second.size();
        for (auto & entry : deferred_it->second)
        {
            out.push_back(std::move(entry));
            CurrentMetrics::sub(CurrentMetrics::KeeperDeferredSessionReads);
        }
        state.deferred_reads.erase(deferred_it);
    }

    chassert(moved_reads <= state.deferred_reads_count);
    state.deferred_reads_count -= moved_reads;
    return false;
}

CommitResolveResult KeeperSessionReadBarrier::resolveCommit(
    int64_t session_id, Coordination::OpNum op_num, Coordination::XID xid)
{
    if (!isPerSessionBarrierOp(op_num))
        return {};

    auto state_ptr = findSession(session_id);
    if (!state_ptr)
        return {};

    std::lock_guard session_lock(state_ptr->mutex);

    DeferredReadRequests collected;
    bool degraded = resolveWrite(*state_ptr, session_id, {op_num, xid}, /*scan_from_back=*/false, collected);

    CommitResolveResult result;
    if (degraded)
        result.reads_to_fail = std::move(collected);
    else
        result.reads_to_execute = std::move(collected);
    return result;
}

DeferredReadRequests KeeperSessionReadBarrier::resolveRollback(
    int64_t session_id, Coordination::OpNum op_num, Coordination::XID xid)
{
    if (!isPerSessionBarrierOp(op_num))
        return {};

    auto state_ptr = findSession(session_id);
    if (!state_ptr)
        return {};

    std::lock_guard session_lock(state_ptr->mutex);

    DeferredReadRequests collected;
    resolveWrite(*state_ptr, session_id, {op_num, xid}, /*scan_from_back=*/true, collected);
    return collected;
}

DeferredReadRequests KeeperSessionReadBarrier::failBatch(const KeeperRequestsForSessions & batch)
{
    DeferredReadRequests collected;
    for (const auto & req : batch)
    {
        const auto op_num = req.request->getOpNum();
        if (req.request->isReadRequest() || !isPerSessionBarrierOp(op_num))
            continue;

        auto state_ptr = findSession(req.session_id);
        if (!state_ptr)
            continue;

        std::lock_guard session_lock(state_ptr->mutex);
        resolveWrite(*state_ptr, req.session_id, {op_num, req.request->xid}, /*scan_from_back=*/true, collected);
    }
    return collected;
}

DeferredReadRequests KeeperSessionReadBarrier::closeSession(int64_t session_id)
{
    auto state_ptr = removeSession(session_id);
    if (!state_ptr)
        return {};

    DeferredReadRequests collected;
    std::lock_guard session_lock(state_ptr->mutex);
    drainAllDeferredReads(*state_ptr, collected);
    clearPendingWrites(*state_ptr);
    return collected;
}

DeferredReadRequests KeeperSessionReadBarrier::shutdown()
{
    std::unordered_map<int64_t, SessionStatePtr> local_sessions;
    {
        std::lock_guard map_lock(map_mutex_);
        local_sessions = std::move(sessions_);
    }

    DeferredReadRequests collected;
    for (auto & [session_id, state_ptr] : local_sessions)
    {
        (void)session_id;
        std::lock_guard session_lock(state_ptr->mutex);
        drainAllDeferredReads(*state_ptr, collected);
        clearPendingWrites(*state_ptr);
    }
    return collected;
}

}
