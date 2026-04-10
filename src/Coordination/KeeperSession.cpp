#include <Coordination/KeeperSession.h>

#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/KeeperSpans.h>


namespace DB
{

KeeperSession::KeeperSession(int64_t session_id_, ResponseCallback callback_, bool quorum_reads, size_t max_active_requests, LocalReadCallback local_read_callback)
    : session_id(session_id_)
    , quorum_reads_(quorum_reads)
    , max_active_requests_(max_active_requests)
    , callback(std::move(callback_))
    , local_read_callback_(std::move(local_read_callback))
{
}

bool KeeperSession::canAcceptRequests() const
{
    std::lock_guard lock(mutex);
    return state == SessionState::Active;
}

KeeperSession::AddRequestResult KeeperSession::addRequest(const KeeperRequestForSession & request)
{
    /// Quorum reads and all non-read requests go through Raft.
    if (quorum_reads_ || !request.request->isReadRequest())
    {
        std::lock_guard lock(mutex);

        if (state != SessionState::Active)
            return {AddResult::SessionClosed, false};

        if (max_active_requests_ > 0 && active_requests.size() >= max_active_requests_)
        {
            LOG_WARNING(LogFrequencyLimiter(log, 5),
                "Per-session request limit ({}) reached for session {}, rejecting xid {} op {}",
                max_active_requests_, session_id, request.request->xid,
                Coordination::opNumToString(request.request->getOpNum()));
            return {AddResult::QueueFull, false};
        }

        active_requests.push_back(request);

        /// Track writes so that subsequent reads can be deferred behind them.
        if (!request.request->isReadRequest())
        {
            ++pending_writes_count_;
            last_enqueued_write_xid_ = request.request->xid;
        }
        return {AddResult::Accepted, true};
    }

    /// Non-quorum read: dispatch inline or defer behind the latest pending write.
    std::lock_guard lock(mutex);

    if (state != SessionState::Active)
        return {AddResult::SessionClosed, false};

    if (max_active_requests_ > 0 && active_requests.size() >= max_active_requests_)
    {
        LOG_WARNING(LogFrequencyLimiter(log, 5),
            "Per-session request limit ({}) reached for session {}, rejecting xid {} op {}",
            max_active_requests_, session_id, request.request->xid,
            Coordination::opNumToString(request.request->getOpNum()));
        return {AddResult::QueueFull, false};
    }

    active_requests.push_back(request);

    if (pending_writes_count_ == 0)
    {
        /// No in-flight writes for this session — dispatch the read immediately.
        local_read_callback_(request);
        return {AddResult::Accepted, false};
    }

    /// There are pending writes — defer behind the most recent one.
    chassert(last_enqueued_write_xid_.has_value());
    ZooKeeperOpentelemetrySpans::maybeInitialize(request.request->spans.read_wait_for_write, request.request->tracing_context);
    deferred_reads_[*last_enqueued_write_xid_].push_back(request);
    return {AddResult::Accepted, false};
}

bool KeeperSession::deliverResponse(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)
{
    std::lock_guard lock(mutex);

    if (state == SessionState::Closed)
        return false;

    if (!callback)
        return false;

    try
    {
        callback(response, std::move(request));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return true;
}

void KeeperSession::close()
{
    std::lock_guard lock(mutex);
    state = SessionState::Closed;
    callback = {};
    active_requests.clear();
}

KeeperSession::SessionState KeeperSession::getState() const
{
    std::lock_guard lock(mutex);
    return state;
}

void KeeperSession::addDeferredRead(Coordination::XID write_xid, const KeeperRequestForSession & read_request)
{
    std::lock_guard lock(mutex);
    deferred_reads_[write_xid].push_back(read_request);
}

KeeperRequestsForSessions KeeperSession::releaseDeferredReads(Coordination::XID write_xid)
{
    std::lock_guard lock(mutex);
    KeeperRequestsForSessions result;
    if (auto it = deferred_reads_.find(write_xid); it != deferred_reads_.end())
    {
        result = std::move(it->second);
        deferred_reads_.erase(it);
    }

    /// Decrement the pending-writes counter.
    if (pending_writes_count_ > 0)
        --pending_writes_count_;

    /// When all in-flight writes have committed, clear the last-write bookmark
    /// so that subsequent reads dispatch inline.
    if (pending_writes_count_ == 0)
        last_enqueued_write_xid_.reset();

    return result;
}

void KeeperSession::clearDeferredReads()
{
    std::lock_guard lock(mutex);
    deferred_reads_.clear();
}

}
