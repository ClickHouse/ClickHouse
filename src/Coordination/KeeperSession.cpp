#include <Coordination/KeeperSession.h>

#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/KeeperSpans.h>


namespace DB
{

KeeperSession::KeeperSession(int64_t session_id_, ResponseCallback callback_, bool quorum_reads, LocalReadCallback local_read_callback)
    : session_id(session_id_)
    , quorum_reads_(quorum_reads)
    , callback(std::move(callback_))
    , local_read_callback_(std::move(local_read_callback))
{
}

bool KeeperSession::canAcceptRequests() const
{
    std::lock_guard lock(mutex);
    return state == SessionState::Active;
}

bool KeeperSession::addRequest(const KeeperRequestForSession & request)
{
    /// Quorum reads and all non-read requests go through Raft.
    if (quorum_reads_ || !request.request->isReadRequest())
    {
        /// Track writes so that subsequent reads can be deferred behind them.
        if (!request.request->isReadRequest())
        {
            std::lock_guard lock(mutex);
            ++pending_writes_count_;
            last_enqueued_write_xid_ = request.request->xid;
        }
        return true;
    }

    /// Non-quorum read: dispatch inline or defer behind the latest pending write.
    std::lock_guard lock(mutex);
    if (pending_writes_count_ == 0)
    {
        /// No in-flight writes for this session — dispatch the read immediately.
        local_read_callback_(request);
        return false;
    }

    /// There are pending writes — defer behind the most recent one.
    chassert(last_enqueued_write_xid_.has_value());
    ZooKeeperOpentelemetrySpans::maybeInitialize(request.request->spans.read_wait_for_write, request.request->tracing_context);
    deferred_reads_[*last_enqueued_write_xid_].push_back(request);
    return false;
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
