#include <Coordination/KeeperSession.h>
#include <Coordination/KeeperRequestsQueue.h>
#include <Coordination/KeeperSessionRegistry.h>
#include <Coordination/SessionRequest.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/KeeperSpans.h>
#include <base/defines.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <fmt/format.h>


namespace DB
{

namespace
{
/// True when the response represents a completed Close operation.
/// The WATCH_XID guard is defense-in-depth (watches never have Close opnum).
bool isCloseResponse(const Coordination::ZooKeeperResponsePtr & response)
{
    return response->xid != Coordination::WATCH_XID
        && response->getOpNum() == Coordination::OpNum::Close;
}
}

KeeperSession::KeeperSession(
    int64_t session_id_,
    ResponseCallback callback_,
    KeeperSessionRegistry & registry_,
    KeeperSubqueuePtr subqueue_,
    LocalReadCallback local_read_dispatch_)
    : session_id(session_id_)
    , callback(std::make_shared<const ResponseCallback>(std::move(callback_)))
    , registry(&registry_)
    , subqueue(std::move(subqueue_))
    , local_read_dispatch(std::move(local_read_dispatch_))
{
}

bool KeeperSession::canAcceptRequests() const
{
    std::lock_guard lock(mutex);
    return state == SessionState::Active;
}


bool KeeperSession::finalizeWithErrors(Coordination::Error error)
{
    std::vector<SessionRequestPtr> error_batch;
    std::shared_ptr<const ResponseCallback> response_callback;

    {
        std::lock_guard lock(mutex);

        if (state == SessionState::Closed)
            return false;

        if (!callback)
        {
            /// No callback — just clean up like Shutdown.
            orphanActiveRequests();
            state = SessionState::Closed;
            return false;
        }

        /// Collect all active requests and create error responses for those
        /// that don't already have one (already-committed entries keep their
        /// existing response).
        for (auto & req : active_requests)
        {
            if (req && req->request && !req->response)
            {
                auto resp = req->request->makeResponse();
                resp->xid = req->request->xid;
                resp->zxid = 0;
                resp->error = error;
                req->response = resp;
            }
            if (req)
                req->setState(RequestState::Completed);
            error_batch.push_back(std::move(req));
        }
        active_requests.clear();

        /// Copy callback, then clear it — no further deliveries possible.
        response_callback = callback;
        callback.reset();
        state = SessionState::Closed;
    }

    /// Append a nullptr sentinel so the TCP handler closes the socket
    /// immediately after sending the error responses, instead of sitting
    /// in the poll loop until session timeout or client-side close.
    bool had_errors = !error_batch.empty();
    error_batch.push_back(nullptr);

    /// Deliver outside mutex. deliverResponses has try-catch.
    if (response_callback)
        deliverResponses(error_batch, *response_callback);

    return had_errors;
}

KeeperSession::DeliveryResult KeeperSession::deliverDirectResponse(
    const Coordination::ZooKeeperResponsePtr & response,
    bool is_watch)
{
    DeliveryResult result = DeliveryResult::NotDelivered;
    std::shared_ptr<const ResponseCallback> response_callback;

    {
        std::lock_guard lock(mutex);

        if (state == SessionState::Closed)
            return result;

        if (!callback)
            return result;

        response_callback = callback;

        if (isCloseResponse(response))
        {
            callback.reset();
            state = SessionState::Closed;
            result = DeliveryResult::DeliveredAndDetach;
        }
        else
        {
            result = DeliveryResult::Delivered;
        }
    }

    /// request is null — TCP handler skips SendingResponse/Sent lifecycle
    /// transitions for direct deliveries (watches, error responses).
    auto wrapper = std::make_shared<SessionRequest>();
    wrapper->session_id = session_id;
    wrapper->response = response;
    wrapper->is_watch_notification = is_watch;
    wrapper->setState(RequestState::Completed);

    try
    {
        std::vector<SessionRequestPtr> batch;
        batch.push_back(std::move(wrapper));
        (*response_callback)(std::move(batch));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return result;
}

KeeperSession::DeliveryResult KeeperSession::deliverDirect(
    const Coordination::ZooKeeperResponsePtr & response)
{
    return deliverDirectResponse(response, /*is_watch=*/false);
}

KeeperSession::DeliveryResult KeeperSession::onWatchNotification(
    const Coordination::ZooKeeperResponsePtr & watch_response)
{
    return deliverDirectResponse(watch_response, /*is_watch=*/true);
}

KeeperSession::DeliveryResult KeeperSession::onRaftResponse(
    Coordination::XID xid,
    const Coordination::ZooKeeperResponsePtr & response)
{
    {
        std::lock_guard lock(mutex);

        if (state == SessionState::Closed)
            return DeliveryResult::NotDelivered;

        if (active_requests.empty())
            return DeliveryResult::NotDelivered;

        auto & front = active_requests.front();
        if (!front || !front->request || front->request->xid != xid)
        {
            LOG_ERROR(log,
                "Response xid {} op {} does not match FIFO head for session {}. "
                "FIFO size={}, head: xid {} op {}. FIFO:{}",
                xid,
                Coordination::opNumToString(response->getOpNum()),
                session_id,
                active_requests.size(),
                front && front->request ? front->request->xid : -1,
                front && front->request ? Coordination::opNumToString(front->request->getOpNum()) : "none",
                serializeActiveRequestsNoLock());
            chassert(false && "FIFO head xid mismatch in onRaftResponse — this is a bug");
            return DeliveryResult::FifoMismatch;
        }

        /// Attach response to the FIFO head. Don't pop or deliver yet —
        /// `onRaftCommitted` will call `advanceQueue` which handles that.
        front->response = response;
        front->setState(RequestState::RaftResponseReady);

        front->onCommitCompleted(
            response->pre_commit_start_us, response->pre_commit_end_us,
            response->commit_start_us, response->commit_end_us);

        /// Don't return DeliveredAndDetach here — the session must remain in the
        /// registry until `commit_callback` → `onRaftCommitted` → `advanceQueue`
        /// delivers the Close response. The commit_callback detaches afterwards.
    }

    return DeliveryResult::Delivered;
}


void KeeperSession::orphanActiveRequests()
{
    if (active_requests.empty())
        return;

    LOG_WARNING(log, "Clearing {} active requests for session {} (orphaned)",
        active_requests.size(), session_id);

    /// Orphaned entries in the shared subqueue or current_batch will flow
    /// through Raft and be silently dropped by `setResponse` (session gone).
    /// Note: orphaned entries still count in KeeperRequestsQueue::total_size
    /// until `pullIntoRaftBatch` drains them. Under heavy churn this can make
    /// `isOverLimit` temporarily over-reject, but requestThread drains fast enough.
    active_requests.clear();
}

String KeeperSession::serializeActiveRequestsNoLock(size_t max_entries) const
{
    String result;
    size_t limit = std::min(active_requests.size(), max_entries);
    for (size_t i = 0; i < limit; ++i)
    {
        auto & entry = active_requests[i];
        if (entry && entry->request)
            result += fmt::format(" [{}]xid={} op={} state={}",
                i, entry->request->xid,
                Coordination::opNumToString(entry->request->getOpNum()),
                static_cast<int>(entry->getState()));
    }
    if (active_requests.size() > limit)
        result += fmt::format(" ...+{} more", active_requests.size() - limit);
    return result;
}

KeeperSession::AddResult KeeperSession::addRequest(SessionRequestPtr keeper_req)
{
    chassert(keeper_req && keeper_req->request);
    bool dispatch_read = false;

    {
        std::lock_guard lock(mutex);

        if (state != SessionState::Active)
        {
            LOG_WARNING(log,
                "Rejecting request for session {} xid {} op {}: session state is {} (not Active)",
                session_id, keeper_req->request->xid,
                Coordination::opNumToString(keeper_req->request->getOpNum()),
                static_cast<int>(state));
            return AddResult::SessionClosed;
        }

        bool is_close = (keeper_req->request->getOpNum() == Coordination::OpNum::Close);

        /// Close bypasses per-session limit — critical for ephemeral cleanup,
        /// matching the global queue exemption in KeeperDispatcher::putRequest.
        size_t limit = registry->maxSessionActiveRequests();
        if (!is_close && limit > 0 && active_requests.size() >= limit)
        {
            LOG_WARNING(LogFrequencyLimiter(log, 5),
                "Per-session request limit ({}) reached for session {}, rejecting xid {} op {}",
                limit, session_id, keeper_req->request->xid,
                Coordination::opNumToString(keeper_req->request->getOpNum()));
            return AddResult::QueueFull;
        }

        auto mode = (registry->quorumReads() || !keeper_req->request->isReadRequest())
            ? KeeperRequestMode::Quorum : KeeperRequestMode::NonQuorum;
        keeper_req->mode = mode;

        if (mode == KeeperRequestMode::NonQuorum)
        {
            if (active_requests.empty())
            {
                /// Empty queue: start a new session-thread local group.
                /// `keeper_req` is safe to access outside `mutex` after the lock is released
                /// because it is in `ExecutingLocal` state — only this thread (the session/TCP
                /// handler thread) reads or writes it until `advanceQueue` pops it.
                /// `advanceQueue(QuorumThread)` skips entries with `KeeperRequestExecutor::SessionThread`,
                /// so there is no data race between the two executor paths.
                keeper_req->executor = KeeperRequestExecutor::SessionThread;
                keeper_req->setState(RequestState::ExecutingLocal, {{"keeper.fast_path", true}});
                active_requests.push_back(keeper_req);
                dispatch_read = true;
            }
            else
            {
                /// Non-empty queue: join the tail's executor group.
                keeper_req->executor = active_requests.back()->executor;
                keeper_req->setState(RequestState::PendingLocal);
                active_requests.push_back(keeper_req);
            }
        }
        else
        {
            /// Quorum (write, quorum read, Close, Auth, Heartbeat, Reconfig).
            /// Push to active_requests first, then subqueue. This makes the
            /// FIFO-before-subqueue ordering obvious: onRaftResponse acquires
            /// session mutex, so it can't see the request until we release.
            /// Lock nesting (session mutex → subqueue mutex) is safe — no reverse path.
            keeper_req->executor = KeeperRequestExecutor::QuorumThread;
            keeper_req->setState(RequestState::PendingRaft);
            active_requests.push_back(keeper_req);
            if (!subqueue->push(keeper_req, /*bypass_limit=*/is_close))
            {
                active_requests.pop_back();
                /// Roll back to Initial so the destructor's OTel safety-net
                /// doesn't fire a spurious "leaked" span for a request that
                /// was never actually enqueued.
                keeper_req->setState(RequestState::Initial);
                LOG_WARNING(LogFrequencyLimiter(log, 5),
                    "Request queue is full, rejecting request for session {}", session_id);
                return AddResult::QueueFull;
            }
        }

        /// Close is terminal for request admission: no later request may
        /// enter the FIFO behind it. Set Finishing AFTER successful enqueue
        /// so that a failed push (queue full) doesn't brick the session.
        if (is_close)
            state = SessionState::Finishing;
    }

    if (dispatch_read)
    {
        /// OTel spans already handled by setState(ExecutingLocal).
        try
        {
            /// span over a local shared_ptr — safe because local_read_dispatch is synchronous.
            local_read_dispatch(std::span<SessionRequestPtr>(&keeper_req, 1));
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            auto err = keeper_req->request->makeResponse();
            err->xid = keeper_req->request->xid;
            err->error = Coordination::Error::ZSYSTEMERROR;
            keeper_req->response = err;
        }
        /// Defensive: if local_read_dispatch returned without filling response
        /// and without throwing, synthesize an error to avoid a stuck session.
        if (!keeper_req->response)
        {
            chassert(false && "local_read_dispatch must always fill response");
            auto err = keeper_req->request->makeResponse();
            err->xid = keeper_req->request->xid;
            err->error = Coordination::Error::ZSYSTEMERROR;
            keeper_req->response = err;
        }
        /// Response filled. Drain from head (session thread group).
        advanceQueue(KeeperRequestExecutor::SessionThread);
    }

    return AddResult::Accepted;
}

void KeeperSession::onRaftCommitted()
{
    advanceQueue(KeeperRequestExecutor::QuorumThread);
}


void KeeperSession::advanceQueue(KeeperRequestExecutor current_executor)
{
    std::vector<SessionRequestPtr> to_deliver;
    std::vector<SessionRequestPtr> read_batch;
    std::shared_ptr<const ResponseCallback> response_callback;

    /// Bounded: each iteration either delivers responses (draining active_requests),
    /// dispatches reads (finite), or returns. active_requests is finite and entries
    /// are consumed monotonically — no path re-enqueues entries.
    while (true)
    {
        /// Re-loop after delivery: the concurrent QuorumThread may have completed entries (via onRaftResponse) while we were delivering outside the lock.
        to_deliver.clear();
        read_batch.clear();
        bool had_close = false;
        bool had_deliveries = false;

        {
            std::lock_guard lock(mutex);
            /// Check session is still alive — if callback was cleared by a
            /// concurrent Close/finalizeWithErrors, stop. The cached
            /// response_callback (set on first iteration) is a shared_ptr copy —
            /// the refcount keeps the callback alive even after clearing.
            if (!callback)
                return;
            if (!response_callback)
                response_callback = callback;
            had_close = popResponseReadyNoLock(to_deliver);
            if (!had_close)
                collectPendingReadsNoLock(current_executor, read_batch);
        }

        had_deliveries = !to_deliver.empty();
        deliverResponses(to_deliver, *response_callback);
        if (had_close)
            return;

        if (!read_batch.empty())
        {
            dispatchReads(read_batch);
            continue;
        }

        if (!had_deliveries)
            return;
    }
}

bool KeeperSession::popResponseReadyNoLock(std::vector<SessionRequestPtr> & out)
{
    while (!active_requests.empty() && active_requests.front()->response)
    {
        auto & head = active_requests.front();

        head->setState(RequestState::Completed);
        auto completed = std::move(head);
        active_requests.pop_front();

        if (isCloseResponse(completed->response))
        {
            callback.reset();
            state = SessionState::Closed;
            out.push_back(std::move(completed));
            return true;
        }

        out.push_back(std::move(completed));
    }
    return false;
}

void KeeperSession::collectPendingReadsNoLock(KeeperRequestExecutor executor, std::vector<SessionRequestPtr> & out)
{
    auto it = active_requests.begin();
    for (; it != active_requests.end(); ++it)
    {
        auto & entry = *it;
        if (entry->getState() != RequestState::PendingLocal || entry->executor != executor)
            break;

        entry->setState(RequestState::ExecutingLocal);
        /// Copy, not move — entry stays in active_requests until popResponseReadyNoLock.
        out.push_back(entry);
    }

    /// Invariant: the next entry (if any) must NOT be another PendingLocal with
    /// the same executor — that would mean we stopped collecting too early.
    chassert(it == active_requests.end()
        || (*it)->getState() != RequestState::PendingLocal
        || (*it)->executor != executor);
}

void KeeperSession::deliverResponses(std::vector<SessionRequestPtr> & responses, const ResponseCallback & cb)
{
    if (responses.empty())
        return;

    /// Note: cb() takes ownership of the vector contents via move.
    try
    {
        cb(std::move(responses));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void KeeperSession::dispatchReads(std::vector<SessionRequestPtr> & reads)
{
    try
    {
        local_read_dispatch(reads);
    }
    catch (...)
    {
        LOG_WARNING(log, "dispatchReads failed for session {}: {}",
            session_id, getCurrentExceptionMessage(true));
        tryLogCurrentException(__PRETTY_FUNCTION__);
        for (auto & req : reads)
        {
            if (!req->response)
            {
                auto err = req->request->makeResponse();
                err->xid = req->request->xid;
                err->error = Coordination::Error::ZSYSTEMERROR;
                req->response = err;
            }
        }
    }

    /// Defensive: if local_read_dispatch returned without filling some responses
    /// and without throwing, synthesize errors to avoid stuck sessions.
    for (auto & req : reads)
    {
        if (!req->response)
        {
            chassert(false && "local_read_dispatch must always fill response");
            auto err = req->request->makeResponse();
            err->xid = req->request->xid;
            err->error = Coordination::Error::ZSYSTEMERROR;
            req->response = err;
        }
    }
}

}
