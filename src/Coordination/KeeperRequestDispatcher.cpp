#include <Coordination/KeeperRequestDispatcher.h>

#if USE_NURAFT

#include <Coordination/CoordinationSettings.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/HistogramMetrics.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

template class NonblockingBoundedQueue<DB::KeeperRequestForSession>;
template class NonblockingBoundedQueue<DB::KeeperResponseForSession>;

namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
    extern const Metric KeeperOutstandingRequests;
}

namespace ProfileEvents
{
    extern const Event KeeperCommitsFailed;
    extern const Event KeeperBatchMaxCount;
    extern const Event KeeperBatchMaxTotalSize;
    extern const Event KeeperBatchMaxReadCount;
    extern const Event KeeperBatchMaxReadTotalSize;
    extern const Event KeeperRequestRejectedDueToSoftMemoryLimitCount;
    extern const Event KeeperStaleRequestsSkipped;
    extern const Event KeeperReadBatchCount;
    extern const Event KeeperReadBatchTotalRequests;
    extern const Event KeeperWriteBatchCount;
    extern const Event KeeperWriteBatchTotalRequests;
}

namespace HistogramMetrics
{
    extern Metric & KeeperCurrentBatchSizeElements;
    extern Metric & KeeperCurrentBatchSizeBytes;
}

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 dispatch_busy_wait_sleep_us;
    extern const CoordinationSettingsUInt64 max_in_flight_request_batches;
    extern const CoordinationSettingsUInt64 max_read_batch_bytes_size;
    extern const CoordinationSettingsUInt64 max_read_batch_size;
    extern const CoordinationSettingsUInt64 max_request_queue_bytes_size;
    extern const CoordinationSettingsUInt64 max_request_queue_size;
    extern const CoordinationSettingsUInt64 max_requests_batch_bytes_size;
    extern const CoordinationSettingsUInt64 max_requests_batch_size;
    extern const CoordinationSettingsUInt64 max_response_queue_bytes_size;
    extern const CoordinationSettingsMilliseconds operation_timeout_ms;
    extern const CoordinationSettingsBool optimize_read_order;
    extern const CoordinationSettingsBool quorum_reads;
    extern const CoordinationSettingsMilliseconds session_shutdown_timeout;
    extern const CoordinationSettingsMilliseconds stream_in_flight_drain_timeout_ms;
    extern const CoordinationSettingsMilliseconds stream_suspect_retry_delay_ms;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

static size_t getSubrequestCount(const Coordination::ZooKeeperRequest & request)
{
    if (const auto * multi = typeid_cast<const Coordination::ZooKeeperMultiRequest *>(&request))
        return multi->requests.size();
    else
        return 1;
}

/// Estimated memory used by a request/response struct, for queue size byte limits.
static size_t getRequestBytesCost(const Coordination::ZooKeeperRequest & request)
{
    return request.bytesSize() + sizeof(Coordination::ZooKeeperRequest);
}
static size_t getResponseBytesCost(const Coordination::ZooKeeperResponse & response)
{
    return response.bytesSize() + sizeof(Coordination::ZooKeeperResponse);
}

static bool checkIfRequestIncreaseMem(const Coordination::ZooKeeperRequestPtr & request)
{
    if (request->getOpNum() == Coordination::OpNum::Create
        || request->getOpNum() == Coordination::OpNum::Create2
        || request->getOpNum() == Coordination::OpNum::CreateIfNotExists
        || request->getOpNum() == Coordination::OpNum::Set)
    {
        return true;
    }
    if (request->getOpNum() == Coordination::OpNum::Multi)
    {
        Coordination::ZooKeeperMultiRequest & multi_req = dynamic_cast<Coordination::ZooKeeperMultiRequest &>(*request);
        /// Add up sizes of create/set requests, subtract sizes of remove requests.
        /// This doesn't really make sense because we're interested in memory usage of znodes, not requests.
        /// But we don't know znode sizes at this point (is the Remove removing a small or big znode?),
        /// so can't do much better here. Maybe it would make sense to move this check to preprocessRequest,
        /// where we have access to the znode states.
        Int64 memory_delta = 0;
        for (const auto & sub_req : multi_req.requests)
        {
            auto sub_zk_request = std::dynamic_pointer_cast<Coordination::ZooKeeperRequest>(sub_req);
            switch (sub_zk_request->getOpNum())
            {
                case Coordination::OpNum::Create:
                case Coordination::OpNum::Create2:
                case Coordination::OpNum::CreateIfNotExists: {
                    Coordination::ZooKeeperCreateRequest & create_req
                        = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*sub_zk_request);
                    memory_delta += create_req.bytesSize();
                    break;
                }
                case Coordination::OpNum::Set: {
                    Coordination::ZooKeeperSetRequest & set_req = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*sub_zk_request);
                    memory_delta += set_req.bytesSize();
                    break;
                }
                case Coordination::OpNum::Remove:
                case Coordination::OpNum::TryRemove: {
                    Coordination::ZooKeeperRemoveRequest & remove_req
                        = dynamic_cast<Coordination::ZooKeeperRemoveRequest &>(*sub_zk_request);
                    memory_delta -= remove_req.bytesSize();
                    break;
                }
                case Coordination::OpNum::RemoveRecursive: {
                    Coordination::ZooKeeperRemoveRecursiveRequest & remove_req
                        = dynamic_cast<Coordination::ZooKeeperRemoveRecursiveRequest &>(*sub_zk_request);
                    memory_delta -= remove_req.bytesSize();
                    break;
                }
                default:
                    break;
            }
        }
        return memory_delta > 0;
    }

    return false;
}


KeeperRequestDispatcher::KeeperRequestDispatcher(KeeperServer * server_)
    : server(server_)
    , keeper_context(server->getKeeperContext())
    , log(getLogger("KeeperRequestDispatcher"))
{
    const auto & coordination_settings = keeper_context->getCoordinationSettings();
    size_t max_request_queue_size = coordination_settings[CoordinationSetting::max_request_queue_size];
    requests_queue.init(max_request_queue_size);
    /// TODO: We could add a setting for this 3.0 multiplier, but here's a better idea.
    ///       Make ~everything operate on batches of requests, including the commit callback and responses
    ///       (yes, commit callback would get a bit awkward because we have to alternate between
    ///        committing requests to storage and executing reads, but that's ok).
    ///       Then responses_queue would contain batches instead of individual responses, so we can
    ///       set its size to, say, 100k and it'll never overflow in practice (remember that there's
    ///        a byte limit in dispatch thread preventing us from starting more requests if response
    ///        queue is big).
    responses_queue.init(static_cast<size_t>(static_cast<double>(max_request_queue_size) * 3.0));

    in_flight_batches = std::vector<InFlightBatch>(std::max(size_t(coordination_settings[CoordinationSetting::max_in_flight_request_batches]), size_t(1)));
}

void KeeperRequestDispatcher::startup()
{
    dispatch_thread = ThreadFromGlobalPool([this] { dispatchThread(); });
    response_thread = ThreadFromGlobalPool([this] { responseThread(); });
}

void KeeperRequestDispatcher::shutdown(bool closed_all_connections)
{
    shutting_down.store(true);
    if (dispatch_thread.joinable())
        dispatch_thread.join();
    if (response_thread.joinable())
        response_thread.join();

    stream.reset();

    /// Drain queues just to check for counter leaks.
    /// Don't bother sending replies because client connections should already be closed by now
    /// (or stuck and timed out, if closed_all_connections is false).
    /// Don't need to do anything with in_flight_batches.
    KeeperRequestForSession request_for_session;
    while (tryPopRequest(request_for_session)) {}
    KeeperResponseForSession response_for_session;
    while (responses_queue.tryPop(response_for_session))
        onResponseDeallocated(*response_for_session.response);
    if (closed_all_connections) // otherwise there might be concurrent putRequest calls or missing onResponseDeallocated calls
    {
        chassert(requests_queue_bytes.load() == 0);
        chassert(response_bytes_in_all_queues.load() == 0);
    }

    /// Send to leader Close requests for active sessions.
    /// Maybe we should go further and do this when client connection is closed for any reason
    /// (KeeperTCPHandler::runImpl return).
    ///
    /// Removing ephemeral znodes without waiting for session timeout is a little strange and
    /// different from vanilla zookeeper. Ephemeral znode may disappear before the client notices
    /// connection loss. But it is important for clickhouse in practice. Otherwise keeper server
    /// restart would cause INSERT queries to stall for 30 seconds (session timeout) while the
    /// leftover ephemeral znode prevents retries.
    KeeperRequestsForSessions close_requests;
    if (server->isLeaderAlive())
    {
        std::lock_guard sessions_lock(sessions_mutex);

        for (const auto & [session_id, session] : sessions)
        {
            auto request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
            request->xid = Coordination::CLOSE_XID;
            using namespace std::chrono;
            KeeperRequestForSession request_info
            {
                .session_id = session_id,
                .time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count(),
                .request = std::move(request),
                .digest = std::nullopt
            };

            close_requests.push_back(std::move(request_info));
        }

        sessions.clear();
    }
    else
    {
        LOG_INFO(log, "Sessions cannot be closed during shutdown because there is no active leader");
    }

    if (!close_requests.empty())
    {
        LOG_INFO(log, "Trying to close {} session(s)", close_requests.size());
        uint64_t timeout_ms = keeper_context->getCoordinationSettings()[CoordinationSetting::session_shutdown_timeout].totalMilliseconds();
        bool sent = false;
        auto start_time = std::chrono::steady_clock::now();
        auto temp_stream = server->raft_instance->open_client_req_stream(timeout_ms);
        if (temp_stream)
        {
            std::vector<nuraft::ptr<nuraft::buffer>> entries;
            entries.reserve(close_requests.size());
            for (const auto & r : close_requests)
                entries.push_back(IKeeperStateMachine::getZooKeeperLogEntry(r));

            temp_stream->append(std::move(entries));

            /// Wait for the request to reach the leader, don't wait for commit.
            ///
            /// TODO: It would be better to wait for commit, at least if the current node is the
            ///       leader. Otherwise we may shut down the leader before it replicates the
            ///       Close requests to followers.
            ///       This change could be done together with the TODO at dropInFlightRequests call
            ///       in recreateStreamWithBackoff, because both need
            ///       client_req_stream::append completion callback (here to get log_idx that needs
            ///       to be committed, there to get error code).
            ///       If shutdown Close is made reliable, remove retries in
            ///       test_session_close_shutdown.
            while (!temp_stream->is_idle() && std::chrono::steady_clock::now() - start_time < std::chrono::milliseconds(timeout_ms))
                std::this_thread::sleep_for(std::chrono::milliseconds(1));

            sent = temp_stream->is_idle();
        }

        if (!sent)
            LOG_WARNING(
                log,
                "Failed to close sessions in {}ms. If they are not closed, they will be closed after session timeout.",
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count());
    }
}

bool KeeperRequestDispatcher::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, bool use_xid_64)
{
    /// (We could check session liveness here, but we already check it when dequeueing, and
    ///  putRequest is called from a place where the session is very likely still alive.)

    KeeperRequestForSession request_info;
    request_info.use_xid_64 = use_xid_64;
    request_info.request = request;
    using namespace std::chrono;
    request_info.time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    request_info.session_id = session_id;

    if (keeper_context->isShutdownCalled())
        return false;

    request->spans.maybeInitialize(KeeperSpan::DispatcherRequestsQueue, request->tracing_context.get());

    int64_t max_request_queue_bytes = int64_t(keeper_context->getCoordinationSettings()[CoordinationSetting::max_request_queue_bytes_size]);
    auto try_push = [&]
    {
        return requests_queue_bytes.load() <= max_request_queue_bytes && requests_queue.tryPush(std::move(request_info));
    };

    if (!try_push())
    {
        /// Queue too big, busy-wait for it to get smaller.
        auto start_time = std::chrono::steady_clock::now();
        while (true)
        {
            /// (This may be called from many KeeperTCPHandler threads, so this sleep shouldn't be
            ///  super short or we may eat all cpu cores and slow down raft. It just needs to be
            ///  shorter than the time it takes to process a full queue of requests.)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

            if (try_push())
                break;

            std::chrono::milliseconds operation_timeout(keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds());
            if (std::chrono::steady_clock::now() - start_time > operation_timeout)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot push request to queue within operation timeout");
        }
    }

    /// We increment requests_queue_bytes after adding it to queue rather than before. So
    ///  * it may briefly become negative if the other thread popped the request and decreased the
    ///     counter very quickly,
    ///  * we may go a little over the limit if multiple threads check the counter simultaneously,
    ///     then increment it before seeing each other's changes.
    /// This is all fine. Alternatively, we could pre-increment the counter and have these problems instead:
    ///  * can get stuck if multiple threads try to enqueue requests whose total size is above the limit,
    ///  * have to un-increment if failed to enqueue.
    requests_queue_bytes.fetch_add(int64_t(getRequestBytesCost(*request)));
    CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequests);

    return true;
}

bool KeeperRequestDispatcher::tryPopRequest(KeeperRequestForSession & request)
{
    bool res = requests_queue.tryPop(request);
    if (res)
    {
        requests_queue_bytes.fetch_sub(int64_t(getRequestBytesCost(*request.request)));
        CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequests);

        request.request->spans.maybeFinalize(
            KeeperSpan::DispatcherRequestsQueue,
            [&]
            {
                return std::vector<OpenTelemetry::SpanAttribute>{
                    {"keeper.operation", Coordination::opNumToString(request.request->getOpNum())},
                    {"keeper.session_id", request.session_id},
                    {"keeper.xid", request.request->xid},
                    {"keeper.dispatcher.requests_queue.size", requests_queue.size()},
                };
            });
    }
    return res;
}

void KeeperRequestDispatcher::onResponse(KeeperResponseForSession response) noexcept
{
    size_t size = getResponseBytesCost(*response.response);
    int64_t max_response_queue_bytes = int64_t(keeper_context->getCoordinationSettings()[CoordinationSetting::max_response_queue_bytes_size]);
    auto try_push = [&]
    {
        return response_bytes_in_all_queues.load() <= max_response_queue_bytes && responses_queue.tryPush(std::move(response));
    };

    if (!try_push())
    {
        /// Queue too big.
        /// This is unusual because dispatchThread doesn't start requests when response queue is
        /// bigger than half the limit.
        /// This covers e.g. the case where many requests have huge responses
        /// (e.g. get on huge znodes or list on fertile znodes).
        /// Here we wait for the queue to get smaller.
        /// I.e. block (probably) commit thread while (probably) KeeperTCPHandler-s send responses.
        /// This seems like an ok way to do flow control for this, but we would still get in trouble
        /// if there are enough big requests that processing the request queue takes longer than session timeout.

        auto start_time = std::chrono::steady_clock::now();
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

            if (try_push())
                break;

            if (std::chrono::steady_clock::now() - start_time > std::chrono::milliseconds(
                    keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds()))
            {
                /// Just drop responses on the floor, I guess. The client will eventually time out
                /// and close the socket. This is a weird enough situation that idk how it would
                /// arise in practice and what would be a good way to handle or avoid it.
                LOG_ERROR(log, "Response queue is too big for too long. Dropping responses.");
                ProfileEvents::increment(ProfileEvents::KeeperCommitsFailed);
                return;
            }
        }
    }

    /// response_bytes_in_all_queues may briefly become negative if the other thread
    /// popped the request and decreased the counter before we got here.
    response_bytes_in_all_queues.fetch_add(int64_t(size));
}

void KeeperRequestDispatcher::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard sessions_lock(sessions_mutex);

    auto [it, inserted] = sessions.try_emplace(session_id);
    if (!inserted)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);

    it->second.response_callback = std::move(callback);
    CurrentMetrics::add(CurrentMetrics::KeeperAliveConnections);
}

void KeeperRequestDispatcher::finishSession(int64_t session_id)
{
    ZooKeeperResponseCallback callback;
    {
        std::lock_guard sessions_lock(sessions_mutex);

        auto it = sessions.find(session_id);
        if (it == sessions.end())
            return; // finishSession may be called multiple times, e.g. from sessionCleanerTask and KeeperTCPHandler

        callback = std::move(it->second.response_callback);
        sessions.erase(it);
    }

    CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);

    if (callback)
    {
        /// This is useful only in the unusual case where a session was timed out by sessionCleanerTask
        /// while the session's KeeperTCPHandler socket is still open. Then the callback tells
        /// KeeperTCPHandler to close the socket.
        auto close_response = std::make_shared<Coordination::ZooKeeperCloseResponse>();
        close_response->error = Coordination::Error::ZSESSIONEXPIRED;
        size_t size = getResponseBytesCost(*close_response);
        response_bytes_in_all_queues.fetch_add(size);
        if (!callback(close_response, nullptr))
            response_bytes_in_all_queues.fetch_sub(size);
    }
}

void KeeperRequestDispatcher::recreateStreamWithBackoff()
{
    /// After we lost connection to leader, we want to sleep for multiple reasons:
    ///  1. If there are requests in flight, wait in hopes that they get committed.
    ///     (E.g. during graceful leader migration.)
    ///     After the sleep we'll have to fail the remaining in-flight requests and
    ///     close their client sessions.
    ///  2. If there's no healthy leader, we can't do much and can as well wait for
    ///     leader election to complete before proceeding. This may also give more
    ///     chance for in-flight requests to get committed and removed from in_flight_batches.
    ///  3. If streams fail to open or get closed quickly without making any progress,
    ///     we don't want to spam reconnects very quickly.
    auto sleep_start = std::chrono::steady_clock::now();
    while (!shutting_down.load())
    {
        auto slept = std::chrono::steady_clock::now() - sleep_start;
        if (slept >= std::chrono::milliseconds(keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds()))
            break;

        auto is_delaying_reconnect = [&]
        {
            return current_stream_is_suspect.load() && slept < std::chrono::milliseconds(
                keeper_context->getCoordinationSettings()[CoordinationSetting::stream_suspect_retry_delay_ms].totalMilliseconds());
        };
        auto is_waiting_for_in_flight_requests = [&]
        {
            return head_idx.load() < tail_idx.load() && slept < std::chrono::milliseconds(
                keeper_context->getCoordinationSettings()[CoordinationSetting::stream_in_flight_drain_timeout_ms].totalMilliseconds());
        };
        if (server->isLeaderAlive() && !is_delaying_reconnect() && !is_waiting_for_in_flight_requests())
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    /// Currently we drop all in-flight appends (that weren't committed within
    /// stream_in_flight_drain_timeout_ms).
    ///
    /// TODO: We could do better: if some batches were cleanly rejected by the leader (e.g. because
    ///       of term mismatch), we can re-send them into the new stream. I imagine it would look
    ///       like this:
    ///  * client_req_stream::append would accept a callback and just call it in `handler`.
    ///  * We'd pass a callback that stores the result status in InFlightBatch. (Probably by adding
    ///    a shared_ptr field in InFlightBatch.)
    ///  * This new InFlightBatch status can be one of: waiting, rejected by leader (can retry this
    ///    and all later batches), accepted by leader (can't retry), network error (can't retry).
    ///  * During the stream_in_flight_drain_timeout_ms wait, we'd stop early if the first in-flight
    ///    batch was rejected by leader.
    ///  * Here we wouldn't drop requests if first in-flight batch was rejected by leader.
    ///    We'd re-send them through the new stream.
    /// Then a graceful leader migration wouldn't cause any client requests to fail.
    dropInFlightRequests();

    stream.reset();

    if (shutting_down.load())
        return;

    if (!server->isLeaderAlive())
    {
        /// Don't attempt reads or writes if there's no working leader.
        /// In particular, avoid doing reads, because they may succeed and see very old data
        /// if this replica was down for a while, then came up and never saw a healthy leader.
        LOG_DEBUG(log, "Not creating append stream because leader is not alive");
        return;
    }

    current_stream_is_suspect.store(true);
    /// May return nullptr.
    stream = server->raft_instance->open_client_req_stream(
        keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds());

    if (stream)
        LOG_INFO(log, "Created append stream");
    else
        LOG_DEBUG(log, "Failed to create append stream");
}

void KeeperRequestDispatcher::dispatchThread()
{
    try
    {
        DB::setThreadName(ThreadName::KEEPER_REQUEST);

        int64_t operation_timeout_ms = keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds();

        auto last_stuck_check_time = std::chrono::steady_clock::now();
        while (!shutting_down.load())
        {
            auto now = std::chrono::steady_clock::now();

            if (!stream || stream->is_abandoned())
            {
                if (stream)
                    LOG_INFO(log, "Append stream is abandoned, will create a new one");

                /// If stream is broken, report errors for in-flight requests and create a new stream.
                recreateStreamWithBackoff();
                continue;
            }

            /// Periodically check that we don't have stuck requests.
            /// In particular, we can get stuck if there's a bug that breaks stream guarantees
            /// (causes reordering or gaps) as our commit callback only checks completion of the request
            /// at the head of the queue.
            if (now > last_stuck_check_time + std::chrono::milliseconds(operation_timeout_ms))
            {
                last_stuck_check_time = now;
                size_t idx = head_idx.load();
                if (tail_idx.load() > idx && now > in_flight_batches[idx % in_flight_batches.size()].start_time + std::chrono::milliseconds(operation_timeout_ms * 10))
                {
                    if (server->isLeaderAlive())
                        LOG_ERROR(log, "Detected stuck or reordered requests. Dropping. This may indicate a bug.");
                    stream.reset();
                    continue;
                }
            }

            /// Check that we don't have too many running requests already.
            /// Currently no limit on byte size or request count, only batch count;
            /// batch size limit x batch count limit is hoped to be sufficient.
            /// Also check that we don't have lots of pending responses to send; may be useful
            /// if there are lots of big reads, so a moderate number of requests are producing lots
            /// of bytes of responses, so we may be bottlenecked by sending those responses through
            /// network.
            size_t batch_idx = tail_idx.load();
            size_t cur_head_idx = head_idx.load();
            size_t num_batches_in_flight = batch_idx - cur_head_idx;
            int64_t max_response_queue_bytes = int64_t(keeper_context->getCoordinationSettings()[CoordinationSetting::max_response_queue_bytes_size]);
            if (num_batches_in_flight >= in_flight_batches.size() ||
                response_bytes_in_all_queues.load() > max_response_queue_bytes / 2 ||
                !stream->is_ready())
            {
                /// Too many batches in flight. Busy-wait.
                ///
                /// Busy-wait is acceptable because there are very few dispatcher threads
                /// (currently one), and keeper server typically uses much fewer cpu cores than
                /// the machine has.
                ///
                /// Busy-wait is actually good because otherwise we'd have to do a futex wake
                /// syscall (e.g. through condition_variable::notify) from the commit thread.
                /// That would be bad because the commit thread is often the bottleneck of
                /// the entire keeper service, and futex wake is sometimes slow.
                ///
                /// This sleep needs to be shorter than the time it takes to drain the whole
                /// in_flight_batches queue, otherwise we'll be sitting idle after the queue went
                /// from full to empty during one sleep.
                ///
                /// (Maybe we have to pace requests more carefully here.
                ///  I'm not sure what's the best way to think about this. Here's one:
                ///
                ///  Consider a simplified toy system where each request has to go through
                ///  a sequence of 5 stages each having maximum throughput of 10 requests/s
                ///  (e.g. limited by single-core cpu speed), connected by large queues, and
                ///  requests take 1 second to move between stages (e.g. network latency).
                ///  Total latency is 5 seconds.
                ///  Maximum total throughput is 10 requests/s, achieved if every stage has
                ///  nonempty input queue at all times.
                ///  Suppose we are careful about pacing: we start 1 new request every 100ms.
                ///  This gives us maximum throughput, with 50 requests in flight at all times.
                ///  Suppose we are less careful and start requests asap, with a limit of
                ///  100 in-flight requests. We send the first 100 requests at once.
                ///  5 seconds later we start getting one request completion every 100ms,
                ///  starting one new request every 100ms. And I guess it just keeps going like
                ///  that, smooth 1 request every 100ms, full throughput. So we're fine, and
                ///  careful pacing is not needed?
                ///  Or maybe there are conditions where this breaks and we instead converge to
                ///  issuing bursts of 100 requests every 5 seconds, with corresponding
                ///  throughput of 20 requests/s?)
                std::this_thread::sleep_for(std::chrono::microseconds(
                    keeper_context->getCoordinationSettings()[CoordinationSetting::dispatch_busy_wait_sleep_us]));
                continue;
            }

            /// Check that we have any requests to execute.
            KeeperRequestForSession request;
            if (!tryPopRequest(request))
            {
                /// No requests to process. Busy-wait here too.
                /// TODO: Perhaps we should replace this with a futex wait to improve throughput on
                ///       latency-bound workloads. E.g. one client doing blocking requests in a loop.
                std::chrono::microseconds dispatch_busy_wait_sleep(
                    keeper_context->getCoordinationSettings()[CoordinationSetting::dispatch_busy_wait_sleep_us]);
                std::this_thread::sleep_for(dispatch_busy_wait_sleep);
                continue;
            }

            /// Pick a batch of requests.

            const auto & coordination_settings = keeper_context->getCoordinationSettings();
            uint64_t max_batch_bytes_size = coordination_settings[CoordinationSetting::max_requests_batch_bytes_size];
            size_t max_batch_size = coordination_settings[CoordinationSetting::max_requests_batch_size];
            bool quorum_reads = coordination_settings[CoordinationSetting::quorum_reads];
            size_t max_read_batch_size = coordination_settings[CoordinationSetting::max_read_batch_size];
            size_t max_read_batch_bytes_size = coordination_settings[CoordinationSetting::max_read_batch_bytes_size];
            bool optimize_read_order = coordination_settings[CoordinationSetting::optimize_read_order];
            bool is_exceeding_memory_soft_limit = server->isExceedingMemorySoftLimit();

            /// Write requests to put in the batch.
            /// (And possibly some read requests that need to go through raft.)
            KeeperRequestsForSessions requests;
            KeeperRequestsForSessions reconfig_requests;
            /// Reads that we can execute right in this thread, before sending the batch to leader.
            KeeperRequestsForSessions early_reads;
            /// Reads to do in between writes in this batch.
            std::vector<std::pair<size_t, KeeperRequestsForSessions>> intermediate_reads;
            /// Reads to do after the whole batch is committed.
            KeeperRequestsForSessions late_reads;
            size_t batch_bytes = 0;
            size_t batch_subrequests = 0;
            size_t reads_bytes = 0;
            size_t reads_requests = 0;
            size_t reads_subrequests = 0;

            {
                std::shared_lock sessions_lock(sessions_mutex);

                /// Read request must be executed after all previous requests from the same session.
                /// There are 3 cases:
                ///  (1) Reads that depend on some earlier writes in the batch we're making.
                ///      Put them in the batch's `reads`.
                ///  (2) Reads that depend on some writes in an earlier batch that is not committed yet.
                ///      Add them to that batch's `late_reads`.
                ///  (3) Reads from sessions that have no writes in progress.
                ///      Execute them right in this thread, before sending the current batch.
                ///      (This case is likely important in practice: it should greatly reduce read
                ///       latency for users that mostly do reads, or that alternate blocking reads
                ///       and writes. This workload is probably common in practice but less common in
                ///       keeper-bench.)
                ///
                /// For case (1) we additionally reorder requests within the batch to have longer
                /// runs of consecutive read requests, because consecutive read requests can be
                /// executed in parallel.
                ///
                /// We only ever reorder requests from different sessions, so ordering within a
                /// session is still guaranteed.

                /// Reordering within a batch.
                /// A read can be moved to a later point, up to the next non-read request from the same session.
                /// As we iterate over requests, we try to shift all reads to as late as possible, until
                /// one of them bumps into the next request from its session; when it does, we flush all
                /// accumulated reads (late_reads) and continue.
                ///
                /// Invariant: late_reads contains reads that should be done after all requests of
                ///            the current batch so far.
                /// Invariant: Session::reordering_version == current_reordering_version
                ///            iff late_reads contains any requests for this session.
                ++current_reordering_version;
                auto flush_to_intermediate_reads = [&]
                {
                    if (late_reads.empty())
                    {
                        chassert(!optimize_read_order);
                        return;
                    }
                    chassert(!requests.empty());
                    intermediate_reads.emplace_back(requests.size(), std::move(late_reads));
                    late_reads.clear();
                    ++current_reordering_version;
                };

                auto check_batch_size_limits = [&]() -> bool
                {
                    if (batch_subrequests >= max_batch_size)
                    {
                        ProfileEvents::increment(ProfileEvents::KeeperBatchMaxCount, 1);
                        return false;
                    }
                    if (batch_bytes >= max_batch_bytes_size)
                    {
                        ProfileEvents::increment(ProfileEvents::KeeperBatchMaxTotalSize, 1);
                        return false;
                    }
                    if (reads_subrequests >= max_read_batch_size)
                    {
                        ProfileEvents::increment(ProfileEvents::KeeperBatchMaxReadCount, 1);
                        return false;
                    }
                    if (reads_bytes >= max_read_batch_bytes_size)
                    {
                        ProfileEvents::increment(ProfileEvents::KeeperBatchMaxReadTotalSize, 1);
                        return false;
                    }
                    return true;
                };

                do
                {
                    Session * session = nullptr;
                    auto op = request.request->getOpNum();
                    if (op != Coordination::OpNum::Close &&
                        op != Coordination::OpNum::SessionID)
                    {
                        auto it = sessions.find(request.session_id);
                        if (it == sessions.end() || it->second.dead.load())
                        {
                            ProfileEvents::increment(ProfileEvents::KeeperStaleRequestsSkipped);
                            continue;
                        }
                        session = &it->second;
                    }

                    if (request.request->getOpNum() == Coordination::OpNum::Reconfig)
                    {
                        /// Reconfig requests go through a separate pipeline, can't be fed into client_req_stream.
                        reconfig_requests.push_back(std::move(request));
                        break; // (just to not add a separate limit for number of reconfigs in a batch)
                    }

                    if (is_exceeding_memory_soft_limit && checkIfRequestIncreaseMem(request.request))
                    {
                        ProfileEvents::increment(ProfileEvents::KeeperRequestRejectedDueToSoftMemoryLimitCount);
                        LOG_WARNING(
                            log,
                            "Processing requests refused because of max_memory_usage_soft_limit {}, the total allocated memory is {}, RSS is {}, request type is {}",
                            ReadableSize(keeper_context->getKeeperMemorySoftLimit()),
                            ReadableSize(total_memory_tracker.get()),
                            ReadableSize(total_memory_tracker.getRSS()),
                            request.request->getOpNum());
                        addErrorResponse(request, Coordination::Error::ZOUTOFMEMORY);
                        continue;
                    }

                    bool is_read = request.request->isReadRequest();

                    /// If read has to go through raft, put it in the batch as if it were a write.
                    /// It's sufficient to do this for the first request of a batch, the rest are ordered after it.
                    if (is_read && requests.empty() && quorum_reads)
                        is_read = false;

                    if (is_read)
                    {
                        /// (We're counting the request towards current batch's limit even if we end
                        ///  up adding it to another batch's late_reads. Because this is much
                        ///  simpler than limiting late_reads size. But doesn't this mean that
                        ///  late_reads can grow unboundedly big? No, it can't get bigger than
                        ///  max_in_flight_request_batches * max_read_batch_size. Think of it as the
                        ///  limit being "amortized" across multiple batches.)
                        reads_requests += 1;
                        reads_subrequests += getSubrequestCount(*request.request);
                        reads_bytes += getRequestBytesCost(*request.request);

                        chassert(session);

                        if (quorum_reads)
                            /// Force the read to be attached to the new batch.
                            session->last_batch_idx = batch_idx;

                        size_t last_batch = session->last_batch_idx;
                        if (last_batch == batch_idx)
                        {
                            /// Case (1): read request should be attached to the current batch.
                            /// Put it in late_reads, which will later be flushed to the batch's `reads`.
                            chassert(!requests.empty());
                            if (session)
                                session->reordering_version = current_reordering_version;
                            late_reads.push_back(std::move(request));
                        }
                        else
                        {
                            /// There are no write requests from this session in current batch so far.
                            bool added = false;
                            if (last_batch >= cur_head_idx)
                            {
                                /// The batch with previous request from this session may still be
                                /// in flight. Try to add the request to that batch's late_reads.
                                /// If added == true, this is case (2).
                                /// If added == false, it means last_batch was committed and all its
                                /// read requests were finished, which means we can execute this
                                /// request right in dispatchThread.
                                added = in_flight_batches[last_batch % in_flight_batches.size()].late_reads.add(request);
                            }
                            if (!added)
                                /// Case (3): do the read right in dispatchThread.
                                early_reads.push_back(std::move(request));
                        }
                    }
                    else
                    {
                        if ((session && session->reordering_version == current_reordering_version) || !optimize_read_order)
                            flush_to_intermediate_reads();
                        if (session)
                            session->last_batch_idx = batch_idx;

                        batch_subrequests += getSubrequestCount(*request.request);
                        batch_bytes += getRequestBytesCost(*request.request);
                        requests.push_back(std::move(request));
                    }

                    request = {}; // suppress clang-tidy false positive
                } while (check_batch_size_limits() && tryPopRequest(request));
            }

            if (!early_reads.empty())
            {
                LOG_TEST(log, "Processing {} reads in dispatch thread. First one: {}", early_reads.size(), early_reads[0].request->toString());
                executeReads(std::move(early_reads));
            }

            if (requests.empty())
                chassert(intermediate_reads.empty() && late_reads.empty());

            if (!requests.empty())
            {
                HistogramMetrics::observe(HistogramMetrics::KeeperCurrentBatchSizeElements, static_cast<HistogramMetrics::Value>(batch_subrequests));
                HistogramMetrics::observe(HistogramMetrics::KeeperCurrentBatchSizeBytes, static_cast<HistogramMetrics::Value>(batch_bytes));
                ProfileEvents::increment(ProfileEvents::KeeperWriteBatchCount);
                ProfileEvents::increment(ProfileEvents::KeeperWriteBatchTotalRequests, requests.size());

                LOG_TEST(log, "Starting batch {}, {} bytes, {} writes, {} reads ({} of them are at the end of batch). First request: {}", batch_idx, batch_bytes, requests.size(), reads_requests, late_reads.size(), requests[0].request->toString());

                std::vector<nuraft::ptr<nuraft::buffer>> entries;
                entries.reserve(requests.size());
                for (const auto & r : requests)
                    entries.push_back(IKeeperStateMachine::getZooKeeperLogEntry(r));

                /// Add information about the batch to the queue of in-flight requests.

                auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
                batch.start_time = std::chrono::steady_clock::now();
                batch.requests = std::move(requests);
                batch.intermediate_reads = std::move(intermediate_reads);
                batch.activate(std::move(late_reads));
                tail_idx.store(batch_idx + 1);

                /// Finally send the requests to leader.

                stream->append(std::move(entries));
            }

            /// Ordering between Reconfig requests and other requests is not very important.
            for (const auto & reconfig_request : reconfig_requests)
            {
                LOG_INFO(log, "Processing reconfig request: {}", reconfig_request.request->toString());
                server->getKeeperStateMachine()->reconfigure(reconfig_request);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException("Unexpected exception in KeeperRequestDispatcher::dispatchThread");
        std::abort();
    }
}

void KeeperRequestDispatcher::popBatch(size_t batch_idx)
{
    auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
    batch.deactivate();
    chassert(head_idx.load() == batch_idx);
    head_idx.store(batch_idx + 1);
}

void KeeperRequestDispatcher::dropInFlightRequests()
{
    std::lock_guard stream_lock(request_completion_mutex);
    size_t batches_dropped = 0;
    size_t requests_dropped = 0;
    size_t reads_dropped = 0;
    while (head_idx.load() < tail_idx.load())
    {
        batches_dropped += 1;
        size_t batch_idx = head_idx.load();
        auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
        while (batch.committed_requests < batch.requests.size())
        {
            requests_dropped += 1;
            addErrorResponse(batch.requests[batch.committed_requests], Coordination::Error::ZCONNECTIONLOSS);
            batch.committed_requests += 1;
            if (batch.intermediate_reads_idx < batch.intermediate_reads.size() &&
                batch.intermediate_reads[batch.intermediate_reads_idx].first == batch.committed_requests)
            {
                for (const auto & read_request : batch.intermediate_reads[batch.intermediate_reads_idx].second)
                {
                    reads_dropped += 1;
                    addErrorResponse(read_request, Coordination::Error::ZCONNECTIONLOSS);
                }
                batch.intermediate_reads_idx += 1;
            }
        }
        chassert(batch.intermediate_reads_idx == batch.intermediate_reads.size());
        while (true)
        {
            auto reads = batch.late_reads.takeAndFinishIfEmpty();
            if (reads.empty())
                break;
            for (const auto & read_request : reads)
            {
                reads_dropped += 1;
                addErrorResponse(read_request, Coordination::Error::ZCONNECTIONLOSS);
            }
        }
        popBatch(batch_idx);
    }
    if (batches_dropped != 0)
        LOG_INFO(log, "Dropped {} batches with {} writes and {} reads", batches_dropped, requests_dropped, reads_dropped);
}

void KeeperRequestDispatcher::addErrorResponse(const KeeperRequestForSession & request_for_session, Coordination::Error error)
{
    auto response = request_for_session.request->makeResponse();
    response->xid = request_for_session.request->xid;
    response->zxid = 0;
    response->error = error;
    response->enqueue_ts = std::chrono::steady_clock::now();
    onResponse(DB::KeeperResponseForSession{request_for_session.session_id, response});
}

void KeeperRequestDispatcher::onCommit(const KeeperRequestForSession & request_for_session)
{
    /// When Close commits, mark the session as dead so that
    /// stale requests still sitting in the backed-up queue will be filtered.
    /// This covers the window between Close commit and `finishSession`
    /// (e.g. `sessionCleanerTask` expired the session but the TCP handler
    /// hasn't disconnected yet). Fires on ALL nodes via RAFT, which is
    /// how followers learn about closed sessions.
    if (request_for_session.request->getOpNum() == Coordination::OpNum::Close)
    {
        std::shared_lock sessions_lock(sessions_mutex);
        auto it = sessions.find(request_for_session.session_id);
        if (it != sessions.end())
            it->second.dead.store(true);
    }

    std::lock_guard stream_lock(request_completion_mutex);

    /// We expect requests to be committed in order with no gaps.
    /// So we only have to check if the newly committed request is the first one in our queue.
    size_t batch_idx = head_idx.load();
    auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
    if (!batch.active.load())
        return; // no in-flight batches
    const auto & req = batch.requests.at(batch.committed_requests);
    if (req.session_id != request_for_session.session_id ||
        req.request->xid != request_for_session.request->xid)
    {
        return;
    }

    if (current_stream_is_suspect.load())
        current_stream_is_suspect.store(false); // a request succeeded, the stream is working

    batch.committed_requests += 1;

    if (batch.intermediate_reads_idx < batch.intermediate_reads.size() &&
        batch.intermediate_reads[batch.intermediate_reads_idx].first == batch.committed_requests)
    {
        auto reads = std::move(batch.intermediate_reads[batch.intermediate_reads_idx].second);
        batch.intermediate_reads_idx += 1;

        /// (We could re-check whether the requests' sessions are still alive, but it doesn't seem
        ///  worth the map lookup cost. We already checked session liveness just before starting
        ///  this raft batch, and hopefully raft commit latency doesn't get high in practice even
        ///  under too much load because we limit the number of in-flight batches.)
        executeReads(std::move(reads));
    }

    if (batch.committed_requests == batch.requests.size())
    {
        chassert(batch.intermediate_reads_idx == batch.intermediate_reads.size());

        /// Execute reads that need to happen right after this batch of writes.
        /// Tricky interaction: dispatchThread may add more reads to this list while we're here.
        ///
        /// Note that "read" requests are not truly read-only; they can add/remove/inspect watches.
        /// So we must execute them in the correct order (within each session).
        /// (E.g. maybe an earlier "read" request adds a watch, then a later "read" request checks its presence - can't reorder.)
        /// (Even though server->putLocalReadRequests internally can execute parts of multiple reads in parallel,
        ///  we must still perform these putLocalReadRequests calls in the correct order.)
        ///
        /// So, what should we do if dispatchThread wants to add more reads while onCommit is executing
        /// previous reads? The new reads should be executed after onCommit is done with the previous reads.
        /// So we allow dispatchThread to add more reads while we're executing previous reads.
        /// That's why there's a loop here, taking and executing newly added reads.
        /// When there are no more reads to execute, LateReads goes into Finished state, which tells
        /// dispatchThread that subsequent reads can be executed right there.
        /// (Alternatively, we could make dispatchThread block waiting for onCommit to finish reading,
        ///  but that's just worse.)
        while (true)
        {
            auto reads = batch.late_reads.takeAndFinishIfEmpty();
            if (reads.empty())
                break;
            executeReads(std::move(reads));
        }

        popBatch(batch_idx);
    }
}

void KeeperRequestDispatcher::executeReads(KeeperRequestsForSessions reads)
{
    ProfileEvents::increment(ProfileEvents::KeeperReadBatchCount);
    ProfileEvents::increment(ProfileEvents::KeeperReadBatchTotalRequests, reads.size());

    using namespace std::chrono;
    auto now_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    for (auto & r : reads)
        r.time = now_ms;

    server->putLocalReadRequests(reads);
}

void KeeperRequestDispatcher::onResponseDeallocated(const Coordination::ZooKeeperResponse & response)
{
    size_t size = getResponseBytesCost(response);
    response_bytes_in_all_queues.fetch_sub(size);
}

void KeeperRequestDispatcher::responseThread()
{
    try
    {
        DB::setThreadName(ThreadName::KEEPER_RESPONSE);

        while (!shutting_down.load())
        {
            KeeperResponseForSession response_for_session;
            if (!responses_queue.tryPop(response_for_session))
            {
                /// Busy-wait.
                std::this_thread::sleep_for(std::chrono::microseconds(
                    keeper_context->getCoordinationSettings()[CoordinationSetting::dispatch_busy_wait_sleep_us]));
                continue;
            }

            const UInt64 dequeue_time_us = ZooKeeperOpentelemetrySpans::now();

            bool deferred_deallocation = false;
            bool response_was_sent = false;
            {
                std::shared_lock sessions_lock(sessions_mutex);

                auto it = sessions.find(response_for_session.session_id);
                if (it != sessions.end())
                {
                    try
                    {
                        deferred_deallocation = it->second.response_callback(response_for_session.response, response_for_session.request);
                        response_was_sent = true;
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                }
            }

            if (!deferred_deallocation)
                onResponseDeallocated(*response_for_session.response);

            if (response_was_sent && response_for_session.request)
            {
                response_for_session.request->spans.maybeFinalize(
                    KeeperSpan::DispatcherResponsesQueue,
                    [&]
                    {
                        return std::vector<OpenTelemetry::SpanAttribute>{
                            {"keeper.operation", Coordination::opNumToString(response_for_session.request->getOpNum())},
                            {"keeper.session_id", response_for_session.session_id},
                            {"keeper.xid", response_for_session.request->xid},
                            {"keeper.dispatcher.responses_queue.size", responses_queue.size()},
                        };
                    },
                    OpenTelemetry::SpanStatus::OK,
                    "",
                    dequeue_time_us);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException("Unexpected exception in KeeperRequestDispatcher::responseThread");
        std::abort();
    }
}

}

#endif
