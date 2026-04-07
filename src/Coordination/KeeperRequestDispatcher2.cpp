#include <Coordination/KeeperRequestDispatcher2.h>

#if USE_NURAFT

#include <Coordination/CoordinationSettings.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/HistogramMetrics.h>
#include <Common/formatReadable.h>

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
    if (auto multi = typeid_cast<const Coordination::ZooKeeperMultiRequest *>(&request))
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


KeeperRequestDispatcher2::KeeperRequestDispatcher2(KeeperServer * server_)
    : server(server_)
    , keeper_context(server->getKeeperContext())
    , log(getLogger("KeeperRequestDispatcher2"))
{
    auto & coordination_settings = keeper_context->getCoordinationSettings();
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

    dispatch_thread = ThreadFromGlobalPool([this] { dispatchThread(); });
    response_thread = ThreadFromGlobalPool([this] { responseThread(); });

    in_flight_batches = std::vector<InFlightBatch>(coordination_settings[CoordinationSetting::max_in_flight_request_batches]);
}

void KeeperRequestDispatcher2::shutdown(bool closed_all_connections)
{
    shutting_down.store(true);
    dispatch_thread.join();
    response_thread.join();

    /// Drain queues just to check for counter leaks.
    /// Don't bother sending replies because client connections should already be closed by now
    /// (or stuck and timed out, if closed_all_connections is false).
    /// Don't need to do anything witht in_flight_batches.
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

    // Send to leader Close requests for active sessions.
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
        auto sessions_closing_done_promise = std::make_shared<std::promise<bool>>();
        auto sessions_closing_done = sessions_closing_done_promise->get_future();
        KeeperAppendStream temp_stream(server);
        temp_stream.putRequestBatch(
            close_requests,
            [my_sessions_closing_done_promise = std::move(sessions_closing_done_promise)](bool ok)
            {
                my_sessions_closing_done_promise->set_value(ok);
            });

        /// Wait for the requests to reach the leader, don't wait for commit.
        int64_t session_shutdown_timeout = keeper_context->getCoordinationSettings()[CoordinationSetting::session_shutdown_timeout].totalMilliseconds();
        auto start_time = std::chrono::steady_clock::now();
        if (sessions_closing_done.wait_for(std::chrono::milliseconds(session_shutdown_timeout)) != std::future_status::ready ||
            !sessions_closing_done.get())
            LOG_WARNING(
                log,
                "Failed to close sessions in {}ms. If they are not closed, they will be closed after session timeout.",
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count());
    }
}

bool KeeperRequestDispatcher2::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, bool use_xid_64)
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

    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.dispatcher_requests_queue, request->tracing_context);

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

            if (std::chrono::steady_clock::now() - start_time > std::chrono::milliseconds(keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds()))
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot push request to queue within operation timeout");
        }
    }

    /// requests_queue_bytes may briefly become negative if the other thread popped the request and
    /// decreased requests_queue_bytes before we got here.
    requests_queue_bytes.fetch_add(int64_t(getRequestBytesCost(*request)));
    CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequests);

    return true;
}

bool KeeperRequestDispatcher2::putLocalReadRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{
    /// Just go through the queue for now.
    return putRequest(request, session_id, /*use_xid_64=*/ true);
}

bool KeeperRequestDispatcher2::tryPopRequest(KeeperRequestForSession & request)
{
    bool res = requests_queue.tryPop(request);
    if (res)
    {
        requests_queue_bytes.fetch_sub(int64_t(getRequestBytesCost(*request.request)));
        CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequests);

        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request.request->spans.dispatcher_requests_queue,
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

void KeeperRequestDispatcher2::onResponse(KeeperResponseForSession response) noexcept
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
        /// This is unusual because dispatchThread doesn't start reqests when response queue is
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

void KeeperRequestDispatcher2::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard sessions_lock(sessions_mutex);

    auto [it, inserted] = sessions.try_emplace(session_id);
    if (!inserted)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);

    it->second.response_callback = std::move(callback);
    CurrentMetrics::add(CurrentMetrics::KeeperAliveConnections);
}

void KeeperRequestDispatcher2::finishSession(int64_t session_id)
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

void KeeperRequestDispatcher2::dispatchThread()
{
    try
    {
        DB::setThreadName(ThreadName::KEEPER_REQUEST);

        auto last_stuck_check_time = std::chrono::steady_clock::now();
        while (!shutting_down.load())
        {
            int64_t operation_timeout_ms = keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds();

            auto now = std::chrono::steady_clock::now();

            /// If stream is broken, drop in-flight requests.
            if (stream && stream->isBroken())
            {
                /// After we lost connection to leader, we want to sleep for multiple reasons:
                ///  1. If there are requests in flight, wait in hopes that they get committed.
                ///     (E.g. during graceful leader migration.)
                ///     After the sleep we'll have to fail the remaining in-flight requests and
                ///     close their client sessions.
                ///  2. If there's no healthy leader, we can't do much and can as well wait for
                ///     leader election to complete before proceeding. This may also give more
                ///     chance for in-flight requests to get committed and removed from in_flight_batches.
                ///  3. If there's no healthy leader, we don't want to spam reconnects very quickly.
                auto sleep_start = now;
                while (true)
                {
                    auto slept = std::chrono::steady_clock::now() - sleep_start;
                    if (slept >= std::chrono::milliseconds(operation_timeout_ms) || shutting_down.load())
                        break;
                    if (server->isLeaderAlive() &&
                        (!current_stream_is_suspect.load() || slept >= std::chrono::milliseconds(
                            keeper_context->getCoordinationSettings()[CoordinationSetting::stream_suspect_retry_delay_ms].totalMilliseconds())) &&
                        (head_idx.load() == tail_idx.load() || slept >= std::chrono::milliseconds(
                            keeper_context->getCoordinationSettings()[CoordinationSetting::stream_in_flight_drain_timeout_ms].totalMilliseconds())))
                        break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }

                dropInFlightRequests();
                stream.reset();
                continue;
            }

            if (!stream)
            {
                current_stream_is_suspect.store(true);
                stream = std::make_shared<KeeperAppendStream>(server);
            }

            /// Periodically check that we don't have stuck requests.
            /// In particular, we can get stuck if there's a bug that breaks stream guarantees
            /// (causes reordering or gaps) as our commit callback only checks completion of the request
            /// at the head of the queue.
            if (now > last_stuck_check_time + std::chrono::milliseconds(operation_timeout_ms))
            {
                last_stuck_check_time = now;
                size_t idx = head_idx.load();
                if (tail_idx.load() > idx && now > in_flight_batches[idx % in_flight_batches.size()].start_time + std::chrono::milliseconds(operation_timeout_ms))
                {
                    if (server->isLeaderAlive())
                        LOG_ERROR(log, "Detected stuck or reordered requests. Dropping. This may indicate a bug.");
                    stream->markAsBroken();
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
            size_t num_batches_in_flight = batch_idx - head_idx.load();
            int64_t max_response_queue_bytes = int64_t(keeper_context->getCoordinationSettings()[CoordinationSetting::max_response_queue_bytes_size]);
            if (num_batches_in_flight >= in_flight_batches.size() || response_bytes_in_all_queues.load() > max_response_queue_bytes / 2)
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
                std::this_thread::sleep_for(std::chrono::microseconds(
                    keeper_context->getCoordinationSettings()[CoordinationSetting::dispatch_busy_wait_sleep_us]));
                continue;
            }

            /// Pick a batch of requests.

            auto & coordination_settings = keeper_context->getCoordinationSettings();
            uint64_t max_batch_bytes_size = coordination_settings[CoordinationSetting::max_requests_batch_bytes_size];
            size_t max_batch_size = coordination_settings[CoordinationSetting::max_requests_batch_size];
            bool quorum_reads = coordination_settings[CoordinationSetting::quorum_reads];
            size_t max_read_batch_size = coordination_settings[CoordinationSetting::max_read_batch_size];
            size_t max_read_batch_bytes_size = coordination_settings[CoordinationSetting::max_read_batch_bytes_size];
            bool optimize_read_order = coordination_settings[CoordinationSetting::optimize_read_order];
            bool is_exceeding_memory_soft_limit = server->isExceedingMemorySoftLimit();

            KeeperRequestsForSessions requests;
            KeeperRequestsForSessions reconfig_requests;
            std::vector<std::pair<size_t, KeeperRequestsForSessions>> reads;
            size_t batch_bytes = 0;
            size_t batch_subrequests = 0;
            size_t reads_bytes = 0;
            size_t reads_subrequests = 0;

            {
                std::shared_lock sessions_lock(sessions_mutex);

                /// Consecutive read requests can be executed in parallel. So we want to reorder
                /// requests to group reads together as much as possible.
                /// We can only reorder requests from different sessions.
                /// A read can be moved to a later point, up to the next non-read request from the same session.
                /// As we iterate over requests, we try to shift all reads to as late as possible, until
                /// on of them bumps into the next request from its session; when it does, we flush all
                /// accumulated reads (deferred_reads) and continue.
                ///
                /// Invariant: Session::reordering_version == current_reordering_version
                ///            iff deferred_reads contains any requests for this session.
                KeeperRequestsForSessions deferred_reads;
                auto flush_deferred_reads = [&]
                {
                    if (deferred_reads.empty())
                        return;
                    chassert(!requests.empty());
                    reads.emplace_back(requests.size(), std::move(deferred_reads));
                    deferred_reads.clear();
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
                        /// Reconfig requests go through a separate pipeline, can't be fed into KeeperAppendStream.
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

                    /// If reads have to go through raft, it's sufficient to send the first read of a batch through raft.
                    /// NOTE: Currently this `if` block is reduntant with the `if (is_read && requests.empty())` below.
                    ///       It's left here to make sure we don't accidentally break quorum_reads if we change the condition below in future.
                    if (is_read && requests.empty() && quorum_reads)
                        is_read = false;

                    if (is_read && requests.empty())
                    {
                        /// Send the first request of a batch through raft, even if it's a read request, for simplicity.
                        /// All reads happen in commit thread.
                        /// We could do some things differently, e.g.:
                        ///  * Do reads right here in dispatchThread if there are no writes in flight.
                        ///    Or no writes for the reads' sessions in flight.
                        ///  * Allow InFlightBatch::reads' first element to contain reads to execute *before*
                        ///    the first request of the batch. Then we wouldn't have to put first read into the raft batch,
                        ///    but would still unnecessarily delay that read until the subsequent batch of writes is done.
                        ///  * Allow InFlightBatch::requests to be empty, so a batch contains only reads and doesn't go through raft.
                        ///    Its reads would be executed immediately after previous non-read-only batch is committed.
                        ///    Or, equivalently, allow adding to `reads` of an already in-flight batch.
                        is_read = false;
                    }

                    if (is_read)
                    {
                        if (session)
                            session->reordering_version = current_reordering_version;
                        reads_subrequests += getSubrequestCount(*request.request);
                        reads_bytes += getRequestBytesCost(*request.request);
                        deferred_reads.push_back(std::move(request));
                    }
                    else
                    {
                        if ((session && session->reordering_version == current_reordering_version) || !optimize_read_order)
                            flush_deferred_reads();

                        batch_subrequests += getSubrequestCount(*request.request);
                        batch_bytes += getRequestBytesCost(*request.request);
                        requests.push_back(std::move(request));
                    }
                } while (check_batch_size_limits() && tryPopRequest(request));

                flush_deferred_reads();
            }
            if (requests.empty())
                chassert(reads.empty());

            if (!requests.empty())
            {
                HistogramMetrics::observe(HistogramMetrics::KeeperCurrentBatchSizeElements, batch_subrequests);
                HistogramMetrics::observe(HistogramMetrics::KeeperCurrentBatchSizeBytes, batch_bytes);

                /// Add information about the batch to the queue of in-flight requests.

                auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
                batch.bytes = batch_bytes;
                batch.start_time = std::chrono::steady_clock::now();
                batch.requests = std::move(requests);
                batch.reads = std::move(reads);
                batch.activate();

                tail_idx.store(batch_idx + 1);

                /// Finally send the requests to leader.

                stream->putRequestBatch(batch.requests);
            }

            /// Ordering between Reconfig requests and other requests is not very important.
            for (const auto & reconfig_request : reconfig_requests)
                server->getKeeperStateMachine()->reconfigure(reconfig_request);
        }
    }
    catch (...)
    {
        tryLogCurrentException("Unexpected exception in KeeperRequestDispatcher2::dispatchThread");
        std::abort();
    }
}

void KeeperRequestDispatcher2::popBatch(size_t batch_idx)
{
    auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
    batch.deactivate();
    head_idx.store(batch_idx + 1);
}

void KeeperRequestDispatcher2::dropInFlightRequests()
{
    std::lock_guard stream_lock(request_completion_mutex);
    while (head_idx.load() < tail_idx.load())
    {
        size_t batch_idx = head_idx.load();
        auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
        while (batch.committed_requests < batch.requests.size())
        {
            addErrorResponse(batch.requests[batch.committed_requests], Coordination::Error::ZCONNECTIONLOSS);
            if (batch.reads_idx < batch.reads.size() && batch.reads[batch.reads_idx].first == batch.committed_requests + 1)
            {
                for (const auto & read_request : batch.reads[batch.reads_idx].second)
                    addErrorResponse(read_request, Coordination::Error::ZCONNECTIONLOSS);
                batch.reads_idx += 1;
            }
            batch.committed_requests += 1;
        }
        chassert(batch.reads_idx == batch.reads.size());
        popBatch(batch_idx);
    }
}

void KeeperRequestDispatcher2::addErrorResponse(const KeeperRequestForSession & request_for_session, Coordination::Error error)
{
    auto response = request_for_session.request->makeResponse();
    response->xid = request_for_session.request->xid;
    response->zxid = 0;
    response->error = error;
    response->enqueue_ts = std::chrono::steady_clock::now();
    onResponse(DB::KeeperResponseForSession{request_for_session.session_id, response});
}

void KeeperRequestDispatcher2::onCommit(const KeeperRequestForSession & request_for_session)
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
        return;

    if (current_stream_is_suspect.load())
        current_stream_is_suspect.store(false); // a request succeeded, the stream is working

    if (batch.reads_idx < batch.reads.size() && batch.reads[batch.reads_idx].first == batch.committed_requests + 1)
    {
        auto reads = std::move(batch.reads[batch.reads_idx].second);
        batch.reads_idx += 1;

        /// (We could re-check whether the requests' sessions are still alive, but it doesn't seem
        ///  worth the map lookup cost. We already checked session liveness just before starting
        ///  this raft batch, and hopefully raft commit latency doesn't get high in practice even
        ///  under too much load because we limit the number of in-flight batches.)
        ProfileEvents::increment(ProfileEvents::KeeperReadBatchCount);
        ProfileEvents::increment(ProfileEvents::KeeperReadBatchTotalRequests, reads.size());

        using namespace std::chrono;
        auto now_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        for (auto & r : reads)
            r.time = now_ms;

        for (const auto & r : reads)
            server->putLocalReadRequest(r);
    }

    batch.committed_requests += 1;

    if (batch.committed_requests == batch.requests.size())
    {
        chassert(batch.reads_idx == batch.reads.size());
        popBatch(batch_idx);
    }
}

void KeeperRequestDispatcher2::onResponseDeallocated(const Coordination::ZooKeeperResponse & response)
{
    size_t size = getResponseBytesCost(response);
    response_bytes_in_all_queues.fetch_sub(size);
}

void KeeperRequestDispatcher2::responseThread()
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
                ZooKeeperOpentelemetrySpans::maybeFinalize(
                    response_for_session.request->spans.dispatcher_responses_queue,
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
        tryLogCurrentException("Unexpected exception in KeeperRequestDispatcher2::responseThread");
        std::abort();
    }
}

}

#endif
