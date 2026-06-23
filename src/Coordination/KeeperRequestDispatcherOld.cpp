#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperRequestDispatcherOld.h>

#if USE_NURAFT

#include <Common/ProfiledLocks.h>
#include <libnuraft/async.hxx>

#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <base/hex.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/OpenTelemetryTracingContext.h>
#include <Common/HistogramMetrics.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/setThreadName.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/checkStackSize.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
#include <Coordination/KeeperCommon.h>
#include <Common/ZooKeeper/KeeperSpans.h>
#include <Interpreters/Context.h>
#include <Common/thread_local_rng.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperReconfiguration.h>

#include <IO/ReadHelpers.h>
#include <Disks/IDisk.h>

#include <atomic>
#include <future>
#include <chrono>
#include <limits>
#include <string>

#include <fmt/ranges.h>

#if USE_JEMALLOC
#include <Common/Jemalloc.h>
#endif

namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
    extern const Metric KeeperOutstandingRequests;
}

namespace ProfileEvents
{
    extern const Event KeeperCommitsFailed;
    extern const Event KeeperCommitWaitElapsedMicroseconds;
    extern const Event KeeperBatchMaxCount;
    extern const Event KeeperBatchMaxTotalSize;
    extern const Event KeeperReadBatchCount;
    extern const Event KeeperReadBatchTotalRequests;
    extern const Event KeeperRequestRejectedDueToSoftMemoryLimitCount;
    extern const Event KeeperStaleRequestsSkipped;
    extern const Event KeeperLiveSessionsLockWaitMicroseconds;
    extern const Event KeeperSessionCallbackLockWaitMicroseconds;
    extern const Event KeeperReadRequestQueueLockWaitMicroseconds;
}

namespace HistogramMetrics
{
    extern Metric & KeeperCurrentBatchSizeElements;
    extern Metric & KeeperCurrentBatchSizeBytes;
}

using namespace std::chrono_literals;

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsMilliseconds dead_session_check_period_ms;
    extern const CoordinationSettingsUInt64 max_request_queue_size;
    extern const CoordinationSettingsUInt64 max_requests_batch_bytes_size;
    extern const CoordinationSettingsUInt64 max_requests_batch_size;
    extern const CoordinationSettingsUInt64 max_read_batch_bytes_size;
    extern const CoordinationSettingsUInt64 max_read_batch_size;
    extern const CoordinationSettingsMilliseconds operation_timeout_ms;
    extern const CoordinationSettingsBool quorum_reads;
    extern const CoordinationSettingsMilliseconds session_shutdown_timeout;
    extern const CoordinationSettingsMilliseconds sleep_before_leader_change_ms;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYSTEM_ERROR;
}

namespace
{

bool checkIfRequestIncreaseMem(const Coordination::ZooKeeperRequestPtr & request)
{
    if (request->getOpNum() == Coordination::OpNum::Create
        || request->getOpNum() == Coordination::OpNum::Create2
        || request->getOpNum() == Coordination::OpNum::CreateTTL
        || request->getOpNum() == Coordination::OpNum::CreateIfNotExists
        || request->getOpNum() == Coordination::OpNum::Set)
    {
        return true;
    }
    if (request->getOpNum() == Coordination::OpNum::Multi)
    {
        Coordination::ZooKeeperMultiRequest & multi_req = dynamic_cast<Coordination::ZooKeeperMultiRequest &>(*request);
        Int64 memory_delta = 0;
        for (const auto & sub_req : multi_req.requests)
        {
            auto sub_zk_request = std::dynamic_pointer_cast<Coordination::ZooKeeperRequest>(sub_req);
            switch (sub_zk_request->getOpNum())
            {
                case Coordination::OpNum::Create:
                case Coordination::OpNum::Create2:
                case Coordination::OpNum::CreateTTL:
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

}

KeeperRequestDispatcherOld::KeeperRequestDispatcherOld(KeeperServer * server_)
    : responses_queue(std::numeric_limits<size_t>::max())
    , server(server_)
    , log(getLogger("KeeperRequestDispatcherOld"))
    , keeper_context(server->getKeeperContext())
{
    requests_queue = std::make_unique<RequestsQueue>(keeper_context->getCoordinationSettings()[CoordinationSetting::max_request_queue_size]);
    request_thread = ThreadFromGlobalPool([this] { requestThread(); });
    responses_thread = ThreadFromGlobalPool([this] { responseThread(); });
}

void KeeperRequestDispatcherOld::onResponse(KeeperResponseForSession response) noexcept
{
    int64_t session_id = response.session_id;
    if (!responses_queue.push(std::move(response)))
    {
        ProfileEvents::increment(ProfileEvents::KeeperCommitsFailed);
        LOG_WARNING(log,
            "Failed to push response with session id {} to the queue, probably because of shutdown",
            session_id);
    }
}

void KeeperRequestDispatcherOld::onCommit(const KeeperRequestForSession & request_for_session)
{
    KeeperRequestsForSessions pending_reads;
    {
        /// check if we have queue of read requests depending on this request to be committed
        SessionAndXID key(request_for_session.session_id, request_for_session.request->xid);
        ProfiledMutexLock lock(read_request_queue_mutex, ProfileEvents::KeeperReadRequestQueueLockWaitMicroseconds);
        if (auto it = read_request_queue.find(key); it != read_request_queue.end())
        {
            pending_reads = std::move(it->second);
            read_request_queue.erase(it);
        }
    }

    /// Bulk filter stale sessions under one lock acquisition.
    /// (We already checked staleness before adding to `pending_reads` in the first place.
    ///  This re-checking is only useful if raft commit latency gets very high, which I'm
    ///  not sure happens in practice even under too much load.)
    {
        ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds);
        int64_t last_checked_session_id = keeper_internal_get_session_id;
        bool last_checked_session_live = true;
        std::erase_if(pending_reads, [&](const KeeperRequestForSession & read_request)
        {
            if (read_request.session_id != last_checked_session_id)
            {
                last_checked_session_id = read_request.session_id;
                last_checked_session_live = live_sessions.contains(last_checked_session_id);
            }
            if (!last_checked_session_live)
            {
                ProfileEvents::increment(ProfileEvents::KeeperStaleRequestsSkipped);
                read_request.request->spans.maybeFinalize(
                    KeeperSpan::ReadWaitForWrite,
                    [&]
                    {
                        return std::vector<OpenTelemetry::SpanAttribute>{
                            {"keeper.operation", Coordination::opNumToString(read_request.request->getOpNum())},
                            {"keeper.session_id", read_request.session_id},
                            {"keeper.xid", read_request.request->xid},
                            {"keeper.stale", true},
                        };
                    },
                    OpenTelemetry::SpanStatus::ERROR,
                    "Session is no longer live");
                return true;
            }
            return false;
        });
    }

    /// Finalize wait-for-write spans and dispatch batch.
    if (!pending_reads.empty())
    {
        for (auto & read_request : pending_reads)
        {
            read_request.request->spans.maybeFinalize(
                KeeperSpan::ReadWaitForWrite,
                [&]
                {
                    return std::vector<OpenTelemetry::SpanAttribute>{
                        {"keeper.operation", Coordination::opNumToString(read_request.request->getOpNum())},
                        {"keeper.session_id", read_request.session_id},
                        {"keeper.xid", read_request.request->xid},
                    };
                });
        }

        if (server->isLeaderAlive())
        {
            ProfileEvents::increment(ProfileEvents::KeeperReadBatchCount);
            ProfileEvents::increment(ProfileEvents::KeeperReadBatchTotalRequests, pending_reads.size());

            using namespace std::chrono;
            auto now_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            for (auto & r : pending_reads)
                r.time = now_ms;

            server->putLocalReadRequests(pending_reads);
        }
        else
        {
            addErrorResponses(pending_reads, Coordination::Error::ZCONNECTIONLOSS, /*may_have_dependent_reads=*/ false);
        }
    }

    /// When Close commits, remove the session from `live_sessions` so that
    /// stale requests still sitting in the backed-up queue will be filtered.
    /// This covers the window between Close commit and `finishSession`
    /// (e.g. `sessionCleanerTask` expired the session but the TCP handler
    /// hasn't disconnected yet). Fires on ALL nodes via RAFT, which is
    /// how followers learn about closed sessions.
    if (request_for_session.request->getOpNum() == Coordination::OpNum::Close)
    {
        ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds);
        live_sessions.erase(request_for_session.session_id);
    }
}

void KeeperRequestDispatcherOld::requestThread()
{
    DB::setThreadName(ThreadName::KEEPER_REQUEST);

    /// Result of requests batch from previous iteration
    RaftAppendResult prev_result = nullptr;
    /// Requests from previous iteration. We store them to be able
    /// to send errors to the client.
    KeeperRequestsForSessions prev_batch;

    /// When draining reads we may pop a non-read request; save it for the next iteration.
    std::optional<KeeperRequestForSession> pending_request;

    const auto & shutdown_called = keeper_context->isShutdownCalled();

    while (!shutdown_called)
    {
        const auto handle_opentelemetry_spans = [this](const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
        {
            if (session_id != keeper_internal_ttl_garbage_collector_session_id)
            {
                request->spans.maybeFinalize(
                    KeeperSpan::DispatcherRequestsQueue,
                    [&]
                    {
                        return std::vector<OpenTelemetry::SpanAttribute>{
                            {"keeper.operation", Coordination::opNumToString(request->getOpNum())},
                            {"keeper.session_id", session_id},
                            {"keeper.xid", request->xid},
                            {"keeper.dispatcher.requests_queue.size", requests_queue->size()},
                        };
                    });
            }
        };

        KeeperRequestForSession request;

        const auto & dynamic_settings = keeper_context->getCoordinationSettings();
        uint64_t operation_timeout_ms = dynamic_settings[CoordinationSetting::operation_timeout_ms].totalMilliseconds();
        uint64_t max_batch_bytes_size = dynamic_settings[CoordinationSetting::max_requests_batch_bytes_size];
        size_t max_batch_size = dynamic_settings[CoordinationSetting::max_requests_batch_size];
        size_t max_read_batch_size = dynamic_settings[CoordinationSetting::max_read_batch_size];
        size_t max_read_batch_bytes_size = dynamic_settings[CoordinationSetting::max_read_batch_bytes_size];
        bool quorum_reads = dynamic_settings[CoordinationSetting::quorum_reads];

        /// The code below do a very simple thing: batch all write (quorum) requests into vector until
        /// previous write batch is not finished or max_batch size achieved. The main complexity goes from
        /// the ability to process read requests without quorum (from local state). So when we are collecting
        /// requests into a batch we must check that the new request is not read request. Otherwise we have to
        /// process all already accumulated write requests, wait them synchronously and only after that process
        /// read request. So reads are some kind of "separator" for writes.
        /// Also there is a special reconfig request also being a separator.
        try
        {
            if (pending_request)
            {
                request = std::move(*pending_request);
                pending_request.reset();
            }
            else if (requests_queue->tryPop(request, operation_timeout_ms))
            {
                CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequests);
            }
            else
            {
                continue;
            }

            if (shutdown_called)
                break;

            /// Skip stale requests for sessions that are no longer live.
            /// Close must pass through RAFT (ephemeral cleanup, watch cleanup, etc.).
            /// SessionID uses internal IDs (session_id = -1), ignore it just to be safe.
            int64_t last_checked_session_id = keeper_internal_get_session_id;
            bool last_checked_session_live = true;
            auto is_stale_session_request = [&](const KeeperRequestForSession & req) -> bool
            {
                if (req.request->getOpNum() != Coordination::OpNum::Close
                    && req.request->getOpNum() != Coordination::OpNum::SessionID)
                {
                    /// Internal sessions (negative IDs, e.g. TTL garbage collector) are always live.
                    if (req.session_id < 0)
                        return false;

                    /// Small optimization: if we check the same session id multiple times in a row,
                    /// do the lookup once and cache the result.
                    if (req.session_id != last_checked_session_id)
                    {
                        ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds);
                        last_checked_session_id = req.session_id;
                        last_checked_session_live = live_sessions.contains(last_checked_session_id);
                    }
                    if (!last_checked_session_live)
                    {
                        ProfileEvents::increment(ProfileEvents::KeeperStaleRequestsSkipped);

                        /// Finalize the dispatcher_requests_queue span that was initialized
                        /// when the request was enqueued. Without this the span leaks because
                        /// handle_opentelemetry_spans (which normally finalizes it) is skipped.
                        req.request->spans.maybeFinalize(
                            KeeperSpan::DispatcherRequestsQueue,
                            [&]
                            {
                                return std::vector<OpenTelemetry::SpanAttribute>{
                                    {"keeper.operation", Coordination::opNumToString(req.request->getOpNum())},
                                    {"keeper.session_id", req.session_id},
                                    {"keeper.xid", req.request->xid},
                                    {"keeper.stale", true},
                                };
                            },
                            OpenTelemetry::SpanStatus::ERROR,
                            "Session is no longer live");

                        return true;
                    }
                }
                return false;
            };

            if (is_stale_session_request(request))
                continue;

            handle_opentelemetry_spans(request.request, request.session_id);

            Int64 mem_soft_limit = keeper_context->getKeeperMemorySoftLimit();
            if (server->isExceedingMemorySoftLimit() && checkIfRequestIncreaseMem(request.request))
            {
                ProfileEvents::increment(ProfileEvents::KeeperRequestRejectedDueToSoftMemoryLimitCount, 1);
                LOG_WARNING(
                    log,
                    "Processing requests refused because of max_memory_usage_soft_limit {}, the total allocated memory is {}, RSS is {}, request type "
                    "is {}",
                    ReadableSize(mem_soft_limit),
                    ReadableSize(total_memory_tracker.get()),
                    ReadableSize(total_memory_tracker.getRSS()),
                    request.request->getOpNum());
                addErrorResponses({request}, Coordination::Error::ZOUTOFMEMORY, /*may_have_dependent_reads=*/ false);
                continue;
            }

            KeeperRequestsForSessions current_batch;
            size_t current_batch_bytes_size = 0;
            KeeperRequestsForSessions read_batch;

            bool has_reconfig_request = false;

            /// If new request is not read request or reconfig request we must process it through quorum.
            /// Otherwise we will process it locally.
            if (request.request->getOpNum() == Coordination::OpNum::Reconfig)
                has_reconfig_request = true;
            else if (quorum_reads || !request.request->isReadRequest())
            {
                current_batch_bytes_size += request.request->bytesSize();
                current_batch.emplace_back(request);
                size_t reads_count = 0;
                size_t reads_bytes_size = 0;

                const auto try_get_request = [&]
                {
                    /// Trying to get batch requests as fast as possible
                    if (requests_queue->tryPop(request))
                    {
                        CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequests);

                        /// Skip stale requests for sessions that are no longer live during batch assembly.
                        if (is_stale_session_request(request))
                            return true; // consumed, keep draining

                        handle_opentelemetry_spans(request.request, request.session_id);

                        /// Don't append read request into batch, we have to process them separately
                        if (!quorum_reads && request.request->isReadRequest())
                        {
                            const auto & last_request = current_batch.back();
                            request.request->spans.maybeInitialize(KeeperSpan::ReadWaitForWrite, request.request->tracing_context.get());
                            ProfiledMutexLock lock(read_request_queue_mutex, ProfileEvents::KeeperReadRequestQueueLockWaitMicroseconds);
                            reads_count += 1;
                            reads_bytes_size += request.request->bytesSize();
                            read_request_queue[{last_request.session_id, last_request.request->xid}].push_back(request);
                        }
                        else if (request.request->getOpNum() == Coordination::OpNum::Reconfig)
                        {
                            has_reconfig_request = true;
                            return false;
                        }
                        else
                        {
                            current_batch_bytes_size += request.request->bytesSize();
                            current_batch.emplace_back(request);
                        }

                        return true;
                    }

                    return false;
                };

                while (!shutdown_called && current_batch.size() < max_batch_size && !has_reconfig_request
                        && current_batch_bytes_size < max_batch_bytes_size
                        && reads_count < max_read_batch_size && reads_bytes_size < max_read_batch_bytes_size
                        && try_get_request())
                    ;

                const auto prev_result_done = [&]
                {
                    return !prev_result || prev_result->has_result();
                };

                /// Waiting until previous append will be successful, or batch is big enough
                while (!shutdown_called && !has_reconfig_request &&
                        !prev_result_done() && current_batch.size() <= max_batch_size
                        && current_batch_bytes_size < max_batch_bytes_size)
                {
                    try_get_request();
                }
            }
            else
            {
                /// Read request with no pending writes — batch consecutive reads.
                size_t reads_bytes_size = request.request->bytesSize();
                read_batch.push_back(request);

                KeeperRequestForSession next;
                while (!shutdown_called
                       && read_batch.size() < max_read_batch_size && reads_bytes_size < max_read_batch_bytes_size
                       && requests_queue->tryPop(next))
                {
                    CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequests);

                    if (is_stale_session_request(next))
                        continue;

                    handle_opentelemetry_spans(next.request, next.session_id);

                    if (next.request->isReadRequest())
                    {
                        reads_bytes_size += next.request->bytesSize();
                        read_batch.push_back(std::move(next));
                    }
                    else
                    {
                        /// Non-read request — save for next iteration.
                        pending_request = std::move(next);
                        break;
                    }
                }
            }

            if (shutdown_called)
                break;

            bool execute_requests_after_write = !read_batch.empty() || has_reconfig_request;

            nuraft::ptr<nuraft::buffer> result_buf = nullptr;
            /// Forcefully process all previous pending requests
            if (prev_result)
                result_buf
                    = forceWaitAndProcessResult(prev_result, prev_batch, /*clear_requests_on_success=*/!execute_requests_after_write);

            /// Process collected write requests batch
            if (!current_batch.empty())
            {
                if (current_batch.size() >= max_batch_size)
                    ProfileEvents::increment(ProfileEvents::KeeperBatchMaxCount, 1);
                else if (current_batch_bytes_size >= max_batch_bytes_size)
                    ProfileEvents::increment(ProfileEvents::KeeperBatchMaxTotalSize, 1);

                LOG_TEST(log, "Processing requests batch, size: {}, bytes: {}", current_batch.size(), current_batch_bytes_size);

                HistogramMetrics::observe(HistogramMetrics::KeeperCurrentBatchSizeElements, static_cast<HistogramMetrics::Value>(current_batch.size()));
                HistogramMetrics::observe(HistogramMetrics::KeeperCurrentBatchSizeBytes, static_cast<HistogramMetrics::Value>(current_batch_bytes_size));

                auto result = server->putRequestBatch(current_batch);

                if (!result)
                {
                    addErrorResponses(current_batch, Coordination::Error::ZCONNECTIONLOSS);
                    current_batch.clear();
                    current_batch_bytes_size = 0;
                }

                prev_batch = std::move(current_batch);
                prev_result = result;
            }

            /// If we will execute read or reconfig next, we have to process result now
            if (execute_requests_after_write)
            {
                Stopwatch watch;
                SCOPE_EXIT(ProfileEvents::increment(ProfileEvents::KeeperCommitWaitElapsedMicroseconds, watch.elapsedMicroseconds()));
                if (prev_result)
                    result_buf = forceWaitAndProcessResult(
                        prev_result, prev_batch, /*clear_requests_on_success=*/!execute_requests_after_write);

                /// In case of older version or disabled async replication, result buf will be set to value of `commit` function
                /// which always returns nullptr
                /// in that case we don't have to do manual wait because are already sure that the batch was committed when we get
                /// the result back
                /// otherwise, we need to manually wait until the batch is committed
                /// TODO: there are a few problems:
                ///  * There can be multiple forceWaitAndProcessResult calls for different
                ///    batches between waitCommittedUpto calls.
                ///    In such case, the addErrorResponses below would apply only to the
                ///    latest of those batches, but they may all be failed.
                ///  * Of those multiple forceWaitAndProcessResult calls, it's possible that an
                ///    earlier one succeeds but a later one fails. Then we won't call
                ///    waitCommittedUpto on the log_idx from the earlier batch, so a subsequent
                ///    read may happen before that write is committed, violating
                ///    read-after-write consistency.
                ///  * With async replication, it's possible for requests to fail even after
                ///    their forceWaitAndProcessResult call succeeds, if the leader died after
                ///    accepting the requests for processing (and returning log_idx) but before
                ///    sending them to a majority of followers. In such case we'll never send
                ///    a response to the client for those requests. And we may execute
                ///    subsequent requests from the same session and send responses for those,
                ///    violating ordering of responses.
                if (result_buf)
                {
                    nuraft::buffer_serializer bs(result_buf);
                    auto log_idx = bs.get_u64();

                    /// if timeout happened set error responses for the requests
                    if (!keeper_context->waitCommittedUpto(log_idx, operation_timeout_ms))
                        addErrorResponses(prev_batch, Coordination::Error::ZOPERATIONTIMEOUT);

                    if (shutdown_called)
                        return;
                }

                prev_batch.clear();
            }

            if (has_reconfig_request)
                server->getKeeperStateMachine()->reconfigure(request);

            /// Dispatch batched read requests
            if (!read_batch.empty())
            {
                if (server->isLeaderAlive())
                {
                    ProfileEvents::increment(ProfileEvents::KeeperReadBatchCount);
                    ProfileEvents::increment(ProfileEvents::KeeperReadBatchTotalRequests, read_batch.size());

                    using namespace std::chrono;
                    auto now_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                    for (auto & r : read_batch)
                        r.time = now_ms;

                    /// (Note: it might make sense to re-check is_stale_session_request here, in
                    ///  case waitCommittedUpto took a while and some sessions expired.
                    ///  But hopefully waitCommittedUpto doesn't take very long even when the
                    ///  servers are overloaded - requests would pile up in KeeperDispatcher
                    ///  queues but would still move quickly through raft.)

                    server->putLocalReadRequests(read_batch);
                }
                else
                {
                    addErrorResponses(read_batch, Coordination::Error::ZCONNECTIONLOSS, /*may_have_dependent_reads=*/ false);
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void KeeperRequestDispatcherOld::responseThread()
{
    DB::setThreadName(ThreadName::KEEPER_RESPONSE);

    const auto & shutdown_called = keeper_context->isShutdownCalled();
    while (!shutdown_called)
    {
        KeeperResponseForSession response_for_session;

        uint64_t max_wait = keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds();

        if (responses_queue.tryPop(response_for_session, max_wait))
        {
            if (shutdown_called)
                break;

            const UInt64 dequeue_time_us = ZooKeeperOpentelemetrySpans::now();

            bool response_was_sent = false;
            try
            {
                response_was_sent = setResponse(response_for_session.session_id, response_for_session.response, response_for_session.request);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

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
}

bool KeeperRequestDispatcherOld::setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)
{
    /// Extract callback under lock, invoke outside to avoid serializing callback
    /// latency under session_to_response_callback_mutex. This is safe because:
    /// - KeeperTCPHandler callbacks capture shared_ptrs by value (always alive)
    /// - KeeperOverDispatcher callbacks capture shared_ptr<CallbackState> (always alive)
    /// Note: setResponse is called from responseThread which is single-threaded,
    /// so concurrent setResponse calls for the same session do not happen.
    /// However, for non-Close responses, finishSession on another thread may
    /// concurrently invoke a copy of the same callback (for ZSESSIONEXPIRED).
    /// Both current callback implementations handle this safely:
    /// KeeperTCPHandler pushes to a ConcurrentBoundedQueue, and
    /// KeeperOverDispatcher's CallbackState is protected by its own mutex.
    ZooKeeperResponseCallback callback;
    {
        ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds);

        auto session_response_callback = session_to_response_callback.find(session_id);

        /// Session was disconnected, just skip this response
        if (session_response_callback == session_to_response_callback.end())
            return false;

        /// Session closed, no more writes — use move to avoid std::function copy overhead
        if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::Close)
        {
            callback = std::move(session_response_callback->second);
            session_to_response_callback.erase(session_response_callback);
            CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
        }
        else
        {
            /// Copy, not move — the entry must stay in the map for future
            /// responses on this session (watches, subsequent requests).
            callback = session_response_callback->second;
        }
    }

    callback(response, request);
    return true;
}

bool KeeperRequestDispatcherOld::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, bool use_xid_64)
{
    if (request->getOpNum() != Coordination::OpNum::Close &&
        request->getOpNum() != Coordination::OpNum::SessionID &&
        session_id >= 0)
    {
        /// If session was already disconnected then we will ignore requests.
        /// Internal sessions (negative IDs, e.g. TTL garbage collector) don't have a callback registered.
        ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds);
        if (!session_to_response_callback.contains(session_id))
            return false;
    }

    KeeperRequestForSession request_info;
    request_info.use_xid_64 = use_xid_64;
    request_info.request = request;
    using namespace std::chrono;
    request_info.time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    request_info.session_id = session_id;

    if (keeper_context->isShutdownCalled())
        return false;

    request->spans.maybeInitialize(KeeperSpan::DispatcherRequestsQueue, request->tracing_context.get());

    if (!requests_queue->tryPush(std::move(request_info), keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds()))
    {
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot push request to queue within operation timeout");
    }
    CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequests);
    return true;
}

void KeeperRequestDispatcherOld::shutdown()
{
    if (requests_queue)
        requests_queue->finish();
    if (request_thread.joinable())
        request_thread.join();

    responses_queue.finish();
    if (responses_thread.joinable())
        responses_thread.join();

    KeeperRequestForSession request_for_session;

    /// Set session expired for all pending requests
    while (requests_queue && requests_queue->tryPop(request_for_session))
    {
        CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequests);
        auto response = request_for_session.request->makeResponse();
        response->error = Coordination::Error::ZSESSIONEXPIRED;
        setResponse(request_for_session.session_id, response);
    }

    KeeperRequestsForSessions close_requests;
    {
        /// Clear all registered sessions
        ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds);

        if (server && server->isLeaderAlive())
        {
            close_requests.reserve(session_to_response_callback.size());
            // send to leader CLOSE requests for active sessions
            for (const auto & [session, response] : session_to_response_callback)
            {
                auto request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
                request->xid = Coordination::CLOSE_XID;
                using namespace std::chrono;
                KeeperRequestForSession request_info
                {
                    .session_id = session,
                    .time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count(),
                    .request = std::move(request),
                    .digest = std::nullopt
                };

                close_requests.push_back(std::move(request_info));
            }
        }

        session_to_response_callback.clear();
    }

    if (server && !close_requests.empty())
    {
        // if there is no leader, there is no reason to do CLOSE because it's a write request
        if (server->isLeaderAlive())
        {
            LOG_INFO(log, "Trying to close {} session(s)", close_requests.size());
            const auto raft_result = server->putRequestBatch(close_requests);
            auto sessions_closing_done_promise = std::make_shared<std::promise<void>>();
            auto sessions_closing_done = sessions_closing_done_promise->get_future();
            raft_result->when_ready([my_sessions_closing_done_promise = std::move(sessions_closing_done_promise)](
                                        nuraft::cmd_result<nuraft::ptr<nuraft::buffer>> & /*result*/,
                                        nuraft::ptr<std::exception> & /*exception*/) { my_sessions_closing_done_promise->set_value(); });

            auto session_shutdown_timeout = keeper_context->getCoordinationSettings()[CoordinationSetting::session_shutdown_timeout].totalMilliseconds();
            if (sessions_closing_done.wait_for(std::chrono::milliseconds(session_shutdown_timeout)) != std::future_status::ready)
                LOG_WARNING(
                    log,
                    "Failed to close sessions in {}ms. If they are not closed, they will be closed after session timeout.",
                    session_shutdown_timeout);
        }
        else
        {
            LOG_INFO(log, "Sessions cannot be closed during shutdown because there is no active leader");
        }
    }
}

void KeeperRequestDispatcherOld::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    bool inserted = false;
    {
        ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds);
        inserted = live_sessions.insert(session_id).second;
    }

    try
    {
        ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds);
        if (!session_to_response_callback.try_emplace(session_id, callback).second)
            throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);
        CurrentMetrics::add(CurrentMetrics::KeeperAliveConnections);
    }
    catch (...)
    {
        if (inserted)
        {
            ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds);
            live_sessions.erase(session_id);
        }
        throw;
    }
}

void KeeperRequestDispatcherOld::finishSession(int64_t session_id)
{
    /// shutdown() method will cleanup sessions if needed
    if (keeper_context->isShutdownCalled())
        return;

    ZooKeeperResponseCallback callback;
    {
        ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds);
        auto session_it = session_to_response_callback.find(session_id);
        if (session_it != session_to_response_callback.end())
        {
            callback = std::move(session_it->second);
            session_to_response_callback.erase(session_it);
            CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
        }
        else
        {
            /// Session was already finished by another path (e.g. `sessionCleanerTask`
            /// raced with `KeeperTCPHandler`). That path already erased from
            /// `live_sessions`.
            return;
        }
    }

    /// Remove from live_sessions so `requestThread` can skip stale requests
    /// still sitting in the queue for this session.
    {
        ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds);
        live_sessions.erase(session_id);
    }

    /// Notify the callback that session is being closed before removing it
    /// This allows clients to mark themselves as expired
    if (callback)
    {
        auto close_response = std::make_shared<Coordination::ZooKeeperCloseResponse>();
        close_response->error = Coordination::Error::ZSESSIONEXPIRED;
        callback(close_response, nullptr);
    }
}

void KeeperRequestDispatcherOld::addErrorResponses(const KeeperRequestsForSessions & requests_for_sessions, Coordination::Error error, bool may_have_dependent_reads)
{
    KeeperRequestsForSessions dependent_reads;

    for (const auto & request_for_session : requests_for_sessions)
    {
        KeeperResponsesForSessions responses;
        auto response = request_for_session.request->makeResponse();
        response->xid = request_for_session.request->xid;
        response->zxid = 0;
        response->error = error;
        response->enqueue_ts = std::chrono::steady_clock::now();
        if (!responses_queue.push(DB::KeeperResponseForSession{request_for_session.session_id, response}))
            throw Exception(ErrorCodes::SYSTEM_ERROR,
                "Could not push error response xid {} zxid {} error message {} to responses queue",
                response->xid,
                response->zxid,
                error);

        if (may_have_dependent_reads)
        {
            SessionAndXID key(request_for_session.session_id, request_for_session.request->xid);
            ProfiledMutexLock lock(read_request_queue_mutex, ProfileEvents::KeeperReadRequestQueueLockWaitMicroseconds);
            if (auto it = read_request_queue.find(key); it != read_request_queue.end())
            {
                dependent_reads.insert(dependent_reads.end(), std::move_iterator(it->second.begin()), std::move_iterator(it->second.end()));
                read_request_queue.erase(it);
            }
        }
    }

    /// Cancel reads that we piggy-backed to the request that failed. They're innocent bystanders
    /// that could otherwise succeed, but we don't have a simple way to run these reads correctly
    /// in this situation. In particular, there may be later write requests from their sessions that
    /// already completed; in that case we can't do the read at all, our committed state is too new.
    if (!dependent_reads.empty())
        addErrorResponses(dependent_reads, error, /*may_have_dependent_reads=*/ false);
}

nuraft::ptr<nuraft::buffer> KeeperRequestDispatcherOld::forceWaitAndProcessResult(
    RaftAppendResult & result, KeeperRequestsForSessions & requests_for_sessions, bool clear_requests_on_success)
{
    if (!result->has_result())
        result->get();

    /// If we get some errors, send them to clients
    if (!result->get_accepted() || result->get_result_code() == nuraft::cmd_result_code::TIMEOUT)
        addErrorResponses(requests_for_sessions, Coordination::Error::ZOPERATIONTIMEOUT);
    else if (result->get_result_code() != nuraft::cmd_result_code::OK)
        addErrorResponses(requests_for_sessions, Coordination::Error::ZCONNECTIONLOSS);

    auto result_buf = result->get();

    result = nullptr;

    if (!result_buf || clear_requests_on_success)
        requests_for_sessions.clear();

    return result_buf;
}

uint64_t KeeperRequestDispatcherOld::SessionAndXIDHash::operator()(std::pair<int64_t, Coordination::XID> p) const
{
    return CityHash_v1_0_2::Hash128to64({uint64_t(p.first), uint64_t(p.second)});
}

}

#endif
