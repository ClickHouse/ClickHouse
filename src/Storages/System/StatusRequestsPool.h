#pragma once

#include <concepts>
#include <Common/ThreadGroupSwitcher.h>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <type_traits>
#include <unordered_map>

#include <Databases/IDatabase.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/StorageID.h>
#include <QueryPipeline/QueryPipeline.h>
#include <base/defines.h>
#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/escapeForFileName.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>


namespace CurrentMetrics
{
extern const Metric SystemReplicasThreads;
extern const Metric SystemReplicasThreadsActive;
extern const Metric SystemReplicasThreadsScheduled;
extern const Metric SystemDatabaseReplicasThreads;
extern const Metric SystemDatabaseReplicasThreadsActive;
extern const Metric SystemDatabaseReplicasThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
extern const int ABORTED;
extern const int QUERY_WAS_CANCELLED;
}

namespace FailPoints
{
extern const char system_replicas_schedule_requests_pause[];
}

/// Defined in StatusRequestsPool.cpp so that this header does not need Interpreters/Context.h.
StoragePtr resolveStatusRequestTable(const StorageID & storage_id);
DatabasePtr resolveStatusRequestDatabase(const String & database_name);

template <typename T>
concept IsHolder = std::derived_from<T, IDatabase> || std::derived_from<T, IStorage>;

template <IsHolder T>
struct StatusHolderBase
{
    using Base = std::conditional_t<std::derived_from<T, IDatabase>, IDatabase, IStorage>;
};

template <IsHolder THolder, IsHolder ...THolders>
requires (... && std::is_same_v<typename StatusHolderBase<THolder>::Base, typename StatusHolderBase<THolders>::Base>) &&
         (... && std::is_same_v<typename THolder::ReplicatedStatus, typename THolders::ReplicatedStatus>)
class StatusRequestsPool;

template <class T, class ...Ts>
class StatusRequestsPools final
{
public:
    explicit StatusRequestsPools(const size_t max_threads)
        : requests_without_zk_fields(max_threads)
        , requests_with_zk_fields(max_threads)
    {
    }

    using StatusPool = StatusRequestsPool<T, Ts...>;
    StatusPool requests_without_zk_fields;
    StatusPool requests_with_zk_fields;
};

/// Allows to "deduplicate" getStatus() requests for the same holder: if a request for a holder is already in progress
/// then the new request will return the same future as the previous one.
template <IsHolder THolder, IsHolder ...THolders>
requires (... && std::is_same_v<typename StatusHolderBase<THolder>::Base, typename StatusHolderBase<THolders>::Base>) &&
         (... && std::is_same_v<typename THolder::ReplicatedStatus, typename THolders::ReplicatedStatus>)
class StatusRequestsPool final
{
public:
    using TStatus = typename THolder::ReplicatedStatus;
    using TPromiseStatus = std::promise<TStatus>;
    using TBaseHolder = StatusHolderBase<THolder>::Base;
    using TBaseHolderPtr = std::shared_ptr<TBaseHolder>;
    using TFuture = std::shared_future<TStatus>;

    /// The pool must not own the holder: a request abandoned by a killed query would keep
    /// a dropped table alive, so DROP TABLE SYNC on it would hang until the pool drains.
    using THolderId = std::conditional_t<std::is_same_v<TBaseHolder, IDatabase>, String, StorageID>;

    struct Request
    {
        UInt64 request_id{};
        THolderId holder_id = make_empty_holder_id();
        std::shared_ptr<TPromiseStatus> promise;
        bool with_zk_fields{};
    };

    struct RequestInfo
    {
        UInt64 request_id = 0;
        TFuture future;
    };

private:
    std::mutex mutex;
    std::unordered_map<String, RequestInfo> current_requests TSA_GUARDED_BY(mutex);
    std::deque<Request> requests_to_schedule TSA_GUARDED_BY(mutex);
    UInt64 request_id TSA_GUARDED_BY(mutex) = 0;

    LoggerPtr log;

    /// thread_pool must be declared last so it is destroyed first.
    /// ~ThreadPool calls finalize() which sets shutdown = true, wakes idle workers,
    /// and joins them. By destroying thread_pool before other members, we guarantee
    /// that worker threads (which access mutex, current_requests, and log) have all
    /// exited before those members are destroyed.
    ThreadPool thread_pool;

public:
    explicit StatusRequestsPool(const size_t max_threads)
        : log(getLogger("StatusRequestsPool"))
        , thread_pool(create_pool(max_threads))
    {
    }

    ~StatusRequestsPool()
    {
        /// Do not call thread_pool.wait() here:
        /// wait() only waits for scheduled_jobs == 0 but does NOT set shutdown = true,
        /// so idle worker threads remain blocked on the local pool's condition variable
        /// (new_job_or_shutdown at ThreadPool.cpp:736) forever.
        /// When GlobalThreadPool::shutdown() later tries to pthread_join the underlying
        /// OS threads stuck in these worker loops, it deadlocks.
        ///
        /// The fix: thread_pool is declared as the last member, so ~ThreadPool runs first.
        /// It calls finalize() which sets shutdown = true, wakes all workers, and joins
        /// them while mutex, current_requests, and log are still alive.

        for (auto & request : requests_to_schedule)
            request.promise->set_exception(
                std::make_exception_ptr(DB::Exception(ErrorCodes::QUERY_WAS_CANCELLED, "StatusRequestsPool is destroyed")));
    }

    RequestInfo addRequest(const TBaseHolderPtr & holder, const bool with_zk_fields)
    {
        std::shared_ptr<TPromiseStatus> promise;
        TFuture future;
        UInt64 this_request_id = 0;

        THolderId holder_id = get_holder_id(holder);
        String holder_key = get_holder_key(holder_id);

        {
            std::lock_guard lock(mutex);

            auto existing_request = current_requests.find(holder_key);
            if (existing_request != current_requests.end())
            {
                LOG_DEBUG(log, "Attaching to existing request for {} {}", get_holder_kind(), get_holder_name(holder_id));
                return existing_request->second;
            }

            this_request_id = request_id;
            ++request_id;

            promise = std::make_shared<TPromiseStatus>();
            future = promise->get_future().share();

            current_requests[holder_key] = {.request_id = this_request_id, .future = future};

            LOG_DEBUG(log, "Making new request for {} {}", get_holder_kind(), get_holder_name(holder_id));

            requests_to_schedule.emplace_back(this_request_id, std::move(holder_id), promise, with_zk_fields);
        }

        return {this_request_id, future};
    }

    void scheduleRequests(const UInt64 max_request_id, const QueryStatusPtr query_status)
    {
        while (true)
        {
            fiu_do_on(FailPoints::system_replicas_schedule_requests_pause, {
                FailPointInjection::pauseFailPoint(FailPoints::system_replicas_schedule_requests_pause);
            });

            if (query_status)
                query_status->checkTimeLimit();

            Request req;
            {
                std::lock_guard lock(mutex);
                if (requests_to_schedule.empty())
                    break;

                req = requests_to_schedule.front();

                if (req.request_id > max_request_id)
                    break;

                requests_to_schedule.pop_front();
            }

            auto get_status_task = [this, req, thread_group = getCurrentThreadGroup()]() mutable
            {
                ThreadGroupSwitcher switcher(thread_group, get_thread_name());

                try
                {
                    TStatus status;

                    /// The holder could have been dropped or detached while the request was waiting in the queue.
                    TBaseHolderPtr base_holder = resolve_holder(req.holder_id);
                    if (!base_holder)
                        throw Exception(ErrorCodes::ABORTED, "Cannot get status of {} {}: it does not exist anymore",
                            get_holder_kind(), get_holder_name(req.holder_id));

                    if (auto * holder = dynamic_cast<THolder *>(base_holder.get()))
                    {
                        holder->getStatus(status, req.with_zk_fields);
                    } else
                    {
                        if (!([&]
                            {
                            if (auto * var_holder = dynamic_cast<THolders *>(base_holder.get()))
                            {
                                var_holder->getStatus(status, req.with_zk_fields);
                                return true;
                            }

                            return false;
                        }() || ...))
                            throw Exception(ErrorCodes::ABORTED, "Cannot get status of {} {}: it was replaced by an object of another type",
                                get_holder_kind(), get_holder_name(req.holder_id));
                    }

                    req.promise->set_value(std::move(status));
                }
                catch (...)
                {
                    /// A holder that went away is an expected outcome and the consumers just skip
                    /// its row, so it is not worth an error in the log.
                    if (getCurrentExceptionCode() == ErrorCodes::ABORTED)
                        LOG_DEBUG(log, "Cannot get status for {} {}: {}", get_holder_kind(), get_holder_name(req.holder_id), getCurrentExceptionMessage(false));
                    else
                        tryLogCurrentException(log, "Error getting status for " + get_holder_kind() + " " + get_holder_name(req.holder_id));
                    req.promise->set_exception(std::current_exception());
                }

                completeRequest(req.holder_id);
            };


            try
            {
                thread_pool.scheduleOrThrowOnError(std::move(get_status_task));
            }
            catch (...)
            {
                tryLogCurrentException(
                    log, "Error scheduling get status task for " + get_holder_kind() + " " + get_holder_name(req.holder_id));
                req.promise->set_exception(std::current_exception());
                completeRequest(req.holder_id);
            }
        }
    }

private:
    void completeRequest(const THolderId & holder_id)
    {
        std::lock_guard lock(mutex);
        current_requests.erase(get_holder_key(holder_id));
    }

    static constexpr std::string get_holder_kind()
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
            return "database";
        else
            return "table";
    }

    static THolderId make_empty_holder_id()
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
            return String{};
        else
            return StorageID::createEmpty();
    }

    static THolderId get_holder_id(const TBaseHolderPtr & holder)
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
            return holder->getDatabaseName();
        else
            return holder->getStorageID();
    }

    /// The deduplication key. Names are escaped because they may contain dots on their own,
    /// and an escaped name never contains a dash, so it cannot look like a UUID.
    static String get_holder_key(const THolderId & holder_id)
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
        {
            return holder_id;
        }
        else
        {
            if (holder_id.hasUUID())
                return toString(holder_id.uuid);

            return escapeForFileName(holder_id.database_name) + "." + escapeForFileName(holder_id.table_name);
        }
    }

    static TBaseHolderPtr resolve_holder(const THolderId & holder_id)
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
            return resolveStatusRequestDatabase(holder_id);
        else
            return resolveStatusRequestTable(holder_id);
    }

    static constexpr auto get_thread_name()
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
            return ThreadName::DATABASE_REPLICAS;
        else
            return ThreadName::SYSTEM_REPLICAS;
    }

    static std::string get_holder_name(const THolderId & holder_id)
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
            return holder_id;
        else
            return holder_id.getNameForLogs();
    }

    static auto create_pool(const size_t max_threads)
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
        {
            return ThreadPool(
                CurrentMetrics::SystemDatabaseReplicasThreads,
                CurrentMetrics::SystemDatabaseReplicasThreadsActive,
                CurrentMetrics::SystemDatabaseReplicasThreadsScheduled,
                max_threads);
        }
        else
        {
            return ThreadPool(
                CurrentMetrics::SystemReplicasThreads,
                CurrentMetrics::SystemReplicasThreadsActive,
                CurrentMetrics::SystemReplicasThreadsScheduled,
                max_threads);
        }
    }
};
}
