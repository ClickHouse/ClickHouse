#pragma once

#include <concepts>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <type_traits>
#include <unordered_map>

#include <Databases/IDatabase.h>
#include <Interpreters/ProcessList.h>
#include <QueryPipeline/QueryPipeline.h>
#include <base/defines.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/logger_useful.h>


namespace CurrentMetrics
{
extern const Metric SystemDatabaseReplicasThreads;
extern const Metric SystemDatabaseReplicasThreadsActive;
extern const Metric SystemDatabaseReplicasThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
}

template <typename T>
concept IsHolder = std::derived_from<T, IDatabase> || std::derived_from<T, IStorage>;

template <IsHolder THolder>
class StatusRequestsPool;

template <class T>
class StatusRequestsPools final
{
public:
    explicit StatusRequestsPools(const size_t max_threads)
        : requests_without_zk_fields(max_threads)
        , requests_with_zk_fields(max_threads)
    {
    }

    using StatusPool = StatusRequestsPool<T>;
    StatusPool requests_without_zk_fields;
    StatusPool requests_with_zk_fields;
};

/// Allows to "deduplicate" getStatus() requests for the same holder: if a request for a holder is already in progress
/// then the new request will return the same future as the previous one.
template <IsHolder THolder>
class StatusRequestsPool final
{
public:
    using TStatus = typename THolder::ReplicatedStatus;
    using TPromiseStatus = std::promise<TStatus>;
    using TBaseHolder = typename std::conditional_t<std::derived_from<THolder, IDatabase>, IDatabase, IStorage>;
    using TBaseHolderPtr = std::shared_ptr<TBaseHolder>;
    using TRequest = std::tuple<UInt64, TBaseHolderPtr, std::shared_ptr<TPromiseStatus>, bool>;
    using TFuture = std::shared_future<TStatus>;

    static constexpr size_t POS_REQUEST_ID_IN_REQUEST = 0;
    static constexpr size_t POS_PROMISE_IN_REQUEST = 2;

    struct RequestInfo
    {
        UInt64 request_id = 0;
        TFuture future;
    };

private:
    ThreadPool thread_pool;

    std::mutex mutex;
    std::unordered_map<TBaseHolderPtr, RequestInfo> current_requests TSA_GUARDED_BY(mutex);
    std::deque<TRequest> requests_to_schedule TSA_GUARDED_BY(mutex);
    UInt64 request_id TSA_GUARDED_BY(mutex) = 0;

    LoggerPtr log;

public:
    explicit StatusRequestsPool(const size_t max_threads)
        : thread_pool(
              CurrentMetrics::SystemDatabaseReplicasThreads,
              CurrentMetrics::SystemDatabaseReplicasThreadsActive,
              CurrentMetrics::SystemDatabaseReplicasThreadsScheduled,
              max_threads)
        , log(getLogger("StatusRequestsPool"))
    {
    }

    ~StatusRequestsPool()
    {
        thread_pool.wait();
        for (auto & request : requests_to_schedule)
            std::get<POS_PROMISE_IN_REQUEST>(request)->set_exception(
                std::make_exception_ptr(DB::Exception(ErrorCodes::QUERY_WAS_CANCELLED, "StatusRequestsPool is destroyed")));
    }

    RequestInfo addRequest(const TBaseHolderPtr holder, const bool with_zk_fields)
    {
        std::shared_ptr<TPromiseStatus> promise;
        TFuture future;
        UInt64 this_request_id = 0;

        {
            std::lock_guard lock(mutex);

            auto existing_request = current_requests.find(holder);
            if (existing_request != current_requests.end())
            {
                LOG_DEBUG(log, "Attaching to existing request for {} {}", get_holder_kind(), get_holder_name(holder));
                return existing_request->second;
            }

            this_request_id = request_id;
            ++request_id;

            promise = std::make_shared<TPromiseStatus>();
            future = promise->get_future().share();

            current_requests[holder] = {.request_id = this_request_id, .future = future};

            LOG_DEBUG(log, "Making new request for {} {}", get_holder_kind(), get_holder_name(holder));

            requests_to_schedule.emplace_back(this_request_id, holder, promise, with_zk_fields);
        }

        return {this_request_id, future};
    }

    void scheduleRequests(const UInt64 max_request_id, const QueryStatusPtr query_status)
    {
        while (true)
        {
            if (query_status)
                query_status->checkTimeLimit();

            TRequest req;
            {
                std::lock_guard lock(mutex);
                if (requests_to_schedule.empty())
                    break;

                req = requests_to_schedule.front();

                if (std::get<POS_REQUEST_ID_IN_REQUEST>(req) > max_request_id)
                    break;

                requests_to_schedule.pop_front();
            }

            auto get_status_task = [this, req, thread_group = CurrentThread::getGroup()]() mutable
            {
                ThreadGroupSwitcher switcher(thread_group, get_thread_name());

                auto & [_, base_holder, promise, with_zk_fields] = req;
                try
                {
                    TStatus status;

                    if (auto * holder = dynamic_cast<THolder *>(base_holder.get()))
                    {
                        holder->getStatus(status, with_zk_fields);
                    }

                    promise->set_value(std::move(status));
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Error getting status for " + get_holder_kind() + " " + get_holder_name(base_holder));
                    promise->set_exception(std::current_exception());
                }

                completeRequest(base_holder);
            };

            auto & [_, base_holder, promise, with_zk_fields] = req;

            try
            {
                thread_pool.scheduleOrThrowOnError(std::move(get_status_task));
            }
            catch (...)
            {
                tryLogCurrentException(
                    log, "Error scheduling get status task for " + get_holder_kind() + " " + get_holder_name(base_holder));
                promise->set_exception(std::current_exception());
                completeRequest(base_holder);
            }
        }
    }

private:
    void completeRequest(TBaseHolderPtr base_holder)
    {
        std::lock_guard lock(mutex);
        current_requests.erase(base_holder);
    }

    static constexpr std::string get_holder_kind()
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
            return "database";
        else
            return "table";
    }

    static constexpr auto get_thread_name()
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
            return "DBReplicas";
        else
            return "SystemReplicas";
    }

    static std::string get_holder_name(const TBaseHolderPtr & base_holder)
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
            return base_holder->getDatabaseName();
        else
            return base_holder->getStorageID().getNameForLogs();
    }
};

}
