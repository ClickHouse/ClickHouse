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
extern const int QUERY_WAS_CANCELLED;
}

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

    struct Request
    {
        UInt64 reqiest_id;
        TBaseHolderPtr base_holder;
        std::shared_ptr<TPromiseStatus> promise;
        bool with_zk_fields;
    };

    struct RequestInfo
    {
        UInt64 request_id = 0;
        TFuture future;
    };

private:
    ThreadPool thread_pool;

    std::mutex mutex;
    std::unordered_map<TBaseHolderPtr, RequestInfo> current_requests TSA_GUARDED_BY(mutex);
    std::deque<Request> requests_to_schedule TSA_GUARDED_BY(mutex);
    UInt64 request_id TSA_GUARDED_BY(mutex) = 0;

    LoggerPtr log;

public:
    explicit StatusRequestsPool(const size_t max_threads)
        : thread_pool(create_pool(max_threads))
        , log(getLogger("StatusRequestsPool"))
    {
    }

    ~StatusRequestsPool()
    {
        thread_pool.wait();
        for (auto & request : requests_to_schedule)
            request.promise->set_exception(
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

            Request req;
            {
                std::lock_guard lock(mutex);
                if (requests_to_schedule.empty())
                    break;

                req = requests_to_schedule.front();

                if (req.reqiest_id > max_request_id)
                    break;

                requests_to_schedule.pop_front();
            }

            auto get_status_task = [this, req, thread_group = CurrentThread::getGroup()]() mutable
            {
                ThreadGroupSwitcher switcher(thread_group, get_thread_name());

                try
                {
                    TStatus status;

                    if (auto * holder = dynamic_cast<THolder *>(req.base_holder.get()))
                    {
                        holder->getStatus(status, req.with_zk_fields);
                    } else
                    {
                        if (!([&]
                            {
                            if (auto * var_holder = dynamic_cast<THolders *>(req.base_holder.get()))
                            {
                                var_holder->getStatus(status, req.with_zk_fields);
                                return true;
                            }

                            return false;
                        }() || ...))
                            status = {};
                    }

                    req.promise->set_value(std::move(status));
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Error getting status for " + get_holder_kind() + " " + get_holder_name(req.base_holder));
                    req.promise->set_exception(std::current_exception());
                }

                completeRequest(req.base_holder);
            };


            try
            {
                thread_pool.scheduleOrThrowOnError(std::move(get_status_task));
            }
            catch (...)
            {
                tryLogCurrentException(
                    log, "Error scheduling get status task for " + get_holder_kind() + " " + get_holder_name(req.base_holder));
                req.promise->set_exception(std::current_exception());
                completeRequest(req.base_holder);
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
            return ThreadName::DATABASE_REPLICAS;
        else
            return ThreadName::SYSTEM_REPLICAS;
    }

    static std::string get_holder_name(const TBaseHolderPtr & base_holder)
    {
        if constexpr (std::is_same_v<TBaseHolder, IDatabase>)
            return base_holder->getDatabaseName();
        else
            return base_holder->getStorageID().getNameForLogs();
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
