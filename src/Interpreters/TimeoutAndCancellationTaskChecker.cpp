#include "TimeoutAndCancellationTaskChecker.h"
#include <algorithm>
#include <base/types.h>
#include <Parsers/IAST.h>
// #include <Interpreters/Context.h>

#include <Interpreters/TemporaryDataOnDisk.h>


namespace DB
{

bool TimeoutAndCancellationTaskChecker::TimedTask::operator<(const TimedTask& other) const
{
    return expiry_time < other.expiry_time;
}

TimeoutAndCancellationTaskChecker::TimeoutAndCancellationTaskChecker(int timeout_microseconds)
    : timeout(timeout_microseconds), worker_thread([this]
    {
        this->threadFunction();
    }) {}

TimeoutAndCancellationTaskChecker::~TimeoutAndCancellationTaskChecker()
{
    shutdownChecker();
    if (worker_thread.joinable())
        worker_thread.join();
}

TimeoutAndCancellationTaskChecker& TimeoutAndCancellationTaskChecker::getInstance(int timeout_microseconds)
{
    static TimeoutAndCancellationTaskChecker instance(timeout_microseconds);
    return instance;
}

void TimeoutAndCancellationTaskChecker::addTask(const std::chrono::steady_clock::time_point& timestamp, const String & query_,
                                                const IAST * ast,ContextMutablePtr query_context, UInt64 watch_start_nanoseconds)
{
    const ClientInfo & client_info = query_context->getClientInfo();
    const Settings & settings = query_context->getSettingsRef();

    IAST::QueryKind query_kind = ast->getQueryKind();

    auto user_process_list = client_info;

    /// Actualize thread group info
    CurrentThread::attachQueryForLog(query_);
    auto thread_group = CurrentThread::getGroup();
    
    auto task = std::make_shared<QueryStatus>(
                query_context,
                query_,
                client_info,
                static_cast<int>(settings.priority),
                std::move(thread_group),
                query_kind,
                settings,
                watch_start_nanoseconds);

    std::lock_guard<std::mutex> lock(mutex);
    tasks.insert({timestamp + std::chrono::microseconds(timeout), task});
    cv.notify_one();
}

void TimeoutAndCancellationTaskChecker::shutdownChecker()
{
    shutdown = true;
    cv.notify_all();
}

void TimeoutAndCancellationTaskChecker::threadFunction()
{
    std::unique_lock<std::mutex> lock(mutex);
    while (!shutdown)
    {
        if (!tasks.empty())
        {
            auto next_expiry = tasks.begin()->expiry_time;
            cv.wait_until(lock, next_expiry, [this]
            {
                return shutdown || (!tasks.empty() && std::chrono::steady_clock::now() >= tasks.begin()->expiry_time);
            });

            while (!tasks.empty() && std::chrono::steady_clock::now() >= tasks.begin()->expiry_time)
            {
                auto expired_task = *tasks.begin();
                expired_task.task->cancelQuery(true);
                tasks.erase(tasks.begin());
            }
        }
        else
            cv.wait(lock, [this]
            {
                return shutdown || !tasks.empty();
            });
    }
}
}
