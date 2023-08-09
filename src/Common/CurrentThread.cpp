#include <memory>

#include "CurrentThread.h"
#include <Common/logger_useful.h>
#include <Common/ThreadStatus.h>
#include <Common/TaskStatsInfoGetter.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>
#include <base/getThreadId.h>
#include <Poco/Logger.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void CurrentThread::updatePerformanceCounters()
{
    if (unlikely(!current_thread))
        return;
    current_thread->updatePerformanceCounters();
}

bool CurrentThread::isInitialized()
{
    return current_thread;
}

ThreadStatus & CurrentThread::get()
{
    if (unlikely(!current_thread))
        throw Exception("Thread #" + std::to_string(getThreadId()) + " status was not initialized", ErrorCodes::LOGICAL_ERROR);

    return *current_thread;
}

ProfileEvents::Counters & CurrentThread::getProfileEvents()
{
    return current_thread ? current_thread->performance_counters : ProfileEvents::global_counters;
}

void CurrentThread::updateProgressIn(const Progress & value)
{
    if (unlikely(!current_thread))
        return;
    current_thread->progress_in.incrementPiecewiseAtomically(value);
}

void CurrentThread::updateProgressOut(const Progress & value)
{
    if (unlikely(!current_thread))
        return;
    current_thread->progress_out.incrementPiecewiseAtomically(value);
}

void CurrentThread::attachInternalTextLogsQueue(const std::shared_ptr<InternalTextLogsQueue> & logs_queue,
                                                LogsLevel client_logs_level)
{
    if (unlikely(!current_thread))
        return;
    current_thread->attachInternalTextLogsQueue(logs_queue, client_logs_level);
}

void CurrentThread::setFatalErrorCallback(std::function<void()> callback)
{
    if (unlikely(!current_thread))
        return;
    current_thread->setFatalErrorCallback(callback);
}

std::shared_ptr<InternalTextLogsQueue> CurrentThread::getInternalTextLogsQueue()
{
    /// NOTE: this method could be called at early server startup stage
    if (unlikely(!current_thread))
        return nullptr;

    if (current_thread->getCurrentState() == ThreadStatus::ThreadState::Died)
        return nullptr;

    return current_thread->getInternalTextLogsQueue();
}

void CurrentThread::attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & queue)
{
    if (unlikely(!current_thread))
        return;
    current_thread->attachInternalProfileEventsQueue(queue);
}

InternalProfileEventsQueuePtr CurrentThread::getInternalProfileEventsQueue()
{
    if (unlikely(!current_thread))
        return nullptr;

    if (current_thread->getCurrentState() == ThreadStatus::ThreadState::Died)
        return nullptr;

    return current_thread->getInternalProfileEventsQueue();
}

ThreadGroupStatusPtr CurrentThread::getGroup()
{
    if (unlikely(!current_thread))
        return nullptr;

    return current_thread->getThreadGroup();
}

}
