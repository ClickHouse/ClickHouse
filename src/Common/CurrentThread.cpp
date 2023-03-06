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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread #{} status was not initialized", std::to_string(getThreadId()));

    return *current_thread;
}

ProfileEvents::Counters & CurrentThread::getProfileEvents()
{
    return current_thread ? *current_thread->current_performance_counters : ProfileEvents::global_counters;
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
    /// It does not make sense to set a callback for sending logs to a client if there's no thread status
    chassert(current_thread);
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

MemoryTracker * CurrentThread::getUserMemoryTracker()
{
    if (unlikely(!current_thread))
        return nullptr;

    auto * tracker = current_thread->memory_tracker.getParent();
    while (tracker && tracker->level != VariableContext::User)
        tracker = tracker->getParent();

    return tracker;
}

void CurrentThread::flushUntrackedMemory()
{
    if (unlikely(!current_thread))
        return;
    current_thread->flushUntrackedMemory();
}

}
