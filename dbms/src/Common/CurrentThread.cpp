#include <memory>

#include "CurrentThread.h"
#include <common/logger_useful.h>
#include <common/likely.h>
#include <Common/ThreadStatus.h>
#include <Common/TaskStatsInfoGetter.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Poco/Logger.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void CurrentThread::updatePerformanceCounters()
{
    get().updatePerformanceCounters();
}

ThreadStatus & CurrentThread::get()
{
    if (unlikely(!current_thread))
        throw Exception("Thread #" + std::to_string(Poco::ThreadNumber::get()) + " status was not initialized", ErrorCodes::LOGICAL_ERROR);

    return *current_thread;
}

ProfileEvents::Counters & CurrentThread::getProfileEvents()
{
    return current_thread ? get().performance_counters : ProfileEvents::global_counters;
}

MemoryTracker & CurrentThread::getMemoryTracker()
{
    return get().memory_tracker;
}

void CurrentThread::updateProgressIn(const Progress & value)
{
    get().progress_in.incrementPiecewiseAtomically(value);
}

void CurrentThread::updateProgressOut(const Progress & value)
{
    get().progress_out.incrementPiecewiseAtomically(value);
}

void CurrentThread::attachInternalTextLogsQueue(const std::shared_ptr<InternalTextLogsQueue> & logs_queue)
{
    get().attachInternalTextLogsQueue(logs_queue);
}

std::shared_ptr<InternalTextLogsQueue> CurrentThread::getInternalTextLogsQueue()
{
    /// NOTE: this method could be called at early server startup stage
    if (!current_thread)
        return nullptr;

    if (get().getCurrentState() == ThreadStatus::ThreadState::Died)
        return nullptr;

    return get().getInternalTextLogsQueue();
}

ThreadGroupStatusPtr CurrentThread::getGroup()
{
    if (!current_thread)
        return nullptr;

    return get().getThreadGroup();
}

}
