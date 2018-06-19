#include "CurrentThread.h"
#include <common/logger_useful.h>
#include <Common/ThreadStatus.h>
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


static ThreadStatusPtr getCurrentThreadImpl()
{
#ifndef NDEBUG
    if (!current_thread || current_thread.use_count() <= 0)
        throw Exception("Thread #" + std::to_string(Poco::ThreadNumber::get()) + " status was not initialized", ErrorCodes::LOGICAL_ERROR);

    if (Poco::ThreadNumber::get() != current_thread->thread_number)
        throw Exception("Current thread has different thread number", ErrorCodes::LOGICAL_ERROR);
#endif

    return current_thread;
}


void CurrentThread::initializeQuery()
{
    getCurrentThreadImpl()->initializeQuery();
}

void CurrentThread::attachTo(const ThreadGroupStatusPtr & thread_group)
{
    getCurrentThreadImpl()->attachQuery(thread_group, true);
}

void CurrentThread::attachToIfDetached(const ThreadGroupStatusPtr & thread_group)
{
    getCurrentThreadImpl()->attachQuery(thread_group, false);
}

void CurrentThread::updatePerformanceCounters()
{
    getCurrentThreadImpl()->updatePerformanceCounters();
}

ThreadStatusPtr CurrentThread::get()
{
    return getCurrentThreadImpl();
}

void CurrentThread::detachQuery()
{
    getCurrentThreadImpl()->detachQuery();
}

void CurrentThread::detachQueryIfNotDetached()
{
    getCurrentThreadImpl()->detachQuery(true);
}

ProfileEvents::Counters & CurrentThread::getProfileEvents()
{
    return current_thread->performance_counters;
}

MemoryTracker & CurrentThread::getMemoryTracker()
{
    return current_thread->memory_tracker;
}

void CurrentThread::updateProgressIn(const Progress & value)
{
    current_thread->progress_in.incrementPiecewiseAtomically(value);
}

void CurrentThread::updateProgressOut(const Progress & value)
{
    current_thread->progress_out.incrementPiecewiseAtomically(value);
}

void CurrentThread::attachInternalTextLogsQueue(const std::shared_ptr<InternalTextLogsQueue> & logs_queue)
{
    getCurrentThreadImpl()->attachInternalTextLogsQueue(logs_queue);
}

std::shared_ptr<InternalTextLogsQueue> CurrentThread::getInternalTextLogsQueue()
{
    /// NOTE: this method could be called at early server startup stage
    /// NOTE: this method could be called in ThreadStatus destructor, therefore we make use_count() check just in case

    if (!current_thread || current_thread.use_count() <= 0)
        return nullptr;

    if (current_thread->getCurrentState() == ThreadStatus::ThreadState::Died)
        return nullptr;

    return current_thread->getInternalTextLogsQueue();
}

std::string CurrentThread::getCurrentQueryID()
{
    if (!current_thread || current_thread.use_count() <= 0)
        return {};

    return current_thread->getQueryID();
}

ThreadGroupStatusPtr CurrentThread::getGroup()
{
    return getCurrentThreadImpl()->getThreadGroup();
}

void CurrentThread::attachQueryContext(Context & query_context)
{
    return getCurrentThreadImpl()->attachQueryContext(query_context);
}

void CurrentThread::finalizePerformanceCounters()
{
    getCurrentThreadImpl()->finalizePerformanceCounters();
}

}
