#include "CurrentThread.h"
#include <common/logger_useful.h>
#include <Common/ThreadStatus.h>
#include <Interpreters/ProcessList.h>
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

void CurrentThread::attachQuery(QueryStatus * parent_process)
{
    ThreadStatusPtr thread = getCurrentThreadImpl();

    if (!parent_process)
        thread->attachQuery(nullptr, nullptr, nullptr, CurrentThread::getSystemLogsQueue());
    else
    {
        thread->attachQuery(
                parent_process, &parent_process->performance_counters, &parent_process->memory_tracker, CurrentThread::getSystemLogsQueue());
    }
}


void CurrentThread::attachQueryFromSiblingThread(const ThreadStatusPtr & sibling_thread)
{
    attachQueryFromSiblingThreadImpl(sibling_thread, true);
}

void CurrentThread::attachQueryFromSiblingThreadIfDetached(const ThreadStatusPtr & sibling_thread)
{
    attachQueryFromSiblingThreadImpl(sibling_thread, false);
}

void CurrentThread::updatePerformanceCounters()
{
    getCurrentThreadImpl()->updatePerformanceCountersImpl();
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

void CurrentThread::attachQueryFromSiblingThreadImpl(ThreadStatusPtr sibling_thread, bool check_detached)
{
    if (sibling_thread == nullptr)
        throw Exception("Sibling thread was not initialized", ErrorCodes::LOGICAL_ERROR);

    ThreadStatusPtr thread = getCurrentThreadImpl();

    if (sibling_thread->getCurrentState() == ThreadStatus::ThreadState::QueryInitializing)
    {
        LOG_WARNING(thread->log, "An attempt to \'fork\' from initializing thread detected."
                                 << " Performance statistics for this thread will be inaccurate");
    }

    QueryStatus * parent_query;
    ProfileEvents::Counters * parent_counters;
    MemoryTracker * parent_memory_tracker;
    SystemLogsQueueWeakPtr logs_queue_ptr;
    {
        /// NOTE: It is almost the only place where ThreadStatus::mutex is required
        /// In other cases ThreadStatus must be accessed only from the current_thread
        std::lock_guard lock(sibling_thread->mutex);

        parent_query = sibling_thread->parent_query;
        if (parent_query)
        {
            parent_counters = &parent_query->performance_counters;
            parent_memory_tracker = &parent_query->memory_tracker;
        }
        else
        {
            /// Fallback
            parent_counters = sibling_thread->performance_counters.getParent();
            parent_memory_tracker = sibling_thread->memory_tracker.getParent();
        }
        logs_queue_ptr = sibling_thread->logs_queue_ptr;
    }

    thread->attachQuery(parent_query, parent_counters, parent_memory_tracker, logs_queue_ptr, check_detached);
}

void CurrentThread::attachSystemLogsQueue(const std::shared_ptr<SystemLogsQueue> & logs_queue)
{
    getCurrentThreadImpl()->attachSystemLogsQueue(logs_queue);
}

std::shared_ptr<SystemLogsQueue> CurrentThread::getSystemLogsQueue()
{
    /// NOTE: this method could be called at early server startup stage
    /// NOTE: this method could be called in ThreadStatus destructor, therefore we make use_count() check just in case

    if (!current_thread || current_thread.use_count() <= 0)
        return nullptr;

    if (current_thread->getCurrentState() == ThreadStatus::ThreadState::Died)
        return nullptr;

    return current_thread->getSystemLogsQueue();
}

std::string CurrentThread::getCurrentQueryID()
{
    if (!current_thread || current_thread.use_count() <= 0 || !current_thread->parent_query)
        return {};

    return current_thread->parent_query->client_info.current_query_id;
}

}
