#include "CurrentThread.h"
#include <Common/ThreadStatus.h>
#include <Poco/Ext/ThreadNumber.h>
#include <common/logger_useful.h>
#include <Interpreters/ProcessList.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


static ThreadStatusPtr assertCurrentThread()
{
    ThreadStatusPtr thread = current_thread;

    if (!thread)
        throw Exception("Thread #" + std::to_string(Poco::ThreadNumber::get()) + " status was not initialized", ErrorCodes::LOGICAL_ERROR);

    if (Poco::ThreadNumber::get() != thread->poco_thread_number)
        throw Exception("Current thread has different thread number", ErrorCodes::LOGICAL_ERROR);

    return thread;
}


void CurrentThread::attachQuery(QueryStatus * parent_process)
{
    assertCurrentThread();

    if (!parent_process)
        current_thread->attachQuery(nullptr, nullptr, nullptr);
    else
        current_thread->attachQuery(parent_process, &parent_process->performance_counters, &parent_process->memory_tracker);
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
    assertCurrentThread();
    current_thread->updatePerfomanceCountersImpl();
}

ThreadStatusPtr CurrentThread::get()
{
    assertCurrentThread();
    return current_thread;
}

void CurrentThread::detachQuery()
{
    assertCurrentThread();
    current_thread->detachQuery();
}

bool CurrentThread::isAttachedToQuery()
{
    std::lock_guard lock(current_thread->mutex);
    return current_thread->is_active_query;
}

ProfileEvents::Counters & CurrentThread::getProfileEvents()
{
    return current_thread->performance_counters;
}

MemoryTracker & CurrentThread::getMemoryTracker()
{
    return current_thread->memory_tracker;
}

void CurrentThread::attachQueryFromSiblingThreadImpl(const ThreadStatusPtr & sibling_thread, bool check_detached)
{
    if (sibling_thread == nullptr)
        throw Exception("Sibling thread was not initialized", ErrorCodes::LOGICAL_ERROR);

    assertCurrentThread();

    QueryStatus * parent_query;
    ProfileEvents::Counters * parent_counters;
    MemoryTracker * parent_memory_tracker;
    {
        std::lock_guard lock(sibling_thread->mutex);
        parent_query = sibling_thread->parent_query;
        parent_counters = sibling_thread->performance_counters.parent;
        parent_memory_tracker = sibling_thread->memory_tracker.getParent();
    }

    current_thread->attachQuery(parent_query, parent_counters, parent_memory_tracker, check_detached);
}

}
