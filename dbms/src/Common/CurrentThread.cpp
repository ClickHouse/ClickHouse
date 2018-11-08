#include <memory>

#include "CurrentThread.h"
#include <common/logger_useful.h>
#include <Common/ThreadStatus.h>
#include <Common/ObjectPool.h>
#include <Common/TaskStatsInfoGetter.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Poco/Logger.h>


#if defined(ARCADIA_ROOT)
#   include <util/thread/singleton.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SimpleObjectPool<TaskStatsInfoGetter> task_stats_info_getter_pool;

// Smoker's implementation to avoid thread_local usage: error: undefined symbol: __cxa_thread_atexit
#if defined(ARCADIA_ROOT)
struct ThreadStatusPtrHolder : ThreadStatusPtr
{
    ThreadStatusPtrHolder() { ThreadStatusPtr::operator=(ThreadStatus::create()); }
};
struct ThreadScopePtrHolder : CurrentThread::ThreadScopePtr
{
    ThreadScopePtrHolder() { CurrentThread::ThreadScopePtr::operator=(std::make_shared<CurrentThread::ThreadScope>()); }
};
#   define current_thread (*FastTlsSingleton<ThreadStatusPtrHolder>())
#   define current_thread_scope (*FastTlsSingleton<ThreadScopePtrHolder>())
#else
/// Order of current_thread and current_thread_scope matters
thread_local ThreadStatusPtr _current_thread = ThreadStatus::create();
thread_local CurrentThread::ThreadScopePtr _current_thread_scope = std::make_shared<CurrentThread::ThreadScope>();
#   define current_thread _current_thread
#   define current_thread_scope _current_thread_scope
#endif

void CurrentThread::updatePerformanceCounters()
{
    get()->updatePerformanceCounters();
}

ThreadStatusPtr CurrentThread::get()
{
#ifndef NDEBUG
    if (!current_thread || current_thread.use_count() <= 0)
        throw Exception("Thread #" + std::to_string(Poco::ThreadNumber::get()) + " status was not initialized", ErrorCodes::LOGICAL_ERROR);

    if (Poco::ThreadNumber::get() != current_thread->thread_number)
        throw Exception("Current thread has different thread number", ErrorCodes::LOGICAL_ERROR);
#endif

    return current_thread;
}

CurrentThread::ThreadScopePtr CurrentThread::getScope()
{
    return current_thread_scope;
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
    get()->attachInternalTextLogsQueue(logs_queue);
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

ThreadGroupStatusPtr CurrentThread::getGroup()
{
    return get()->getThreadGroup();
}

}
