#include <memory>

#include "CurrentThread.h"
#include <Common/logger_useful.h>
#include <Common/ThreadStatus.h>
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

void CurrentThread::updatePerformanceCountersIfNeeded()
{
    if (unlikely(!current_thread))
        return;
    current_thread->updatePerformanceCountersIfNeeded();
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

std::shared_ptr<InternalTextLogsQueue> CurrentThread::getInternalTextLogsQueue()
{
    /// NOTE: this method could be called at early server startup stage
    if (unlikely(!current_thread))
        return nullptr;

    return current_thread->getInternalTextLogsQueue();
}

InternalProfileEventsQueuePtr CurrentThread::getInternalProfileEventsQueue()
{
    if (unlikely(!current_thread))
        return nullptr;

    return current_thread->getInternalProfileEventsQueue();
}

void CurrentThread::attachInternalTextLogsQueue(const std::shared_ptr<InternalTextLogsQueue> & logs_queue,
                                                LogsLevel client_logs_level)
{
    if (unlikely(!current_thread))
        return;
    current_thread->attachInternalTextLogsQueue(logs_queue, client_logs_level);
}


ThreadGroupPtr CurrentThread::getGroup()
{
    if (unlikely(!current_thread))
        return nullptr;

    return current_thread->getThreadGroup();
}

ContextPtr CurrentThread::getQueryContext()
{
    if (unlikely(!current_thread))
        return {};

    return current_thread->getQueryContext();
}

std::string_view CurrentThread::getQueryId()
{
    if (unlikely(!current_thread))
        return {};

    return current_thread->getQueryId();
}

void CurrentThread::attachReadResource(ResourceLink link)
{
    if (unlikely(!current_thread))
        return;
    if (current_thread->read_resource_link)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread #{} has been already attached to read resource", std::to_string(getThreadId()));
    current_thread->read_resource_link = link;
}

void CurrentThread::detachReadResource()
{
    if (unlikely(!current_thread))
        return;
    if (!current_thread->read_resource_link)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread #{} has not been attached to read resource", std::to_string(getThreadId()));
    current_thread->read_resource_link.reset();
}

ResourceLink CurrentThread::getReadResourceLink()
{
    if (unlikely(!current_thread))
        return {};
    return current_thread->read_resource_link;
}

void CurrentThread::attachWriteResource(ResourceLink link)
{
    if (unlikely(!current_thread))
        return;
    if (current_thread->write_resource_link)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread #{} has been already attached to write resource", std::to_string(getThreadId()));
    current_thread->write_resource_link = link;
}

void CurrentThread::detachWriteResource()
{
    if (unlikely(!current_thread))
        return;
    if (!current_thread->write_resource_link)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread #{} has not been attached to write resource", std::to_string(getThreadId()));
    current_thread->write_resource_link.reset();
}

ResourceLink CurrentThread::getWriteResourceLink()
{
    if (unlikely(!current_thread))
        return {};
    return current_thread->write_resource_link;
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
