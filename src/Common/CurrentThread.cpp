#include <chrono>
#include <memory>

#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <Common/ThreadStatus.h>
#include <Core/Settings.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>
#include <base/getThreadId.h>
#include <Poco/Logger.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 interactive_delay;
}

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

ContextPtr CurrentThread::tryGetQueryContext()
{
    if (unlikely(!current_thread))
        return {};

    return current_thread->tryGetQueryContext();
}

void CurrentThread::throwIfQueryCancelled()
{
    if (auto query_context = tryGetQueryContext())
    {
        /// Incoming cancel packets on a TCP connection are only processed when we send progress.
        /// Slow operations (analysis steps, long function calls) don't emit intermediate progress,
        /// so the interactive cancel callback would never fire from the executor side. Invoke it
        /// here, throttled to at most once per `interactive_delay` per thread so we don't flood
        /// the wire or do redundant work in tight polling loops.
        if (auto cancel_callback = query_context->getInteractiveCancelCallback())
        {
            /// Track context identity so the throttle resets on query boundaries: a worker thread
            /// can run multiple queries back-to-back, and we don't want query A's recent poll to
            /// suppress query B's first cancel check. Stored as `const void *` because we never
            /// dereference it — pointer identity is enough, and a freed-then-reused address only
            /// risks a single throttled poll (the same outcome as if A and B shared the slot).
            static thread_local const void * last_query_context = nullptr;
            static thread_local std::chrono::steady_clock::time_point last_callback_at{};
            const auto * current_query_context = static_cast<const void *>(query_context.get());
            const auto now = std::chrono::steady_clock::now();
            const auto period = std::chrono::microseconds(query_context->getSettingsRef()[Setting::interactive_delay]);
            if (current_query_context != last_query_context || now - last_callback_at >= period)
            {
                last_query_context = current_query_context;
                last_callback_at = now;
                cancel_callback();
            }
        }

        /// `checkTimeLimit` enforces both `is_killed` (set by KILL QUERY or by the
        /// CancellationChecker reaching the deadline) and the elapsed-time limit directly,
        /// so the polling site stays responsive even if the CancellationChecker thread is
        /// slow to fire the deadline.
        if (auto query_status = query_context->getProcessListElementSafe())
            query_status->checkTimeLimit();
    }
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

void CurrentThread::attachReadThrottler(const ThrottlerPtr & throttler)
{
    if (unlikely(!current_thread))
        return;
    if (current_thread->read_throttler)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread #{} has been already attached to read throttler", std::to_string(getThreadId()));
    current_thread->read_throttler = throttler;
}

void CurrentThread::detachReadThrottler()
{
    if (unlikely(!current_thread))
        return;
    if (!current_thread->read_throttler)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread #{} has not been attached to read throttler", std::to_string(getThreadId()));
    current_thread->read_throttler.reset();
}

ThrottlerPtr CurrentThread::getReadThrottler()
{
    if (unlikely(!current_thread))
        return {};
    return current_thread->read_throttler;
}

void CurrentThread::attachWriteThrottler(const ThrottlerPtr & throttler)
{
    if (unlikely(!current_thread))
        return;
    if (current_thread->write_throttler)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread #{} has been already attached to write throttler", std::to_string(getThreadId()));
    current_thread->write_throttler = throttler;
}

void CurrentThread::detachWriteThrottler()
{
    if (unlikely(!current_thread))
        return;
    if (!current_thread->write_throttler)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread #{} has not been attached to write throttler", std::to_string(getThreadId()));
    current_thread->write_throttler.reset();
}

ThrottlerPtr CurrentThread::getWriteThrottler()
{
    if (unlikely(!current_thread))
        return {};
    return current_thread->write_throttler;
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
