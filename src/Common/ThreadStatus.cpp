#include <Common/Exception.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/QueryProfiler.h>
#include <Common/ThreadStatus.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <base/getPageSize.h>
#include <base/errnoToString.h>
#include <Interpreters/Context.h>

#include <Poco/Logger.h>

#include <csignal>
#include <sys/mman.h>


namespace DB
{

thread_local ThreadStatus constinit * current_thread = nullptr;

#if !defined(SANITIZER)
namespace
{

/// For aarch64 16K is not enough (likely due to tons of registers)
constexpr size_t UNWIND_MINSIGSTKSZ = 32 << 10;

/// Alternative stack for signal handling.
///
/// This stack should not be located in the TLS (thread local storage), since:
/// - TLS locates data on the per-thread stack
/// - And in case of stack in the signal handler will grow too much,
///   it will start overwriting TLS storage
///   (and note, that it is not too small, due to StackTrace obtaining)
/// - Plus there is no way to determine TLS block size, yes there is
///   __pthread_get_minstack() in glibc, but it is private and hence not portable.
///
/// Also we should not use getStackSize() (pthread_attr_getstack()) since it
/// will return 8MB, and this is too huge for signal stack.
struct ThreadStack
{
    ThreadStack()
        : data(aligned_alloc(getPageSize(), getSize()))
    {
        /// Add a guard page
        /// (and since the stack grows downward, we need to protect the first page).
        mprotect(data, getPageSize(), PROT_NONE);
    }
    ~ThreadStack()
    {
        mprotect(data, getPageSize(), PROT_WRITE|PROT_READ);
        free(data);
    }

    static size_t getSize() { return std::max<size_t>(UNWIND_MINSIGSTKSZ, MINSIGSTKSZ); }
    void * getData() const { return data; }

private:
    /// 16 KiB - not too big but enough to handle error.
    void * data;
};

}

static thread_local ThreadStack alt_stack;
static thread_local bool has_alt_stack = false;
#endif

ThreadGroup::ThreadGroup()
    : master_thread_id(CurrentThread::get().thread_id)
{}

ThreadStatus::ThreadStatus(bool check_current_thread_on_destruction_)
    : thread_id{getThreadId()}, check_current_thread_on_destruction(check_current_thread_on_destruction_)
{
    chassert(!current_thread);

    last_rusage = std::make_unique<RUsageCounters>();

    memory_tracker.setDescription("Thread");
    log = getLogger("ThreadStatus");

    current_thread = this;

    /// NOTE: It is important not to do any non-trivial actions (like updating ProfileEvents or logging) before ThreadStatus is created
    /// Otherwise it could lead to SIGSEGV due to current_thread dereferencing

    /// Will set alternative signal stack to provide diagnostics for stack overflow errors.
    /// If not already installed for current thread.
    /// Sanitizer makes larger stack usage and also it's incompatible with alternative stack by default (it sets up and relies on its own).
#if !defined(SANITIZER)
    if (!has_alt_stack)
    {
        /// Don't repeat tries even if not installed successfully.
        has_alt_stack = true;

        /// We have to call 'sigaltstack' before first 'sigaction'. (It does not work other way, for unknown reason).
        stack_t altstack_description{};
        altstack_description.ss_sp = alt_stack.getData();
        altstack_description.ss_flags = 0;
        altstack_description.ss_size = ThreadStack::getSize();

        if (0 != sigaltstack(&altstack_description, nullptr))
        {
            LOG_WARNING(log, "Cannot set alternative signal stack for thread, {}", errnoToString());
        }
        else
        {
            /// Obtain existing sigaction and modify it by adding a flag.
            struct sigaction action{};
            if (0 != sigaction(SIGSEGV, nullptr, &action))
            {
                LOG_WARNING(log, "Cannot obtain previous signal action to set alternative signal stack for thread, {}", errnoToString());
            }
            else if (!(action.sa_flags & SA_ONSTACK))
            {
                action.sa_flags |= SA_ONSTACK;

                if (0 != sigaction(SIGSEGV, &action, nullptr))
                {
                    LOG_WARNING(log, "Cannot set action with alternative signal stack for thread, {}", errnoToString());
                }
            }
        }
    }
#endif
}

ThreadGroupPtr ThreadStatus::getThreadGroup() const
{
    chassert(current_thread == this);
    return thread_group;
}

const String & ThreadStatus::getQueryId() const
{
    return query_id_from_query_context;
}

ContextPtr ThreadStatus::getQueryContext() const
{
    return query_context.lock();
}

ContextPtr ThreadStatus::getGlobalContext() const
{
    return global_context.lock();
}

void ThreadGroup::attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue, LogsLevel logs_level)
{
    std::lock_guard lock(mutex);
    shared_data.logs_queue_ptr = logs_queue;
    shared_data.client_logs_level = logs_level;
}

void ThreadStatus::attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue,
                                               LogsLevel logs_level)
{
    local_data.logs_queue_ptr = logs_queue;
    local_data.client_logs_level = logs_level;

    if (thread_group)
        thread_group->attachInternalTextLogsQueue(logs_queue, logs_level);
}

InternalTextLogsQueuePtr ThreadStatus::getInternalTextLogsQueue() const
{
    return local_data.logs_queue_ptr.lock();
}

InternalProfileEventsQueuePtr ThreadStatus::getInternalProfileEventsQueue() const
{
    return local_data.profile_queue_ptr.lock();
}

const String & ThreadStatus::getQueryForLog() const
{
    return local_data.query_for_logs;
}

LogsLevel ThreadStatus::getClientLogsLevel() const
{
    return local_data.client_logs_level;
}

void ThreadStatus::flushUntrackedMemory()
{
    if (untracked_memory == 0)
        return;

    memory_tracker.adjustWithUntrackedMemory(untracked_memory);
    untracked_memory = 0;
}

bool ThreadStatus::isQueryCanceled() const
{
    if (!thread_group)
        return false;

    if (local_data.query_is_canceled_predicate)
        return local_data.query_is_canceled_predicate();
    return false;
}

ThreadStatus::~ThreadStatus()
{
    flushUntrackedMemory();

    /// It may cause segfault if query_context was destroyed, but was not detached
    auto query_context_ptr = query_context.lock();
    assert((!query_context_ptr && getQueryId().empty()) || (query_context_ptr && getQueryId() == query_context_ptr->getCurrentQueryId()));

    /// detachGroup if it was attached
    if (deleter)
        deleter();

    chassert(!check_current_thread_on_destruction || current_thread == this);

    /// Only change current_thread if it's currently being used by this ThreadStatus
    /// For example, PushingToViews chain creates and deletes ThreadStatus instances while running in the main query thread
    if (current_thread == this)
        current_thread = nullptr;
    else if (check_current_thread_on_destruction)
        LOG_ERROR(log, "current_thread contains invalid address");
}

void ThreadStatus::updatePerformanceCounters()
{
    try
    {
        RUsageCounters::updateProfileEvents(*last_rusage, performance_counters);
        if (taskstats)
            taskstats->updateCounters(performance_counters);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void ThreadStatus::updatePerformanceCountersIfNeeded()
{
    if (last_rusage->thread_id == 0)
        return; // Performance counters are not initialized, so there is no need to update them

    constexpr UInt64 performance_counters_update_period_microseconds = 10 * 1000; // 10 milliseconds
    UInt64 total_elapsed_microseconds = stopwatch.elapsedMicroseconds();
    if (last_performance_counters_update_time + performance_counters_update_period_microseconds < total_elapsed_microseconds)
    {
        updatePerformanceCounters();
        last_performance_counters_update_time = total_elapsed_microseconds;
    }
}

void ThreadStatus::onFatalError()
{
    if (fatal_error_callback)
        fatal_error_callback();
}

ThreadStatus * MainThreadStatus::main_thread = nullptr;

MainThreadStatus & MainThreadStatus::getInstance()
{
    static MainThreadStatus thread_status;
    return thread_status;
}

MainThreadStatus::MainThreadStatus()
{
    main_thread = current_thread;
}

MainThreadStatus::~MainThreadStatus()
{
    main_thread = nullptr;
}

}
