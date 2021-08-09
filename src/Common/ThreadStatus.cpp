#include <Common/Exception.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/QueryProfiler.h>
#include <Common/ThreadStatus.h>
#include <common/errnoToString.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <Poco/Logger.h>
#include <common/getThreadId.h>
#include <common/getPageSize.h>

#include <csignal>


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


thread_local ThreadStatus * current_thread = nullptr;
thread_local ThreadStatus * main_thread = nullptr;

#if !defined(SANITIZER) && !defined(ARCADIA_BUILD)
namespace
{

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
        : data(aligned_alloc(getPageSize(), size))
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

    static size_t getSize() { return size; }
    void * getData() const { return data; }

private:
    /// 16 KiB - not too big but enough to handle error.
    static constexpr size_t size = std::max<size_t>(16 << 10, MINSIGSTKSZ);
    void * data;
};

}

static thread_local ThreadStack alt_stack;
static thread_local bool has_alt_stack = false;
#endif


ThreadStatus::ThreadStatus()
    : thread_id{getThreadId()}
{
    last_rusage = std::make_unique<RUsageCounters>();

    memory_tracker.setDescription("(for thread)");
    log = &Poco::Logger::get("ThreadStatus");

    current_thread = this;

    /// NOTE: It is important not to do any non-trivial actions (like updating ProfileEvents or logging) before ThreadStatus is created
    /// Otherwise it could lead to SIGSEGV due to current_thread dereferencing

    /// Will set alternative signal stack to provide diagnostics for stack overflow errors.
    /// If not already installed for current thread.
    /// Sanitizer makes larger stack usage and also it's incompatible with alternative stack by default (it sets up and relies on its own).
#if !defined(SANITIZER) && !defined(ARCADIA_BUILD)
    if (!has_alt_stack)
    {
        /// Don't repeat tries even if not installed successfully.
        has_alt_stack = true;

        /// We have to call 'sigaltstack' before first 'sigaction'. (It does not work other way, for unknown reason).
        stack_t altstack_description{};
        altstack_description.ss_sp = alt_stack.getData();
        altstack_description.ss_flags = 0;
        altstack_description.ss_size = alt_stack.getSize();

        if (0 != sigaltstack(&altstack_description, nullptr))
        {
            LOG_WARNING(log, "Cannot set alternative signal stack for thread, {}", errnoToString(errno));
        }
        else
        {
            /// Obtain existing sigaction and modify it by adding a flag.
            struct sigaction action{};
            if (0 != sigaction(SIGSEGV, nullptr, &action))
            {
                LOG_WARNING(log, "Cannot obtain previous signal action to set alternative signal stack for thread, {}", errnoToString(errno));
            }
            else if (!(action.sa_flags & SA_ONSTACK))
            {
                action.sa_flags |= SA_ONSTACK;

                if (0 != sigaction(SIGSEGV, &action, nullptr))
                {
                    LOG_WARNING(log, "Cannot set action with alternative signal stack for thread, {}", errnoToString(errno));
                }
            }
        }
    }
#endif
}

ThreadStatus::~ThreadStatus()
{
    try
    {
        if (untracked_memory > 0)
            memory_tracker.alloc(untracked_memory);
        else
            memory_tracker.free(-untracked_memory);
    }
    catch (const DB::Exception &)
    {
        /// It's a minor tracked memory leak here (not the memory itself but it's counter).
        /// We've already allocated a little bit more than the limit and cannot track it in the thread memory tracker or its parent.
    }

#if !defined(ARCADIA_BUILD)
    /// It may cause segfault if query_context was destroyed, but was not detached
    auto query_context_ptr = query_context.lock();
    assert((!query_context_ptr && query_id.empty()) || (query_context_ptr && query_id == query_context_ptr->getCurrentQueryId()));
#endif

    if (deleter)
        deleter();
    current_thread = nullptr;
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

void ThreadStatus::assertState(const std::initializer_list<int> & permitted_states, const char * description) const
{
    for (auto permitted_state : permitted_states)
    {
        if (getCurrentState() == permitted_state)
            return;
    }

    if (description)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected thread state {}: {}", getCurrentState(), description);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected thread state {}", getCurrentState());
}

void ThreadStatus::attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue,
                                               LogsLevel client_logs_level)
{
    logs_queue_ptr = logs_queue;

    if (!thread_group)
        return;

    std::lock_guard lock(thread_group->mutex);
    thread_group->logs_queue_ptr = logs_queue;
    thread_group->client_logs_level = client_logs_level;
}

void ThreadStatus::setFatalErrorCallback(std::function<void()> callback)
{
    fatal_error_callback = std::move(callback);

    if (!thread_group)
        return;

    std::lock_guard lock(thread_group->mutex);
    thread_group->fatal_error_callback = fatal_error_callback;
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
    : ThreadStatus()
{
    main_thread = current_thread;
}
MainThreadStatus::~MainThreadStatus()
{
    main_thread = nullptr;
}

}
