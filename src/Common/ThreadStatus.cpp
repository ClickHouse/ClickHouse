#include <Common/Exception.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/QueryProfiler.h>
#include <Common/ThreadStatus.h>
#include <base/errnoToString.h>
#include <Interpreters/Context.h>

#include <Poco/Logger.h>
#include <base/getThreadId.h>
#include <base/getPageSize.h>

#include <csignal>
#include <mutex>
#include <sys/mman.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

thread_local ThreadStatus constinit * current_thread = nullptr;

#if !defined(SANITIZER)
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

    static size_t getSize() { return std::max<size_t>(16 << 10, MINSIGSTKSZ); }
    void * getData() const { return data; }

private:
    /// 16 KiB - not too big but enough to handle error.
    void * data;
};

}

static thread_local ThreadStack alt_stack;
static thread_local bool has_alt_stack = false;
#endif

ContextWeakPtr ThreadGroupStatus::getQueryContextWeak() const
{
    return query_context;
}

ContextWeakPtr ThreadGroupStatus::getGlobalContextWeak() const
{
    return global_context;
}

ThreadGroupStatus::FatalErrorCallback ThreadGroupStatus::getFatalErrorCallback() const
{
    return fatal_error_callback;
}

void ThreadGroupStatus::link(ThreadStatusPtr thread)
{
    std::lock_guard lock(mutex);

    /// NOTE: thread may be attached multiple times if it is reused from a thread pool.
    thread_ids.insert(thread->thread_id);
    threads.insert(thread);
}

void ThreadGroupStatus::unlink(ThreadStatusPtr thread)
{
    std::lock_guard guard(mutex);
    threads.erase(thread);
}

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
#if !defined(SANITIZER)
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

void ThreadStatus::flushUntrackedMemory()
{
    if (untracked_memory == 0)
        return;

    memory_tracker.adjustWithUntrackedMemory(untracked_memory);
    untracked_memory = 0;
}

ThreadStatus::~ThreadStatus()
{
    flushUntrackedMemory();

    /// It may cause segfault if query_context was destroyed, but was not detached
    auto query_context_ptr = query_context.lock();
    assert((!query_context_ptr && query_id.empty()) || (query_context_ptr && query_id == query_context_ptr->getCurrentQueryId()));

    /// detachGroup if it was attached
    if (deleter)
        deleter();

    /// Only change current_thread if it's currently being used by this ThreadStatus
    /// For example, PushingToViews chain creates and deletes ThreadStatus instances while running in the main query thread
    if (current_thread == this)
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

void ThreadStatus::assertState(ThreadState permitted_state, const char * description) const
{
    auto curr_state = thread_state.load();

    if (curr_state == permitted_state)
        return;

    if (description)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected thread state {}: {}", curr_state, description);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected thread state {}", curr_state);
}

void ThreadStatus::attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue,
                                               LogsLevel client_logs_level)
{
    logs_queue_ptr = logs_queue;
    chassert(thread_group);

    std::lock_guard lock(thread_group->mutex);
    thread_group->logs_queue_ptr = logs_queue;
    thread_group->client_logs_level = client_logs_level;
}

void ThreadStatus::attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & profile_queue)
{
    profile_queue_ptr = profile_queue;

    chassert(thread_group);

    std::lock_guard lock(thread_group->mutex);
    thread_group->profile_queue_ptr = profile_queue;
}

void ThreadStatus::onFatalError()
{
    /// No thread group - no callback
    if (!thread_group)
        return;

    std::lock_guard lock(thread_group->mutex);
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
