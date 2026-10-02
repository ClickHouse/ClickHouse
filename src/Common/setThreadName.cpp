#include <string_view>
#include <unordered_map>
#include "config.h"

#include <pthread.h>

#if defined(OS_DARWIN) || defined(OS_SUNOS)
#elif defined(OS_FREEBSD)
#include <pthread_np.h>
#else
#include <sys/prctl.h>
#endif

#include <cstring>

#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/Jemalloc.h>
#include <Common/MemoryTracker.h>
#include <base/scope_guard.h>

#include <atomic>

constexpr size_t THREAD_NAME_SIZE = 16;


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PTHREAD_ERROR;
}

static_assert(sizeof(enum ThreadName) == 1, "ThreadName enum size is larger than 1 byte");

template<std::size_t N>
constexpr std::string_view as_string_view(char const (&s)[N])
{
    static_assert(N <= THREAD_NAME_SIZE, "Thread name too long");
    return std::string_view(s, N-1); // N-1 does not include null terminator
}

const static std::unordered_map<std::string_view, ThreadName> str_to_thread_name = []{
    std::unordered_map<std::string_view, ThreadName> result;

    #define THREAD_NAME_ACTION(NAME, STR) result[as_string_view(STR)] = ThreadName::NAME;
    THREAD_NAME_VALUES(THREAD_NAME_ACTION)
    #undef THREAD_NAME_ACTION

    return result;
}();

std::string_view toString(ThreadName name)
{
    switch (name)
    {
        case ThreadName::UNKNOWN: return as_string_view("Unknown");

        #define THREAD_NAME_ACTION(NAME, STR) case ThreadName::NAME: return as_string_view(STR);
        THREAD_NAME_VALUES(THREAD_NAME_ACTION)
        #undef THREAD_NAME_ACTION
    }
}

static ThreadName parseThreadName(const std::string_view & name)
{
    if (auto it = str_to_thread_name.find(name); it != str_to_thread_name.end())
        return it->second;
    else
        return ThreadName::UNKNOWN;
}

/// Cache thread_name to avoid prctl(PR_GET_NAME) for query_log/text_log
static thread_local ThreadName thread_name = ThreadName::UNKNOWN;

#if !defined(OS_DARWIN) && !defined(OS_SUNOS) && !defined(OS_FREEBSD)
/// `PR_SET_VMA_ANON_NAME` was introduced in Linux 5.17. On older kernels
/// `prctl` returns EINVAL and the `MemoryThreadStacks*` async metrics will
/// not populate. We only record the unsupported state silently here;
/// `ServerAsynchronousMetrics` (which knows whether the user actually opted
/// into heavy metrics) is responsible for emitting the log warning and
/// the `system.warnings` row. That avoids startup-log noise on default
/// servers running on old kernels — they never asked for these metrics.
#ifndef PR_SET_VMA
#define PR_SET_VMA 0x53564d41
#endif
#ifndef PR_SET_VMA_ANON_NAME
#define PR_SET_VMA_ANON_NAME 0
#endif

static std::atomic<bool> g_stack_vma_naming_unsupported{false};

static void nameCurrentThreadStackVMA() noexcept
{
    /// `pthread_getattr_np` may call `realloc` on the main thread to read
    /// `/proc/self/maps` for guard-size detection. The main thread sets its
    /// name at server startup, outside any `DENY_ALLOCATIONS_IN_SCOPE`. For
    /// pthread-created child threads the stack range is recorded at clone()
    /// time, so no allocation happens. The wrapper here covers the edge
    /// cases (debug builds, unusual contexts).
    ALLOW_ALLOCATIONS_IN_SCOPE;

    try
    {
        pthread_attr_t attr;
        if (pthread_getattr_np(pthread_self(), &attr) != 0)
            return;
        SCOPE_EXIT({ pthread_attr_destroy(&attr); });

        void * addr = nullptr;
        size_t size = 0;
        if (pthread_attr_getstack(&attr, &addr, &size) != 0 || addr == nullptr)
            return;

        if (prctl(PR_SET_VMA, PR_SET_VMA_ANON_NAME, addr, size, THREAD_STACK_VMA_NAME) == 0)
            return;

        /// Older kernels (< 5.17) return EINVAL. Flip the global flag so the
        /// metrics layer can surface this once via `system.warnings` and a
        /// single log line. Stay silent here.
        if (errno == EINVAL)
            g_stack_vma_naming_unsupported.store(true, std::memory_order_relaxed);
    }
    catch (...) /// NOLINT(bugprone-empty-catch) Ok, stack-naming is best-effort.
    {
    }
}
#endif

void setThreadName(ThreadName name)
{
#if !defined(OS_DARWIN) && !defined(OS_SUNOS) && !defined(OS_FREEBSD)
    /// First-call hook per thread: tag the stack VMA so `AsynchronousMetrics`
    /// can recognize it in `/proc/self/smaps`. Subsequent calls are skipped
    /// via TLS flag to avoid repeating a no-op syscall.
    thread_local bool stack_vma_named = false;
    if (!stack_vma_named)
    {
        stack_vma_named = true;
        nameCurrentThreadStackVMA();
    }
#endif

    // Skip rename on no-op.
    if (thread_name == name)
        return;

    auto thread_name_view = toString(name);
    if (thread_name_view.size() > THREAD_NAME_SIZE)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Thread name cannot be longer than 15 bytes");
    if (thread_name_view.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Thread name cannot be empty");

    // copy string view to string to avoid tidy build error:
    // `call may not be null terminated, provide size information to the callee to prevent potential issues`
    auto thread_name_str = std::string(thread_name_view);

#if defined(OS_FREEBSD)
    pthread_set_name_np(pthread_self(), thread_name_str.data());
    if ((false))
#elif defined(OS_DARWIN)
    if (0 != pthread_setname_np(thread_name_str.data()))
#elif defined(OS_SUNOS)
    // On illumos, pthread_setname_np is prohibitively expensive due to contention in procfs.
    // To avoid the performance penalty, skip os-level thread renaming, and fall back to the
    // cached thread-local name. Note: revert if illumos learns a fast path for thread renaming.
    if ((false))
#else
    if (0 != prctl(PR_SET_NAME, thread_name_str.data(), 0, 0, 0))
#endif
        if (errno != ENOSYS && errno != EPERM)    /// It's ok if the syscall is unsupported or not allowed in some environments.
            throw DB::ErrnoException(DB::ErrorCodes::PTHREAD_ERROR, "Cannot set thread name with prctl(PR_SET_NAME, ...)");

    thread_name = name;

#if USE_JEMALLOC
    DB::Jemalloc::setValue("thread.prof.name", thread_name_str.data());
#endif
}

ThreadName getThreadName()
{
    if (thread_name != ThreadName::UNKNOWN)
        return thread_name;

    char tmp_thread_name[THREAD_NAME_SIZE] = {};

#if defined(OS_DARWIN)
    if (pthread_getname_np(pthread_self(), tmp_thread_name, THREAD_NAME_SIZE))
        throw DB::Exception(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with pthread_getname_np()");
#elif defined(OS_SUNOS)
    // Skip os-level thread name lookup on illumos, since we skip thread renames in setThreadName.
#elif defined(OS_FREEBSD)
// TODO: make test. freebsd will have this function soon https://freshbsd.org/commit/freebsd/r337983
//    if (pthread_get_name_np(pthread_self(), thread_name, THREAD_NAME_SIZE))
//        throw DB::Exception(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with pthread_get_name_np()");
#else
    if (0 != prctl(PR_GET_NAME, tmp_thread_name, 0, 0, 0))
        if (errno != ENOSYS && errno != EPERM)    /// It's ok if the syscall is unsupported or not allowed in some environments.
            throw DB::ErrnoException(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with prctl(PR_GET_NAME)");
#endif

    return parseThreadName(std::string_view{tmp_thread_name});
}

bool isThreadStackVMANamingUnsupported() noexcept
{
#if !defined(OS_DARWIN) && !defined(OS_SUNOS) && !defined(OS_FREEBSD)
    return g_stack_vma_naming_unsupported.load(std::memory_order_relaxed);
#else
    /// This warning is specific to the Linux `PR_SET_VMA_ANON_NAME` path.
    /// On Darwin the `MemoryThreadStacks*` metrics are derived from the Mach
    /// VM map and have no kernel-version dependency; on other platforms the
    /// metrics do not exist. Either way there is nothing to warn about, so
    /// return `false` and a future caller without an OS_LINUX guard does not
    /// publish a spurious warning here.
    return false;
#endif
}

}
