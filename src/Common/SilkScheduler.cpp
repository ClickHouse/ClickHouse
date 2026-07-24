#include <Common/SilkScheduler.h>

#if USE_SILK

#include <IO/SilkFiberJob.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

#include <silk/util/init.h>
#include <Common/SilkSchedulerOptions.h>
#include <silk/fibers/fiber.h>

#include <atomic>

namespace DB
{

namespace
{

std::atomic<bool> silk_scheduler_initialized{false};

/// Scheduler threads are raw pthreads outside ClickHouse's thread-pool machinery,
/// so they never get a `ThreadStatus` the usual way; create one lazily, once per
/// thread, the first time it borrows any fiber at all.
/// The scheduler is never destroyed (see SilkScheduler.h), so this thread_local
/// lives for the lifetime of the process and is never torn down while attached.
thread_local std::unique_ptr<ThreadStatus> silk_worker_thread_status;

/// These fire on every fiber switch across the whole process, for every fiber
/// the server-wide Silk scheduler ever runs. The blind cast to `SilkFiberJobHeader`
/// is only safe because of the convention documented on that struct: this is the
/// only `FiberScheduler` instance in the server, and every spawn site places the
/// header first in its parameters. Hooks fire balanced (one resume per one suspend
/// on the same borrowing OS thread) and never for proxy fibers, so attach/detach
/// here always pair up.
void silkFiberResumeHook(silk::Fiber * fiber) noexcept
{
    if (!current_thread)
    {
        try
        {
            silk_worker_thread_status = std::make_unique<ThreadStatus>();
        }
        catch (...)
        {
            /// `ThreadStatus::ThreadStatus()` publishes `current_thread = this` before
            /// the alt-stack setup that can still throw (e.g. `ErrnoException` from a
            /// failed `aligned_alloc`, see `ThreadStack`). If that throw happens, our
            /// destructor never runs, so `current_thread` is left dangling at a `this`
            /// that `make_unique` is about to free — every later allocation on this OS
            /// thread would dereference freed memory, forever, because the
            /// `!current_thread` guard above would never see it as null again. Reset it
            /// explicitly to restore the invariant so the next resume retries, and skip
            /// attaching this time.
            current_thread = nullptr;
            return;
        }
    }

    try
    {
        const auto * header = static_cast<const SilkFiberJobHeader *>(silk::FiberScheduler::getFiberParameters(fiber));
        if (header->thread_group)
        {
            /// Hooks must pair up (see the comment above); if a pairing regression ever
            /// attaches without a matching detach, `attachToGroupIfDetached` would
            /// silently no-op onto the wrong group instead of failing loudly.
            chassert(!CurrentThread::getGroup() || CurrentThread::getGroup() == header->thread_group);
            CurrentThread::attachToGroupIfDetached(header->thread_group);
        }
    }
    catch (...) /// Ok
    {
        /// Attribution is best-effort on the fiber-switch path: never let it take
        /// down the scheduler thread (the hook is noexcept, and even logging can
        /// throw here).
    }
}

void silkFiberSuspendHook(silk::Fiber * fiber) noexcept
{
    try
    {
        const auto * header = static_cast<const SilkFiberJobHeader *>(silk::FiberScheduler::getFiberParameters(fiber));
        if (header->thread_group)
            CurrentThread::detachFromGroupIfNotDetached();
    }
    catch (...) /// Ok
    {
        /// Same as the resume hook: best-effort attribution on a noexcept
        /// fiber-switch path - never let it take down the scheduler thread.
    }
}

}

silk::FiberScheduler::Options makeServerSilkSchedulerOptions()
{
    silk::FiberScheduler::Options options;
    /// OpenSSL handshakes and the AWS SDK run on fiber stacks and need more
    /// room than the silk default (matches gtest_silk_fiber_stream_socket).
    options.fiberStackSize = 320 * 1024;
    /// Attach/detach the submitter's ThreadGroup on the OS thread a fiber
    /// borrows, so memory accounting and per-user throttling follow the fiber
    /// across scheduler threads. Must be set before `initialize`.
    options.fiberResume = silkFiberResumeHook;
    options.fiberSuspend = silkFiberSuspendHook;
    return options;
}

void initializeSilkScheduler()
{
    silk::initialize();
    silk::FiberScheduler::Options options = makeServerSilkSchedulerOptions();
    silk::FiberScheduler::initialize(&options);
    silk_scheduler_initialized.store(true);
}

bool isSilkSchedulerInitialized()
{
    return silk_scheduler_initialized.load();
}

}

#else

namespace DB
{

void initializeSilkScheduler()
{
}

bool isSilkSchedulerInitialized()
{
    return false;
}

}

#endif
