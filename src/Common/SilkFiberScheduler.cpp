#include <Common/SilkFiberScheduler.h>

#if USE_SILK

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FiberLocal.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>

#if defined(SILK_THREAD_LOCAL_STORAGE_SANITIZER)
#    include <Common/SilkThreadLocalStorageSanitizer.h>
#endif

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/init.h>
#include <silk/util/perf.h>

#include <liburing.h>

#include <cerrno>
#include <functional>
#include <memory>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int IO_URING_INIT_FAILED;
}

namespace Silk
{

namespace
{

/// We need to formally guarantee that any Silk-initialization-related writes to memory
/// happen-before any reads that follow a isFiberSchedulerInitialized call that returns true.
/// Therefore, release-acquire is required.
std::atomic<bool> fiber_scheduler_initialized = false;

constinit FiberLocal<bool, FiberLocalSlot::INSIDE_SILK_FIBER> inside_silk_fiber;

constexpr uint8_t CLICKHOUSE_FIBER_CATEGORY = 1;

bool isClickHouseFiber() noexcept
{
    return silk::FiberScheduler::getCurrentFiberId().category == CLICKHOUSE_FIBER_CATEGORY;
}

struct FiberContext
{
    FiberLocalStorage::Holder fiber_local_storage;
    std::function<int()> task;

    static int main(FiberContext * self) noexcept
    {
#if defined(SILK_THREAD_LOCAL_STORAGE_SANITIZER)
        silk_thread_local_storage_sanitizer_fiber_init_hook();
#endif
        inside_silk_fiber = true;
        try
        {
            DB::ThreadStatus thread_status(DB::ThreadStatus::NoOSThreadTag{});
            return self->task();
        }
        catch (...)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
            return EIO;
        }
    }
};

void onFiberResume(silk::Fiber * fiber) noexcept
{
    if (!isClickHouseFiber())
        return;

    auto * context = static_cast<FiberContext *>(silk::FiberScheduler::getFiberParameters(fiber));
    FiberLocalStorage::swap(*context->fiber_local_storage);

    /// Nothing runs in a parked fiber, so what onFiberSuspend published must still be published.
    chassert(!DB::current_thread
        || DB::current_thread->untracked_memory.load() == DB::current_thread->per_cpu_untracked_memory.contributed);
}

void onFiberSuspend(silk::Fiber * fiber) noexcept
{
    if (!isClickHouseFiber())
        return;

    /// There can be a practically unbounded number of fibers.
    /// Each fiber gets a small buffer of untracked memory which it does not publish
    /// (see ServerSetting::per_cpu_untracked_memory_thread_buffer).
    /// So to prevent tens of gigabytes of untracked memory, fibers should publish
    /// that memory buffer to per-CPU counters at suspend.
    if (DB::current_thread)
        DB::current_thread->publishUntrackedMemory();

    auto * context = static_cast<FiberContext *>(silk::FiberScheduler::getFiberParameters(fiber));
    FiberLocalStorage::swap(*context->fiber_local_storage);
}

void onMemoryMapped(void * ptr, size_t size) noexcept
{
    DB::MemoryTrackerSwitcher switcher{&total_memory_tracker};
    auto trace = CurrentMemoryTracker::allocNoThrow(static_cast<Int64>(size));
    trace.onAlloc(ptr, size);
}

void onMemoryUnmapped(void * ptr, size_t size) noexcept
{
    DB::MemoryTrackerSwitcher switcher{&total_memory_tracker};
    auto trace = CurrentMemoryTracker::free(static_cast<Int64>(size));
    trace.onFree(ptr, size);
}

/// The silk scheduler asserts, and so aborts the process, when io_uring cannot be set up or lacks a
/// feature it relies on. Probe first, so that a server with the runtime enabled where io_uring is
/// unavailable (an old kernel, or a container whose seccomp profile blocks it, which the default
/// Docker profile does) refuses to start with an explanation instead of a core dump.
void checkIOUringIsUsable()
{
    io_uring ring{};
    io_uring_params params{};
    const int res = io_uring_queue_init_params(/* entries */ 8, &ring, &params);
    if (res < 0)
        DB::ErrnoException::throwWithErrno(
            DB::ErrorCodes::IO_URING_INIT_FAILED,
            -res,
            "Cannot initialize the silk fiber runtime: io_uring is not available. "
            "Check that the kernel supports io_uring and that seccomp does not block it, "
            "or disable the `enable_silk_runtime` server setting");
    SCOPE_EXIT({ io_uring_queue_exit(&ring); });

    /// Silk relies on NODROP (Linux 5.5) so that a full completion queue never loses a completion,
    /// and on CQE_SKIP (Linux 5.17) for its cross-ring wakeups.
    constexpr unsigned required_features = IORING_FEAT_NODROP | IORING_FEAT_CQE_SKIP;
    if ((params.features & required_features) != required_features)
        throw DB::Exception(
            DB::ErrorCodes::IO_URING_INIT_FAILED,
            "Cannot initialize the silk fiber runtime: the kernel's io_uring lacks features required by silk "
            "(IORING_FEAT_NODROP needs Linux 5.5, IORING_FEAT_CQE_SKIP needs Linux 5.17). "
            "Upgrade the kernel or disable the `enable_silk_runtime` server setting");
}

}

void initializeFiberScheduler(uint32_t fiber_stack_size)
{
    checkIOUringIsUsable();

    silk::initialize();

    const silk::FiberScheduler::Options options =
    {
        .fiberStackSize = fiber_stack_size,
        .fiberSuspend = &onFiberSuspend,
        .fiberResume = &onFiberResume,
        .accountMemoryMapped = &onMemoryMapped,
        .accountMemoryUnmapped = &onMemoryUnmapped,
    };
    silk::FiberScheduler::initialize(&options);

    fiber_scheduler_initialized.store(true, std::memory_order_release);
}

void destroyFiberScheduler()
{
    fiber_scheduler_initialized.store(false, std::memory_order_release);

    silk::FiberScheduler::destroy();
    silk::destroy();
}

/// TOCTOU is only possible if the function is run outside the global thread pool
/// because global thread pool shutdown explicitly preceeds Silk runtime destruction.
bool isFiberSchedulerInitialized()
{
    return fiber_scheduler_initialized.load(std::memory_order_acquire);
}

bool isInsideFiber()
{
    return inside_silk_fiber.get();
}

RuntimeCounters getRuntimeCounters()
{
    RuntimeCounters result;

    if (!isFiberSchedulerInitialized())
        return result;

    const uint32_t count = silk::Perf::getSimpleCounterCount();
    if (count == 0)
        return result;

    /// No TOCTOU is possible:
    /// 1. Registration is append-only and confined to initializeFiberScheduler.
    /// 2. Destruction presupposes quiesced readers as Silk runtime is explicitly destroyed after shutting down the global thread pool.
    /// 3. In between, the counter set is immutable.
    /// 4. In theory, if the current thread reads a stale counter count, getSimpleCounters respects the counterArraySize argument.
    auto accumulated = std::make_unique<silk::Perf::SimpleCounter[]>(count);
    const uint32_t written = silk::Perf::getSimpleCounters(0, accumulated.get(), count);

    result.reserve(written);
    for (uint32_t i = 0; i < written; ++i)
        result.emplace_back(silk::Perf::getSimpleCounterInfo(i).name, accumulated[i].value.load(std::memory_order_relaxed));

    return result;
}

/// 0 = success; ENOMEM = fiber allocation failed.
int spawn(std::function<int()> task, silk::FiberFuture & future)
{
    return silk::FiberScheduler::run(
        &FiberContext::main,
        FiberContext{ .fiber_local_storage = FiberLocalStorage::create(), .task = std::move(task) },
        CLICKHOUSE_FIBER_CATEGORY,
        &future);
}

int runBlocking(std::function<int()> task)
{
    silk::FiberFuture future;
    int r = spawn(std::move(task), future);
    return r ? r : future.wait();
}

}

#endif
