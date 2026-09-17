#include <Common/SilkScheduler.h>

#include <atomic>

namespace DB
{

namespace
{
    std::atomic<bool> silk_configured_but_not_started{false};
}

bool isSilkConfiguredButNotStarted()
{
    return silk_configured_but_not_started.load();
}

void setSilkConfiguredButNotStarted(bool value)
{
    silk_configured_but_not_started.store(value);
}

}

#if USE_SILK

#include <IO/SilkFiberJob.h>
#include <Common/CurrentThread.h>
#include <Common/ErrnoException.h>

#include <Common/SilkFiberScheduler.h>
#include <silk/fibers/fiber.h>

#include <sys/syscall.h>
#include <unistd.h>

#include <utility>

/// Neither the x86_64 nor the aarch64 glibc sysroots we build the Silk targets with ship
/// `<linux/io_uring.h>` (its syscall numbers postdate those sysroots' frozen kernel headers), so
/// `__NR_io_uring_setup` isn't defined either. Silk only supports these two architectures (see
/// contrib/silk-cmake/CMakeLists.txt), and the syscall was assigned the same number on both
/// (io_uring's syscalls are part of Linux's "generic" table, unified across architectures since
/// their introduction). A constexpr rather than a fallback `#define`: defining a
/// double-underscore macro trips `-Wreserved-macro-identifier` under `-Weverything -Werror`.
#if defined(__NR_io_uring_setup)
constexpr Int64 NR_IO_URING_SETUP = __NR_io_uring_setup;
#elif defined(__x86_64__) || defined(__aarch64__)
constexpr Int64 NR_IO_URING_SETUP = 425;
#else
    #error "Unsupported architecture for the io_uring probe"
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int IO_URING_INIT_FAILED;
}

namespace
{

/// Probes that `io_uring_setup(2)` actually works before starting the scheduler, so a container
/// with io_uring blocked (seccomp profile, or `kernel.io_uring_disabled`) fails server startup
/// loudly instead of leaving every disk-group connection to hang or silently misbehave later.
/// Raw syscall rather than `silk`/liburing: this TU intentionally does not link liburing (its
/// symbols aren't visible here), and - as noted above - we can't even name the real
/// `struct io_uring_params` for lack of the header. A zeroed, generously oversized scratch buffer
/// is safe to pass either way: the real ABI-frozen struct is 120 bytes and has not grown since
/// `io_uring`'s introduction, and we never read the buffer back, only whether the call succeeds.
/// This only proves the syscall is reachable; it does not exercise submission, polling, or any
/// other ring behaviour.
void probeIoUringAvailable()
{
    alignas(8) unsigned char io_uring_setup_params[128] = {};
    int fd = static_cast<int>(syscall(NR_IO_URING_SETUP, /*entries*/ 4, io_uring_setup_params));
    if (fd < 0)
        throw ErrnoException(ErrorCodes::IO_URING_INIT_FAILED,
            "Cannot start the Silk fiber scheduler: io_uring_setup failed - io_uring is unavailable "
            "(commonly blocked by container seccomp profiles or kernel.io_uring_disabled); unset "
            "disk_connections_use_silk or enable io_uring");
    [[maybe_unused]] int rc = close(fd);
}

std::atomic<bool> silk_scheduler_initialized{false};

/// Fires on every fiber switch - both when a fiber is suspended and when it is resumed,
/// possibly on a different carrier OS thread each time - for every fiber of the
/// `SilkFiberCategory::FETCH` category (the scheduler-wide hooks installed by
/// `Silk::initializeFiberScheduler` dispatch on the category and forward FETCH fibers
/// here; see `Silk::setFiberHooksForCategory`). A single swap of `DB::current_thread`
/// (the borrowing OS thread's own thread-local pointer) with the header's parked slot serves
/// as both directions: swap is its own inverse, so the same call both saves the carrier's
/// `current_thread` and installs the fiber's own (on suspend) and restores the carrier's
/// `current_thread` while re-parking the fiber's own (on resume) - whichever of the two this
/// particular call happens to be. The fiber's own `ThreadStatus` is created and destroyed by
/// the spawn site (see FiberFetchMachineRunner.cpp), never by this hook; the hook only ever
/// swaps a pointer. The blind cast to `SilkFiberJobHeader` is only safe because of the
/// convention documented on that struct: this is the only `FiberScheduler` instance in the
/// server, and every spawn site places the header first in its parameters.
void onFiberResumeSuspend(silk::Fiber * fiber) noexcept
{
    auto * header = static_cast<SilkFiberJobHeader *>(silk::FiberScheduler::getFiberParameters(fiber));
    /// `current_thread` is a FiberLocal, so the swap goes through its load/store; FETCH
    /// fibers keep sharing the carrier's FiberLocalStorage for the other slots (the same
    /// sharing they had when those variables were plain TLS).
    ThreadStatus * fiber_current_thread = header->saved_current_thread;
    header->saved_current_thread = current_thread;
    current_thread = fiber_current_thread;
}

}

void registerReaderExecutorFiberHooks()
{
    /// Swap `DB::current_thread` in and out as a FETCH fiber migrates across carrier OS
    /// threads. Swap is its own inverse, so the same function serves as both hooks.
    Silk::setFiberHooksForCategory(SilkFiberCategory::FETCH, onFiberResumeSuspend, onFiberResumeSuspend);
}

void initializeSilkScheduler()
{
    probeIoUringAvailable();
    registerReaderExecutorFiberHooks();
    /// The process supports a single `silk::FiberScheduler`; it may already have been
    /// started by the `enable_silk_runtime` startup path, in which case the fibers of both
    /// kinds share it (the switch hooks dispatch on the fiber category).
    if (!Silk::isFiberSchedulerInitialized())
        Silk::initializeFiberScheduler(Silk::DEFAULT_FIBER_STACK_SIZE);
    silk_scheduler_initialized.store(true);
}

bool isSilkSchedulerInitialized()
{
    return silk_scheduler_initialized.load();
}

uint64_t currentSilkFiberId()
{
    /// `FiberId` is a packed bitfield union over a `uint64_t`; `.raw` is the whole thing.
    /// Zero-initialized (all-zero `raw`) off-fiber, and the per-CPU counter that feeds it is
    /// seeded at 1 (see fiber.cpp), so a real fiber's id never collides with that sentinel.
    return silk::FiberScheduler::getCurrentFiberId().raw;
}

}

#else

namespace DB
{

void initializeSilkScheduler()
{
}

void registerReaderExecutorFiberHooks()
{
}

bool isSilkSchedulerInitialized()
{
    return false;
}

uint64_t currentSilkFiberId()
{
    return 0;
}

}

#endif
