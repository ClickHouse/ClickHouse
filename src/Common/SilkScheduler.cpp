#include <Common/SilkScheduler.h>

#if USE_SILK

#include <IO/SilkFiberJob.h>
#include <Common/CurrentThread.h>

#include <silk/util/init.h>
#include <Common/SilkSchedulerOptions.h>
#include <silk/fibers/fiber.h>

#include <atomic>
#include <utility>

namespace DB
{

namespace
{

std::atomic<bool> silk_scheduler_initialized{false};

/// Fires on every fiber switch across the whole process - both when a fiber is suspended
/// and when it is resumed, possibly on a different carrier OS thread each time - for every
/// fiber the server-wide Silk scheduler ever runs. A single swap of `DB::current_thread`
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
    std::swap(header->saved_current_thread, current_thread);
}

}

silk::FiberScheduler::Options makeServerSilkSchedulerOptions()
{
    silk::FiberScheduler::Options options;
    /// OpenSSL handshakes and the AWS SDK run on fiber stacks and need more
    /// room than the silk default (matches gtest_silk_fiber_stream_socket).
    options.fiberStackSize = 320 * 1024;
    /// Swap `DB::current_thread` in and out as a fiber migrates across carrier OS threads.
    /// Must be set before `initialize`.
    options.fiberResume = onFiberResumeSuspend;
    options.fiberSuspend = onFiberResumeSuspend;
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
