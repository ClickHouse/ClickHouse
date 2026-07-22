#include <Common/SilkScheduler.h>

#if USE_SILK

#include <silk/util/init.h>
#include <silk/fibers/fiber.h>

#include <atomic>

namespace DB
{

namespace
{
    std::atomic<bool> silk_scheduler_initialized{false};
}

void initializeSilkScheduler()
{
    silk::initialize();
    silk::FiberScheduler::Options options;
    /// OpenSSL handshakes and the AWS SDK run on fiber stacks and need more
    /// room than the silk default (matches gtest_silk_fiber_stream_socket).
    options.fiberStackSize = 320 * 1024;
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
