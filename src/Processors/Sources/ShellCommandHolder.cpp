#include <Processors/Sources/ShellCommandHolder.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>

namespace CurrentMetrics
{
    extern const Metric ExecutableUDFSharedMemoryPooledBytes;
    extern const Metric MemoryTrackingUnmeasured;
}

namespace DB
{

ShellCommandHolder::~ShellCommandHolder()
{
    /// The idle worker goes first, before the region and its charge: it holds a descriptor to
    /// the region, so the pages stay resident for as long as it lives, and a member is only
    /// destroyed after this body - which would drop the charge while the worker still holds
    /// the pages, for the whole wait `~ShellCommand` starts with. Its stdin is closed first,
    /// so that a worker written to exit on EOF does so at once rather than sitting out
    /// `command_termination_timeout` blocked on its next request. Past that budget a
    /// shared-memory worker's process group is sent `SIGKILL` and the worker reaped
    /// (`ShellCommand::Config::own_process_group`), so nothing in it can write into the region
    /// once the charge below is gone.
    if (returned_command)
    {
        /// `closeInputs` flushes and closes the pipes and throws on failure; this destructor
        /// must not, and it still has to release the process, the region and its charge.
        try
        {
            returned_command->closeInputs();
        }
        catch (...)
        {
            tryLogCurrentException("ShellCommandHolder");
        }
        returned_command.reset();
    }

    shared_memory.reset();

    if (persistent_memory_charge)
        unchargePersistentMemory(persistent_memory_charge);
}

std::unique_ptr<ShellCommand> ShellCommandHolder::buildCommand()
{
    if (returned_command)
        return std::move(returned_command);

    return func(inheritedRegionFds());
}

std::vector<std::pair<int, int>> ShellCommandHolder::inheritedRegionFds() const
{
    std::vector<std::pair<int, int>> fds;
    if (shared_memory)
        fds.emplace_back(SHARED_MEMORY_CHILD_FD, shared_memory->fd());
    return fds;
}

SharedMemoryRegionPtr ShellCommandHolder::getOrCreateSharedMemory(size_t size, bool & created)
{
    if (!shared_memory)
    {
        shared_memory = std::make_shared<SharedMemoryRegion>(size);
        created = true;
    }
    else
        created = false;

    return shared_memory;
}

SharedMemoryRegion * ShellCommandHolder::sharedMemoryRegionOverTheCap(size_t max_size) const
{
    if (!shared_memory)
        return nullptr;
    shared_memory->refreshFootprint();
    if (shared_memory->isOverTheCap(max_size))
        return shared_memory.get();
    return nullptr;
}

void ShellCommandHolder::discardWorkerAndRegion() noexcept
{
    /// Nobody waits for how a worker that is thrown away exits: it is signalled at once rather
    /// than given `command_termination_timeout`, which whoever drops it would sit out.
    if (returned_command)
        returned_command->discardWithoutGrace();
    returned_command.reset();
    shared_memory.reset();

    if (persistent_memory_charge)
        unchargePersistentMemory(persistent_memory_charge);

    /// The region the next borrow creates is a fresh, zero-filled file with nobody's data in
    /// it: there is no previous borrower to scrub it for, and a `memset` of a region that
    /// is already zero would be a wasted write of its whole size.
    last_borrower.reset();
}

void ShellCommandHolder::releaseChargeToBorrower()
{
    if (persistent_memory_charge)
        unchargePersistentMemory(persistent_memory_charge);
}

void ShellCommandHolder::acquireChargeFromBorrower(size_t bytes) noexcept
{
    /// A borrow that failed before it took the charge over (`releaseChargeToBorrower`) leaves
    /// the holder still charging the region, so the charge is brought to the new figure rather
    /// than added on top of what is there.
    if (bytes == persistent_memory_charge)
        return;

    if (bytes > persistent_memory_charge)
        chargePersistentMemory(bytes - persistent_memory_charge);
    else
        unchargePersistentMemory(persistent_memory_charge - bytes);
}

void ShellCommandHolder::chargePersistentMemory(size_t bytes) noexcept
{
    total_memory_tracker.adjustWithUntrackedMemory(static_cast<Int64>(bytes));
    persistent_memory_charge += bytes;
    CurrentMetrics::add(CurrentMetrics::MemoryTrackingUnmeasured, static_cast<Int64>(bytes));

    /// The same bytes, reported on their own. The server-wide tracker they were just added to
    /// carries everything else the server allocates as well, so it cannot answer how much of it
    /// is regions held by idle pooled workers - which is the part an administrator sizing
    /// `max_server_memory_usage` against a pool has to know, and the only part a test of this
    /// hand-over can assert exactly.
    CurrentMetrics::add(CurrentMetrics::ExecutableUDFSharedMemoryPooledBytes, static_cast<Int64>(bytes));
}

void ShellCommandHolder::unchargePersistentMemory(size_t bytes) noexcept
{
    total_memory_tracker.adjustWithUntrackedMemory(-static_cast<Int64>(bytes));
    persistent_memory_charge -= bytes;
    CurrentMetrics::sub(CurrentMetrics::MemoryTrackingUnmeasured, static_cast<Int64>(bytes));

    CurrentMetrics::sub(CurrentMetrics::ExecutableUDFSharedMemoryPooledBytes, static_cast<Int64>(bytes));
}

}
