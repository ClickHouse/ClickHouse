#pragma once

#include <Common/SharedMemoryRegion.h>
#include <Common/ShellCommand.h>
#include <Core/UUID.h>
#include <Processors/Sources/ShellCommandSource.h>

#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace DB
{

/// The descriptor number under which the command's process inherits the shared-memory region.
/// Chosen above the three standard streams, and the shared-memory transport admits no extra pipes
/// (it takes exactly one input), so nothing else in the child is numbered here.
inline constexpr int SHARED_MEMORY_CHILD_FD = 3;

class ShellCommandHolder
{
public:
    /// Builds the process. It is given the descriptors the child has to inherit - the shared-memory
    /// region, as `{child_fd, parent_fd}` - which is why the region has to exist before the
    /// process does: a `memfd` has no name a process could open later, so the only way for the
    /// command to reach it is to have been started with it.
    using ShellCommandBuilderFunc = std::function<std::unique_ptr<ShellCommand>(const std::vector<std::pair<int, int>> & inherited_fds)>;

    explicit ShellCommandHolder(ShellCommandBuilderFunc && func_)
        : func(std::move(func_))
    {}

    ~ShellCommandHolder();

    /// Whether the next `buildCommand` hands back a process that has already served a borrow. A
    /// caller that has to distinguish "this worker may have left something on its pipes" from "this
    /// process was started a moment ago" needs to ask before building.
    bool hasReturnedCommand() const { return returned_command != nullptr; }

    /// Hands back the process that served the previous borrow, or starts a new one. A new one
    /// inherits the region this holder owns at that moment, so it has to have been created
    /// already (see `getOrCreateSharedMemory`); a returned one inherited it when it was started.
    std::unique_ptr<ShellCommand> buildCommand();

    /// The descriptors a process started now has to inherit: the region under child descriptor
    /// `SHARED_MEMORY_CHILD_FD`, which is also the number its request names it by.
    std::vector<std::pair<int, int>> inheritedRegionFds() const;

    void returnCommand(std::unique_ptr<ShellCommand> command) { returned_command = std::move(command); }

    /// The process that served the previous borrow, still held here, or null if the next
    /// `buildCommand` would start a fresh one. For the probes a borrow runs on a reused worker
    /// before it builds anything on it: they read its pipes, and what they decide - keep it, drop
    /// it, or drop it together with its region - has to be decided before the region is taken
    /// over, because taking it over is what a dropped worker's region must not survive.
    ShellCommand * returnedCommand() const { return returned_command.get(); }

    /// Who borrowed this worker last: the user, and the roles the query ran with.
    ///
    /// A pooled region is not cleared between borrows, and a pool serves the queries of every
    /// user: what one query left in the region, the command can read while serving the next.
    /// Over the pipes a command only ever saw what it was sent. The borrower's identity is what
    /// tells "the next query" from "a query of somebody else": a borrow by a different one scrubs
    /// the region (`scrubRegionForBorrower`), a borrow by the same one does not pay for it. The
    /// identity is the user's id and the current roles, not the user's name, for the reasons the
    /// query result cache keys its entries by the same pair (`QueryResultCache::Key`): a user
    /// created under the name of a dropped one is not the dropped one, and the roles of one user
    /// can be tied to different row policies, so what a query saw under one set of roles must not
    /// be readable by the command under another.
    struct BorrowerIdentity
    {
        std::optional<UUID> user_id;
        std::vector<UUID> current_roles;

        bool operator==(const BorrowerIdentity &) const = default;
    };

    const std::optional<BorrowerIdentity> & lastBorrower() const { return last_borrower; }
    void recordBorrower(BorrowerIdentity borrower) { last_borrower = std::move(borrower); }

    /// The shared-memory region for this process, created once and reused across pool borrows.
    ///
    /// Creating and growing the region does not charge any memory tracker here: while the holder is
    /// borrowed, the borrowing query owns the charge (see `releaseChargeToBorrower`).
    SharedMemoryRegionPtr getOrCreateSharedMemory(size_t size, bool & created);

    /// What the region costs - its footprint as `sharedMemoryRegionOverTheCap` last read it, or the
    /// source's own cap check on the same region - or zero if there is none. Lets the borrower charge
    /// its query memory tracker for the right number of bytes BEFORE the region is created or reused,
    /// because creating one commits its pages, and the server for what a pooled worker holds while
    /// it is idle. The footprint rather than the mapped size: a growth that reserved its pages but
    /// could not map them, a command that extended the file behind the server's back, or one that
    /// committed pages past its end, all leave the region costing more than the mapping shows, and
    /// those pages cost what any others do (`SharedMemoryRegion::refreshFootprint`). Read at every
    /// borrow and every hand-back, so that whatever the command added is charged from then on.
    size_t lastSeenSharedMemorySize() const { return shared_memory ? shared_memory->footprint() : 0; }

    /// The region if it is over the cap - re-read now (`SharedMemoryRegion::isOverTheCap`) - or
    /// null otherwise. What a borrow checks before it builds anything on the worker.
    SharedMemoryRegion * sharedMemoryRegionOverTheCap(size_t max_size) const;

    /// Drops the returned process and its region together, so that the next `buildCommand` starts
    /// a fresh process with a fresh region. A process and its region live and die together - the
    /// process reached it by inheriting its descriptor at `exec` - so there is no dropping
    /// one without the other.
    ///
    /// A borrow can discard the worker before it has taken the region's charge over
    /// (`releaseChargeToBorrower`), while the holder still charges it globally. The charge is
    /// dropped only after both are gone: the region stays resident until the process has been
    /// signalled and reaped, so it has to stay counted until then. Once the borrower has taken the charge over there is nothing left here
    /// to drop, and the borrower's own charge covers the region until it is gone.
    void discardWorkerAndRegion() noexcept;

    /// A region is charged to one memory tracker at a time, chosen by who can observe it:
    /// while the holder is borrowed, the borrowing query's tracker owns the charge, so the memory
    /// limit of that query still covers the region; while the holder sits idle in the process pool
    /// the region stays mapped with no query to charge, so the global tracker owns it instead.
    /// The charge is handed over in both directions rather than taken twice, because a query
    /// charge already propagates up into `total_memory_tracker` — charging both would count the
    /// same bytes twice there and let a handful of pooled workers exhaust
    /// `max_server_memory_usage` on paper. A charge cannot be moved between trackers atomically,
    /// so for the moment between releasing it on one side and taking it on the other the region
    /// is charged to neither (see `cleanup`): the global figure is low by the region's size, and
    /// by the sum of them when several hand-overs coincide. The invariant is about the states
    /// between hand-overs, not inside them.
    ///
    /// With `memory_worker_correct_memory_tracker` on (the default) `MemoryWorker` replaces the
    /// global tracker's value with a measurement on every tick, and no measurement it uses sees the
    /// region's pages (they are `shmem`). So every charge for a region, the query's and the
    /// holder's alike, is also counted in `MemoryTrackingUnmeasured`, which the worker adds to the
    /// measurement: the charge survives the correction and keeps counting towards
    /// `max_server_memory_usage`.
    ///
    /// Called by the borrower right before it charges its query for the region, after it has
    /// inspected the returned worker and discarded it if it had to: a discarded worker takes its
    /// charge with it (`discardWorkerAndRegion`), so the region stays counted while the process
    /// is being destroyed.
    void releaseChargeToBorrower();

    /// Restore the idle charge from a footprint measured and capped by the borrower before it
    /// released the query charge. The borrower discards the worker if that measurement fails.
    /// This changes accounting for existing memory and must not throw during cleanup.
    void acquireChargeFromBorrower(size_t bytes) noexcept;

private:
    /// Straight into the server-wide tracker, past this thread's: no query owns these bytes. And
    /// without the limit: the charge accounts for pages that are there already, so it cannot be
    /// refused - which is also what lets the hand-over be `noexcept`.
    void chargePersistentMemory(size_t bytes) noexcept;

    void unchargePersistentMemory(size_t bytes) noexcept;

    std::unique_ptr<ShellCommand> returned_command;
    ShellCommandBuilderFunc func;
    SharedMemoryRegionPtr shared_memory;
    std::optional<BorrowerIdentity> last_borrower;
    size_t persistent_memory_charge = 0;
};

/// A `ShellCommandHolder` borrowed from a `ProcessPool`, together with the pool it goes back to: it
/// owns the pool's slot for as long as the borrow lasts.
///
/// `BorrowedObjectPool` never decrements what it has allocated, so a borrowed holder that is
/// destroyed instead of being returned costs the pool one slot for good, and after `pool_size` of
/// such losses every call fails with "Could not get process from pool". Whatever path leaves this
/// object still owning the holder - an exception between the borrow and the hand-over to a source,
/// a member initializer of the source that throws, a teardown that throws before it gets to the
/// return - its destructor returns the holder to the pool. The normal path returns it explicitly
/// (`returnToPool`), at the point where the teardown has decided what goes back with it; the
/// destructor is the safety net for every other path, so the holder can be passed and stored like
/// any other movable value.
///
/// What must not outlive the slot has to be gone before the slot is returned: a worker that is not
/// going back with the holder has to die first, or the query waiting for the slot starts a
/// replacement while it is still alive (see `cleanup` of the sources). So an owner that holds such
/// a worker beside this object declares the worker after it, which destroys the worker first.
///
/// Empty - owning nothing - when default-constructed, once moved from, and after `returnToPool`.
/// A command that is not pooled has no borrowed holder, and its object stays empty.
class BorrowedShellCommandHolder
{
public:
    BorrowedShellCommandHolder() = default;

    BorrowedShellCommandHolder(ShellCommandHolderPtr holder_, std::shared_ptr<ProcessPool> pool_) noexcept
        : holder(std::move(holder_))
        , pool(std::move(pool_))
    {}

    BorrowedShellCommandHolder(BorrowedShellCommandHolder && other) noexcept
        : holder(std::move(other.holder))
        , pool(std::move(other.pool))
    {}

    /// Returns the holder this object owns, if any, before it takes over the other one.
    BorrowedShellCommandHolder & operator=(BorrowedShellCommandHolder && other) noexcept
    {
        if (this != &other)
        {
            returnToPool();
            holder = std::move(other.holder);
            pool = std::move(other.pool);
        }
        return *this;
    }

    BorrowedShellCommandHolder(const BorrowedShellCommandHolder &) = delete;
    BorrowedShellCommandHolder & operator=(const BorrowedShellCommandHolder &) = delete;

    ~BorrowedShellCommandHolder() { returnToPool(); }

    /// Hands the holder back to its pool, with the worker it holds at this moment; a no-op for an
    /// empty object. Never throws (`BorrowedObjectPool::returnObject`).
    ///
    /// A holder that goes back without a worker goes back without a region as well: a process and
    /// its region live and die together (`discardWorkerAndRegion`), and a region left behind would
    /// be handed - with the data of whoever used it last - to the fresh process the next borrow
    /// starts. On the normal paths the region is gone already, or the worker was handed back with
    /// it; this is for a teardown that did not get that far.
    void returnToPool() noexcept
    {
        if (holder)
        {
            if (!holder->hasReturnedCommand())
                holder->discardWorkerAndRegion();
            pool->returnObject(std::move(holder));
        }
        pool.reset();
    }

    ShellCommandHolder * get() const { return holder.get(); }
    ShellCommandHolder * operator->() const { return holder.get(); }
    ShellCommandHolder & operator*() const { return *holder; }
    explicit operator bool() const { return holder != nullptr; }

private:
    ShellCommandHolderPtr holder;
    std::shared_ptr<ProcessPool> pool;
};

}
