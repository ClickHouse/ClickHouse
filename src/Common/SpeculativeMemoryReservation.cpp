#include <Common/SpeculativeMemoryReservation.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>

#include <memory>
#include <mutex>


namespace DB
{

namespace
{

/// The per-thread slot holding the reservation shared by the guards created on that thread.
/// A guard can be destroyed on another thread (a fiber resumed there, or executor destruction
/// on cancellation), so the slot is protected by a mutex and kept alive by a `shared_ptr`
/// for as long as any reservation taken through it exists, even after its thread exits.
struct Slot
{
    std::mutex mutex;
    SpeculativeMemoryReservation::State * state = nullptr;
};

thread_local std::shared_ptr<Slot> current_slot;

}

struct SpeculativeMemoryReservation::State
{
    Int64 size = 0;
    MemoryTracker * credited_query_tracker = nullptr;
    std::shared_ptr<Slot> slot;
    /// Protected by `slot->mutex`.
    size_t references = 1;
};

SpeculativeMemoryReservation::SpeculativeMemoryReservation()
{
    if (!current_slot)
        current_slot = std::make_shared<Slot>();

    {
        std::lock_guard lock(current_slot->mutex);
        if (current_slot->state)
        {
            ++current_slot->state->references;
            state = current_slot->state;
            return;
        }
    }

    const Int64 requested = additional_memory_tracking_per_thread.load(std::memory_order_relaxed);
    if (requested <= 0)
        return;

    /// Only this thread installs a state into its own slot, and other threads touch the slot
    /// only through a state installed there, so the slot stays empty while the lock is not held.
    /// May throw `MEMORY_LIMIT_EXCEEDED`. Nothing has been modified yet, so a throw leaves no
    /// state to unwind.
    MemoryTracker * credited_query_tracker = CurrentMemoryTracker::allocGlobal(requested);

    auto new_state = std::make_unique<State>();
    new_state->size = requested;
    new_state->credited_query_tracker = credited_query_tracker;
    new_state->slot = current_slot;

    std::lock_guard lock(current_slot->mutex);
    state = new_state.release();
    current_slot->state = state;
}

SpeculativeMemoryReservation::~SpeculativeMemoryReservation()
{
    if (!state)
        return;

    CurrentThread::flushUntrackedMemory();

    {
        std::lock_guard lock(state->slot->mutex);
        if (--state->references > 0)
            return;
        state->slot->state = nullptr;
    }

    std::unique_ptr<State> last_state(state);
    CurrentMemoryTracker::freeGlobal(last_state->size, last_state->credited_query_tracker);
}

Int64 SpeculativeMemoryReservation::getSize() const
{
    return state ? state->size : 0;
}

}
