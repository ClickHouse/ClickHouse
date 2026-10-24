#include <Common/SpeculativeMemoryReservation.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>

#include <memory>
#include <mutex>
#include <vector>


namespace DB
{

namespace
{

/// The per-thread slot holding the live reservations taken on that thread, at most one per
/// query: guards of the same query share one. A pooled thread can run guards of several queries
/// while suspended guards of earlier ones are still alive, so every live reservation stays
/// discoverable here until its last guard is destroyed - a query returning to the thread shares
/// its existing charge instead of taking another one. The list is tiny (usually one element).
/// A guard can be destroyed on another thread (a fiber resumed there, or executor destruction
/// on cancellation), so the slot is protected by a mutex and kept alive by a `shared_ptr`
/// for as long as any reservation taken through it exists, even after its thread exits.
struct Slot
{
    std::mutex mutex;
    std::vector<SpeculativeMemoryReservation::State *> states;
};

thread_local std::shared_ptr<Slot> current_slot;

}

struct SpeculativeMemoryReservation::State
{
    Int64 size = 0;
    MemoryTracker * credited_query_tracker = nullptr;
    /// Owns `credited_query_tracker` (a nested group keeps its parents alive) for as long as
    /// the charge exists: the last guard can be destroyed after its query has finished,
    /// e.g. a suspended fiber of a nested executor, and must still uncredit a live tracker.
    ThreadGroupPtr thread_group;
    std::shared_ptr<Slot> slot;
    /// Protected by `slot->mutex`.
    size_t references = 1;
};

SpeculativeMemoryReservation::SpeculativeMemoryReservation()
{
    if (!current_slot)
        current_slot = std::make_shared<Slot>();

    /// Share the thread's reservation only within the same query. A pooled thread can start
    /// a guard for another query while a suspended guard of the previous one is still alive;
    /// that guard must take its own charge, credited to its own query, instead of inheriting
    /// the credit and tracker of the previous query. The comparison is safe from pointer reuse
    /// because every listed state keeps the thread group owning its tracker alive.
    MemoryTracker * query_tracker = CurrentMemoryTracker::getQueryTrackerForGlobal();
    {
        std::lock_guard lock(current_slot->mutex);
        for (State * existing : current_slot->states)
        {
            if (existing->credited_query_tracker == query_tracker)
            {
                ++existing->references;
                state = existing;
                return;
            }
        }
        /// Reserve room up front, so the insertion after a successful charge cannot throw.
        current_slot->states.reserve(current_slot->states.size() + 1);
    }

    const Int64 requested = additional_memory_tracking_per_thread.load(std::memory_order_relaxed);
    if (requested <= 0)
        return;

    /// Allocate the state before taking the charge, so nothing can throw between a successful
    /// charge and the guard taking ownership of it.
    auto new_state = std::make_unique<State>();
    new_state->size = requested;
    new_state->credited_query_tracker = query_tracker;
    new_state->thread_group = CurrentThread::getGroup();
    new_state->slot = current_slot;

    /// May throw `MEMORY_LIMIT_EXCEEDED`. Nothing has been charged or installed yet, so a throw
    /// leaves no state to unwind.
    [[maybe_unused]] MemoryTracker * credited_query_tracker = CurrentMemoryTracker::allocGlobal(requested);
    chassert(credited_query_tracker == query_tracker);

    /// Only this thread inserts states into its own slot, so no state of this query has appeared
    /// since the lookup above, and the capacity reserved there is still available (other threads
    /// only erase elements).
    std::lock_guard lock(current_slot->mutex);
    state = new_state.release();
    current_slot->states.push_back(state);
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
        std::erase(state->slot->states, state);
    }

    std::unique_ptr<State> last_state(state);
    CurrentMemoryTracker::freeGlobal(last_state->size, last_state->credited_query_tracker);
}

Int64 SpeculativeMemoryReservation::getSize() const
{
    return state ? state->size : 0;
}

}
