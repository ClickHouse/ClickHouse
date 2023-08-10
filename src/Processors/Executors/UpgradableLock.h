#pragma once
#include <atomic>
#include <cassert>
#include <list>
#include <mutex>
#include <condition_variable>

namespace DB
{

/// RWLock which allows to upgrade read lock to write lock.
/// Read locks should be fast if there is no write lock.
///
/// Newly created write lock waits for all active read locks.
/// Newly created read lock waits for all write locks. Starvation is possible.
///
/// Mutex must live longer than locks.
/// Read lock must live longer than corresponding  write lock.
///
/// For every write lock, a new internal state is created inside mutex.
/// This state is not deallocated until the destruction of mutex itself.
///
/// Usage example:
///
/// UpgradableMutex mutex;
/// {
///     UpgradableMutex::ReadLock read_lock(mutex);
///     ...
///     {
///         UpgradableMutex::WriteLock write_lock(read_lock);
///         ...
///     }
///     ...
/// }
class UpgradableMutex
{
private:
    /// Implementation idea
    ///
    /// ----------- (read scope)
    /// ++num_readers
    /// ** wait for active writer (in loop, starvation is possible here) **
    ///
    /// =========== (write scope)
    /// ** create new State **
    /// ** wait for active writer (in loop, starvation is possible here) **
    /// ** wait for all active readers **
    ///
    /// ** notify all waiting readers for the current state.
    /// =========== (end write scope)
    ///
    /// --num_readers
    /// ** notify current active writer **
    /// ----------- (end read scope)
    struct State
    {
        size_t num_waiting = 0;
        bool is_done = false;

        std::mutex mutex;
        std::condition_variable read_condvar;
        std::condition_variable write_condvar;

        void wait() noexcept
        {
            std::unique_lock lock(mutex);
            ++num_waiting;
            write_condvar.notify_one();
            while (!is_done)
                read_condvar.wait(lock);
        }

        void lock(std::atomic_size_t & num_readers_) noexcept
        {
            /// Note : num_locked is an atomic
            /// which can change it's value without locked mutex.
            /// We support an invariant that after changing num_locked value,
            /// UpgradableMutex::write_state is checked, and in case of active
            /// write lock, we always notify it's write condvar.
            std::unique_lock lock(mutex);
            ++num_waiting;
            while (num_waiting < num_readers_.load())
                write_condvar.wait(lock);
        }

        void unlock() noexcept
        {
            {
                std::unique_lock lock(mutex);
                is_done = true;
            }
            read_condvar.notify_all();
        }
    };

    std::atomic_size_t num_readers = 0;

    std::list<State> states;
    std::mutex states_mutex;
    std::atomic<State *> write_state{nullptr};

    void lock() noexcept
    {
        ++num_readers;
        while (auto * state = write_state.load())
            state->wait();
    }

    void unlock() noexcept
    {
        --num_readers;
        while (auto * state = write_state.load())
            state->write_condvar.notify_one();
    }

    State * allocState()
    {
        std::lock_guard guard(states_mutex);
        return &states.emplace_back();
    }

    void upgrade(State & state) noexcept
    {
        State * expected = nullptr;

        /// Only change nullptr -> state is possible.
        while (!write_state.compare_exchange_strong(expected, &state))
        {
            expected->wait();
            expected = nullptr;
        }

        state.lock(num_readers);
    }

    void degrade(State & state) noexcept
    {
        State * my = write_state.exchange(nullptr);
        if (&state != my)
            std::terminate();
        state.unlock();
    }

public:
    class ReadGuard
    {
    public:
        explicit ReadGuard(UpgradableMutex & lock_) : lock(lock_) { lock.lock(); }
        ~ReadGuard() { lock.unlock(); }

        UpgradableMutex & lock;
    };

    class WriteGuard
    {
    public:
        explicit WriteGuard(ReadGuard & read_guard_) : read_guard(read_guard_)
        {
            state = read_guard.lock.allocState();
            read_guard.lock.upgrade(*state);
        }

        ~WriteGuard()
        {
            if (state)
                read_guard.lock.degrade(*state);
        }

    private:
        ReadGuard & read_guard;
        State * state = nullptr;
    };
};

}
