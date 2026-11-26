#pragma once

/// Convenience header that provides ThreadGroupSwitcher and getCurrentThreadGroup
/// without pulling in the full CurrentThread.h (which includes ThreadStatus.h).

#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <boost/core/noncopyable.hpp>
#include <Common/ServerOwnedMemory.h>
#include <Common/setThreadName.h>

namespace DB
{

class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

class ThreadStatus;

/// Returns the thread group of the current thread, or nullptr if not attached.
/// This is equivalent to CurrentThread::getGroup() but avoids including CurrentThread.h.
ThreadGroupPtr getCurrentThreadGroup();

/// Whether the current thread has a `ThreadStatus`, so that it can attach to a group.
bool hasThreadStatus();

/**
 * RAII wrapper around CurrentThread::attachToGroup/detachFromGroupIfNotDetached.
 *
 * To run a pool job for the query that schedules it, prefer `attachedJob`.
 */
class ThreadGroupSwitcher : private boost::noncopyable
{
public:
    /// If thread_group_ is nullptr or equal to current thread group, does nothing.
    /// allow_existing_group:
    ///  * If false, asserts that the thread is not already attached to a different group.
    ///    Use this when running a task in a thread pool.
    ///  * If true, remembers the current group and thread name and restores them in destructor.
    /// If thread_name is not empty, calls setThreadName along the way; should be at most 15 bytes long.
    ThreadGroupSwitcher(ThreadGroupPtr thread_group_, ThreadName thread_name, bool allow_existing_group = false) noexcept;
    ~ThreadGroupSwitcher();

private:
    ThreadStatus * prev_thread = nullptr;
    ThreadGroupPtr prev_thread_group;
    ThreadGroupPtr thread_group;
    /// Name of a borrowed thread (allow_existing_group=true), saved before renaming and restored in the
    /// destructor. A separate bool gates the restore because UNKNOWN is a valid saved name, not a sentinel.
    ThreadName prev_thread_name = ThreadName::UNKNOWN;
    bool should_restore_prev_thread_name = false;
};


/// A pool job that runs `f` attached to the thread group of the scheduling thread, like
///   pool->scheduleOrThrow(attachedJob(ThreadName::X, [captures] { ... }));
/// `f` is destroyed attached as well, so what it captured is released by the query it belongs to, also when the job is
/// dropped without running. The job and its state are the server's, so they cost no query memory themselves.
template <typename F>
auto attachedJob(ThreadName thread_name, F && f)
{
    struct State
    {
        ThreadGroupPtr thread_group;
        std::optional<std::decay_t<F>> f;
        ThreadName thread_name{};

        ~State()
        {
            /// Not run: dropped by the pool, or after a failed schedule on the scheduling thread, where this does nothing.
            /// A thread without a `ThreadStatus` cannot attach, and releases it as it is.
            if (!f || !hasThreadStatus())
                return;
            ThreadGroupSwitcher switcher(std::move(thread_group), thread_name, /*allow_existing_group=*/ true);
            f.reset();
        }
    };
    auto job_state = std::allocate_shared<State>(ServerOwnedAllocator<State>{}, getCurrentThreadGroup(), std::forward<F>(f), thread_name);
    return makeServerOwnedJob([state = std::move(job_state)]
    {
        ThreadGroupSwitcher switcher(std::move(state->thread_group), state->thread_name);
        auto local = std::move(*state->f);
        state->f.reset();
        local();
    });
}

}
