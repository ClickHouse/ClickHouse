#pragma once

/// Convenience header that provides ScopedThreadAttributes and getCurrentThreadGroup
/// without pulling in the full CurrentThread.h (which includes ThreadStatus.h).

#include <memory>
#include <boost/core/noncopyable.hpp>
#include <Common/setThreadName.h>

namespace DB
{

class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

class ThreadStatus;

/// Returns the thread group of the current thread, or nullptr if not attached.
/// This is equivalent to CurrentThread::getGroup() but avoids including CurrentThread.h.
ThreadGroupPtr getCurrentThreadGroup();

/**
 * RAII wrapper that switches the current thread's attribution for the duration of a scope:
 * the thread group (CurrentThread::attachToGroup/detachFromGroupIfNotDetached) and the
 * thread name (setThreadName). The two are independent: the name is switched and restored
 * even when there is no group to attach to.
 *
 * Typically used for inheriting thread group when scheduling tasks on a thread pool:
 *   pool->scheduleOrThrow([thread_group = getCurrentThreadGroup()]()
 *       {
 *           ScopedThreadAttributes scoped_attributes(thread_group, ThreadName::MY_THREAD);
 *           ...
 *       });
 */
class ScopedThreadAttributes : private boost::noncopyable
{
public:
    /// Name: if thread_name is not UNKNOWN, calls setThreadName regardless of the group logic
    /// below. The previous name is restored in the destructor, except after a successful attach
    /// from a detached thread: there the new name is intentionally left in place, because
    /// ThreadPoolImpl::worker reads it after the job returns to name the tracing span
    /// (and resets it on the next iteration). If the switch itself fails (this constructor is
    /// noexcept and only logs), the name is restored immediately rather than in the destructor,
    /// so the scope body is not attributed to a switch that never happened.
    /// Group: if thread_group_ is nullptr or equal to the current thread group, does nothing.
    /// allow_existing_group:
    ///  * If false, asserts that the thread is not already attached to a different group.
    ///    Use this when running a task in a thread pool.
    ///  * If true, remembers the current group and restores it in the destructor.
    ScopedThreadAttributes(ThreadGroupPtr thread_group_, ThreadName thread_name, bool allow_existing_group = false) noexcept;
    ~ScopedThreadAttributes();

private:
    /// Puts the pre-switch name back and clears should_restore_prev_thread_name, so it is safe to
    /// call from both the constructor's failure path and the destructor. A no-op if there is
    /// nothing to restore.
    void restorePrevThreadName() noexcept;

    ThreadStatus * prev_thread = nullptr;
    ThreadGroupPtr prev_thread_group;
    ThreadGroupPtr thread_group;
    /// The name before the rename, restored in the destructor. Not just a ThreadName: a thread
    /// that was never renamed carries the binary name (e.g. "clickhouse" for the server's main
    /// thread), which has no enum value and must be restored verbatim.
    ThreadNameSnapshot prev_thread_name;
    bool should_restore_prev_thread_name = false;
};


}
