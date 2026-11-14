#pragma once

/// Convenience header that provides ThreadGroupSwitcher and getCurrentThreadGroup
/// without pulling in the full CurrentThread.h (which includes ThreadStatus.h).

#include <memory>
#include <optional>
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
 * RAII wrapper that switches the thread name (setThreadName) and the thread group
 * (CurrentThread::attachToGroup/detachFromGroupIfNotDetached) for the duration of a scope.
 *
 * Typically used for inheriting thread group when scheduling tasks on a thread pool:
 *   pool->scheduleOrThrow([thread_group = CurrentThread::getGroup()]()
 *       {
 *           ThreadGroupSwitcher switcher(thread_group, ThreadName::MY_THREAD);
 *           ...
 *       });
 */
class ThreadGroupSwitcher : private boost::noncopyable
{
public:
    /// Name: if thread_name is not UNKNOWN, the thread is renamed for the scope and the previous name is
    /// restored in the destructor. Except when the thread was detached and the attach succeeded: then
    /// the thread is a pool worker running a job, and the name is left as is, since after the job the
    /// worker uses it as the name of the tracing span (and renames the thread back before the next job).
    ///
    /// Group: if thread_group_ is nullptr or equal to the current thread group, does nothing.
    ///
    /// allow_existing_group:
    ///  * If false, asserts that the thread is not already attached to a different group.
    ///    Use this when running a task in a thread pool.
    ///  * If true, remembers the current group and restores it in the destructor.
    ThreadGroupSwitcher(ThreadGroupPtr thread_group_, ThreadName thread_name, bool allow_existing_group = false) noexcept;
    ~ThreadGroupSwitcher();

private:
    void restorePrevThreadName() noexcept;

    ThreadStatus * prev_thread = nullptr;
    ThreadGroupPtr prev_thread_group;
    ThreadGroupPtr thread_group;
    /// Empty once restored or when the name is to be kept.
    std::optional<ThreadNamePair> prev_thread_name;
};

}
