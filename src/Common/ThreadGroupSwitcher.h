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
 * RAII wrapper that switches the thread group and the thread name for the duration of a scope.
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
    /// thread_group_: nullptr or the current group means no group switch.
    /// thread_name: UNKNOWN means no rename.
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
    std::optional<ThreadNamePair> prev_thread_name;
};

}
