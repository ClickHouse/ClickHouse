#pragma once

/// Convenience header that provides ThreadGroupSwitcher and getCurrentThreadGroup
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
 * RAII wrapper around CurrentThread::attachToGroup/detachFromGroupIfNotDetached.
 *
 * Typically used for inheriting thread group when scheduling tasks on a thread pool:
 *   pool->scheduleOrThrow([thread_group = CurrentThread::getGroup()]()
 *       {
 *           ThreadGroupSwitcher switcher(thread_group, "MyThread");
 *           ...
 *       });
 */
class ThreadGroupSwitcher : private boost::noncopyable
{
public:
    /// If thread_group_ is nullptr or equal to current thread group, does nothing.
    /// allow_existing_group:
    ///  * If false, asserts that the thread is not already attached to a different group.
    ///    Use this when running a task in a thread pool.
    ///  * If true, remembers the current group and restores it in destructor.
    /// If thread_name is not empty, calls setThreadName along the way; should be at most 15 bytes long.
    ThreadGroupSwitcher(ThreadGroupPtr thread_group_, ThreadName thread_name, bool allow_existing_group = false) noexcept;
    explicit ThreadGroupSwitcher(ThreadGroupPtr thread_group_, bool allow_existing_group = false) noexcept;

    ~ThreadGroupSwitcher();

private:
    ThreadStatus * prev_thread = nullptr;
    ThreadGroupPtr prev_thread_group;
    ThreadGroupPtr thread_group;
};


}
