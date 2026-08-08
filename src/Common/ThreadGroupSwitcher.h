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

/// Like `getCurrentThreadGroup`, but never returns a borrowed group (see `ThreadGroup`): a borrowed
/// group is only valid while its parent query is alive, and async work may outlive that query.
/// For a borrowed group it returns the group's async-callback companion, which preserves the query's
/// cancellation predicates and metadata but owns its accounting. As a consequence, async work scheduled
/// from a borrowed scope (materialized/window view processing, async insert flush, `EXPLAIN ANALYZE`)
/// is accounted at the thread/global level, not to the borrowed group — per-scope diagnostics derived
/// from the group (e.g. `peak_memory_usage` in `system.query_views_log`) exclude such async allocations.
ThreadGroupPtr getCurrentThreadGroupForAsyncCallback();

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


}
