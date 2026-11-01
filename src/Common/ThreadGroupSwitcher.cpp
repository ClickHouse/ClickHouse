#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

namespace DB
{

namespace FailPoints
{
    extern const char thread_group_switcher_post_attach_failure[];
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FAULT_INJECTED;
}

ThreadGroupPtr getCurrentThreadGroup()
{
    if (!current_thread)
        return nullptr;
    return current_thread->getThreadGroup();
}

ThreadGroupSwitcher::ThreadGroupSwitcher(ThreadGroupPtr thread_group_, ThreadName thread_name, bool allow_existing_group) noexcept
    : thread_group(std::move(thread_group_))
{
    try
    {
        if (!thread_group)
            return;

        prev_thread = current_thread;
        prev_thread_group = CurrentThread::getGroup();
        if (prev_thread_group)
        {
            if (prev_thread_group == thread_group)
            {
                thread_group = nullptr;
                prev_thread_group = nullptr;
                return;
            }
            else if (!allow_existing_group)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread ({}) is already attached to a group (master_thread_id {})", thread_name, prev_thread_group->master_thread_id);
            else
            {
                /// Borrowing a thread that owns a group: remember its name (even UNKNOWN) before the
                /// setThreadName below renames it, so we can restore it with the group.
                prev_thread_name = getThreadName();
                should_restore_prev_thread_name = true;
                CurrentThread::detachFromGroupIfNotDetached();
            }
        }

        if (!prev_thread)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tried to attach thread ({}) to a group, but the ThreadStatus is not initialized", thread_name);

        LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

        CurrentThread::attachToGroup(thread_group);
        setThreadName(thread_name);

        /// Simulate a failure after the attach succeeded (e.g. setThreadName throwing),
        /// to verify the catch block detaches from the target group and restores the
        /// previous one instead of leaving the thread attached to the failed target.
        fiu_do_on(FailPoints::thread_group_switcher_post_attach_failure,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure after attachToGroup");
        });
    }
    catch (...)
    {
        /// Unexpected. For caller's convenience avoid throwing exceptions.
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        try
        {
            LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
            /// The attach may have succeeded before a later step (e.g. setThreadName) threw,
            /// leaving the thread on the target group. Detach it first.
            if (CurrentThread::getGroup() == thread_group)
                CurrentThread::detachFromGroupIfNotDetached();
            /// Restore the borrowed group and name here: the destructor early-returns on this path
            /// (both groups are nulled below).
            if (prev_thread_group && !CurrentThread::getGroup())
                CurrentThread::attachToGroup(prev_thread_group);
            if (should_restore_prev_thread_name)
                setThreadName(prev_thread_name);
        }
        catch (...)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        thread_group = nullptr;
        prev_thread_group = nullptr;
    }
}

ThreadGroupSwitcher::~ThreadGroupSwitcher()
{
    if (!thread_group)
        return;

    try
    {
        ThreadStatus * cur_thread = current_thread;
        ThreadGroupPtr cur_thread_group = CurrentThread::getGroup();
        if (cur_thread != prev_thread)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ThreadGroupSwitcher-s are not properly nested: current thread changed between scope start ({}) and end ({})", prev_thread ? std::to_string(prev_thread->thread_id) : "nullptr", cur_thread ? std::to_string(cur_thread->thread_id) : "nullptr");
        if (cur_thread_group != thread_group)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ThreadGroupSwitcher-s are not properly nested: current thread group changed between scope start (master_thread_id {}) and end ({})", thread_group->master_thread_id, cur_thread_group ? "master_thread_id " + std::to_string(cur_thread_group->master_thread_id) : "nullptr");
        thread_group.reset();

        CurrentThread::detachFromGroupIfNotDetached();

        if (prev_thread_group)
        {
            LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
            CurrentThread::attachToGroup(prev_thread_group);
            /// Restore the borrowed thread's original name, else it keeps the async-pool name and
            /// misattributes later logs, traces, /proc names and jemalloc thread profiles.
            if (should_restore_prev_thread_name)
                setThreadName(prev_thread_name);
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
