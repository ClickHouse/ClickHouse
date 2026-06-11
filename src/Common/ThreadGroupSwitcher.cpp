#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

        /// The thread may have a query_id which the group being attached cannot reestablish:
        /// either assigned directly without any group (e.g. `BgSchPool::<uuid>` set by
        /// BackgroundSchedulePool), or kept alive by an outer switcher in the same way.
        /// Detaching clears the thread's query_id, so remember it to preserve it manually.
        prev_query_id = std::string(CurrentThread::getQueryId());

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
                CurrentThread::detachFromGroupIfNotDetached();
        }

        if (!prev_thread)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tried to attach thread ({}) to a group, but the ThreadStatus is not initialized", thread_name);

        LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

        CurrentThread::attachToGroup(thread_group);

        /// If the new group cannot provide a query_id (e.g. a scope group created on a thread
        /// without any group: its query context is not owned by anyone and is already expired,
        /// so attaching did not set any query_id), keep the thread's previous one.
        if (!prev_query_id.empty() && CurrentThread::getQueryId().empty())
            prev_thread->setQueryId(std::string(prev_query_id));

        setThreadName(thread_name);
        LOG_TEST(getLogger("ThreadGroupSwitcher"), "Attach thread {} to thread group with master_thread_id {}", thread_name, thread_group->master_thread_id);
    }
    catch (...)
    {
        /// Unexpected. For caller's convenience avoid throwing exceptions.
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
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
        }

        /// Restore the query_id if reattaching could not reestablish it (see the constructor).
        if (!prev_query_id.empty() && CurrentThread::getQueryId().empty())
            prev_thread->setQueryId(std::move(prev_query_id));
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
