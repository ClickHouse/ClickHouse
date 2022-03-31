#include "threadPoolCallbackRunner.h"

#include <base/scope_guard_safe.h>

#include <Common/CurrentThread.h>

namespace DB
{

CallbackRunner threadPoolCallbackRunner(ThreadPool & pool)
{
    return [pool = &pool, thread_group = CurrentThread::getGroup()](auto callback) mutable
    {
        pool->scheduleOrThrow(
            [&, callback = std::move(callback), thread_group]()
            {
                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                SCOPE_EXIT_SAFE({
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();

                    /// After we detached from the thread_group, parent for memory_tracker inside ThreadStatus will be reset to it's parent.
                    /// Typically, it may be changes from Process to User.
                    /// Usually it could be ok, because thread pool task is executed before user-level memory tracker is destroyed.
                    /// However, thread could stay alive inside the thread pool, and it's ThreadStatus as well.
                    /// When, finally, we destroy the thread (and the ThreadStatus),
                    /// it can use memory tracker in the ~ThreadStatus in order to alloc/free untracked_memory,
                    /// and by this time user-level memory tracker may be already destroyed.
                    ///
                    /// As a work-around, reset memory tracker to total, which is always alive.
                    CurrentThread::get().memory_tracker.setParent(&total_memory_tracker);
                });
                callback();
            });
    };
}

}
