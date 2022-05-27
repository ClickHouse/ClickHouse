#include <Processors/Executors/ExecutorTasks.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ExecutorTasks::finish()
{
    {
        std::lock_guard lock(mutex);
        finished = true;
        async_task_queue.finish();
    }

    std::lock_guard guard(executor_contexts_mutex);

    for (auto & context : executor_contexts)
        context->wakeUp();
}

void ExecutorTasks::rethrowFirstThreadException()
{
    for (auto & executor_context : executor_contexts)
        executor_context->rethrowExceptionIfHas();
}

void ExecutorTasks::tryWakeUpAnyOtherThreadWithTasks(ExecutionThreadContext & self, std::unique_lock<std::mutex> & lock)
{
    if (!task_queue.empty() && !threads_queue.empty() && !finished)
    {
        size_t next_thread = self.thread_number + 1 == num_threads ? 0 : (self.thread_number + 1);
        auto thread_to_wake = task_queue.getAnyThreadWithTasks(next_thread);

        if (threads_queue.has(thread_to_wake))
            threads_queue.pop(thread_to_wake);
        else
            thread_to_wake = threads_queue.popAny();

        lock.unlock();
        executor_contexts[thread_to_wake]->wakeUp();
    }
}

void ExecutorTasks::tryGetTask(ExecutionThreadContext & context)
{
    {
        std::unique_lock lock(mutex);

        if (auto * async_task = context.tryPopAsyncTask())
        {
            context.setTask(async_task);
            --num_waiting_async_tasks;
        }
        else if (!task_queue.empty())
            context.setTask(task_queue.pop(context.thread_number));

        if (context.hasTask())
        {
            tryWakeUpAnyOtherThreadWithTasks(context, lock);
            return;
        }

        if (threads_queue.size() + 1 == num_threads && async_task_queue.empty() && num_waiting_async_tasks == 0)
        {
            lock.unlock();
            finish();
            return;
        }

    #if defined(OS_LINUX)
        if (num_threads == 1)
        {
            /// If we execute in single thread, wait for async tasks here.
            auto res = async_task_queue.wait(lock);
            if (!res)
            {
                if (finished)
                    return;
                throw Exception("Empty task was returned from async task queue", ErrorCodes::LOGICAL_ERROR);
            }

            context.setTask(static_cast<ExecutingGraph::Node *>(res.data));
            return;
        }
    #endif

        threads_queue.push(context.thread_number);
    }

    context.wait(finished);
}

void ExecutorTasks::pushTasks(Queue & queue, Queue & async_queue, ExecutionThreadContext & context)
{
    context.setTask(nullptr);

    /// Take local task from queue if has one.
    if (!queue.empty() && !context.hasAsyncTasks())
    {
        context.setTask(queue.front());
        queue.pop();
    }

    if (!queue.empty() || !async_queue.empty())
    {
        std::unique_lock lock(mutex);

#if defined(OS_LINUX)
        while (!async_queue.empty() && !finished)
        {
            int fd = async_queue.front()->processor->schedule();
            async_task_queue.addTask(context.thread_number, async_queue.front(), fd);
            async_queue.pop();
        }
#endif

        while (!queue.empty() && !finished)
        {
            task_queue.push(queue.front(), context.thread_number);
            queue.pop();
        }

        tryWakeUpAnyOtherThreadWithTasks(context, lock);
    }
}

void ExecutorTasks::init(size_t num_threads_, bool profile_processors)
{
    num_threads = num_threads_;
    threads_queue.init(num_threads);
    task_queue.init(num_threads);

    {
        std::lock_guard guard(executor_contexts_mutex);

        executor_contexts.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            executor_contexts.emplace_back(std::make_unique<ExecutionThreadContext>(i, profile_processors));
    }
}

void ExecutorTasks::fill(Queue & queue)
{
    std::lock_guard lock(mutex);

    size_t next_thread = 0;
    while (!queue.empty())
    {
        task_queue.push(queue.front(), next_thread);
        queue.pop();

        ++next_thread;
        if (next_thread >= num_threads)
            next_thread = 0;
    }
}

void ExecutorTasks::processAsyncTasks()
{
#if defined(OS_LINUX)
    {
        /// Wait for async tasks.
        std::unique_lock lock(mutex);
        while (auto task = async_task_queue.wait(lock))
        {
            auto * node = static_cast<ExecutingGraph::Node *>(task.data);
            executor_contexts[task.thread_num]->pushAsyncTask(node);
            ++num_waiting_async_tasks;

            if (threads_queue.has(task.thread_num))
            {
                threads_queue.pop(task.thread_num);
                executor_contexts[task.thread_num]->wakeUp();
            }
        }
    }
#endif
}

}
