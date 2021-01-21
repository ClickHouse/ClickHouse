#include <Processors/Executors/ExecutorTasks.h>

namespace DB
{

void ExecutionThreadContext::wait(std::atomic_bool & finished)
{
    std::unique_lock lock(mutex);

    condvar.wait(lock, [&]
    {
        return finished || wake_flag;
    });

    wake_flag = false;
}

bool ExecutorTasks::runExpandPipeline(size_t thread_number, std::function<bool()> callback)
{
    executor_contexts[thread_number]->task_list.emplace_back(std::move(callback));

    ExecutionThreadContext::ExpandPipelineTask * desired = &executor_contexts[thread_number]->task_list.back();
    ExecutionThreadContext::ExpandPipelineTask * expected = nullptr;

    while (!expand_pipeline_task.compare_exchange_strong(expected, desired))
    {
        if (!doExpandPipeline(expected, true))
            return false;

        expected = nullptr;
    }

    return doExpandPipeline(desired, true);
}

bool ExecutorTasks::doExpandPipeline(ExecutionThreadContext::ExpandPipelineTask * task, bool processing)
{
    std::unique_lock lock(task->mutex);

    if (processing)
        ++task->num_waiting_processing_threads;

    task->condvar.wait(lock, [&]()
    {
        return task->num_waiting_processing_threads >= num_processing_executors || expand_pipeline_task != task;
    });

    bool result = true;

    /// After condvar.wait() task may point to trash. Can change it only if it is still in expand_pipeline_task.
    if (expand_pipeline_task == task)
    {
        result = task->callback();

        expand_pipeline_task = nullptr;

        lock.unlock();
        task->condvar.notify_all();
    }

    return result;
}

void ExecutorTasks::finish()
{
    {
        std::lock_guard lock(task_queue_mutex);
        finished = true;
        async_task_queue.finish();
    }

    std::lock_guard guard(executor_contexts_mutex);

    for (auto & context : executor_contexts)
    {
        {
            std::lock_guard lock(context->mutex);
            context->wake_flag = true;
        }

        context->condvar.notify_one();
    }
}

void ExecutorTasks::rethrowFirstThreadException()
{
    for (auto & executor_context : executor_contexts)
        if (executor_context->exception)
            std::rethrow_exception(executor_context->exception);
}

void ExecutorTasks::wakeUpExecutor(size_t thread_num)
{
    std::lock_guard guard(executor_contexts[thread_num]->mutex);
    executor_contexts[thread_num]->wake_flag = true;
    executor_contexts[thread_num]->condvar.notify_one();
}


void ExecutorTasks::expandPipelineStart()
{
    ++num_processing_executors;
    while (auto * task = expand_pipeline_task.load())
        doExpandPipeline(task, true);
}

void ExecutorTasks::expandPipelineEnd()
{
    --num_processing_executors;
    while (auto * task = expand_pipeline_task.load())
        doExpandPipeline(task, false);
}

ExecutingGraph::Node * ExecutorTasks::tryGetTask(size_t thread_num)
{
    ExecutingGraph::Node * node = nullptr;
    auto & context = executor_contexts[thread_num];

    {
        std::unique_lock lock(task_queue_mutex);

        if (!context->async_tasks.empty())
        {
            node = context->async_tasks.front();
            context->async_tasks.pop();
            --num_waiting_async_tasks;

            if (context->async_tasks.empty())
                context->has_async_tasks = false;
        }
        else if (!task_queue.empty())
            node = task_queue.pop(thread_num);

        if (node)
        {
            if (!task_queue.empty() && !threads_queue.empty())
            {
                auto thread_to_wake = task_queue.getAnyThreadWithTasks(thread_num + 1 == num_threads ? 0 : (thread_num + 1));

                if (threads_queue.has(thread_to_wake))
                    threads_queue.pop(thread_to_wake);
                else
                    thread_to_wake = threads_queue.popAny();

                lock.unlock();
                wakeUpExecutor(thread_to_wake);
            }

            return node;
        }

        if (threads_queue.size() + 1 == num_threads && async_task_queue.empty() && num_waiting_async_tasks == 0)
        {
            lock.unlock();
            finish();
            return nullptr;
        }

    #if defined(OS_LINUX)
        if (num_threads == 1)
        {
            /// If we execute in single thread, wait for async tasks here.
            auto res = async_task_queue.wait(lock);
            if (!res)
                throw Exception("Empty task was returned from async task queue", ErrorCodes::LOGICAL_ERROR);

            return  static_cast<ExecutingGraph::Node *>(res.data);
        }
    #endif

        threads_queue.push(thread_num);
    }

    context->wait(finished);

    return nullptr;
}

void ExecutorTasks::pushTasks(Queue & queue, Queue & async_queue, size_t thread_num)
{
    auto & context = executor_contexts[thread_num];
    /// Take local task from queue if has one.
    if (!queue.empty() && !context->has_async_tasks)
    {
        context->node = queue.front();
        queue.pop();
    }

    if (!queue.empty() || !async_queue.empty())
    {
        std::unique_lock lock(task_queue_mutex);

#if defined(OS_LINUX)
        while (!async_queue.empty() && !finished)
        {
            async_task_queue.addTask(thread_num, async_queue.front(), async_queue.front()->processor->schedule());
            async_queue.pop();
        }
#endif

        while (!queue.empty() && !finished)
        {
            task_queue.push(queue.front(), thread_num);
            queue.pop();
        }

        if (!threads_queue.empty() && !task_queue.empty() && !finished)
        {
            auto thread_to_wake = task_queue.getAnyThreadWithTasks(thread_num + 1 == num_threads ? 0 : (thread_num + 1));

            if (threads_queue.has(thread_to_wake))
                threads_queue.pop(thread_to_wake);
            else
                thread_to_wake = threads_queue.popAny();

            lock.unlock();

            wakeUpExecutor(thread_to_wake);
        }
    }
}

void ExecutorTasks::init(size_t num_threads_)
{
    num_threads = num_threads_;
    threads_queue.init(num_threads);
    task_queue.init(num_threads);

    {
        std::lock_guard guard(executor_contexts_mutex);

        executor_contexts.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            executor_contexts.emplace_back(std::make_unique<ExecutionThreadContext>());
    }
}

void ExecutorTasks::fill(Queue & queue)
{
    std::lock_guard lock(task_queue_mutex);

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
        std::unique_lock lock(task_queue_mutex);
        while (auto task = async_task_queue.wait(lock))
        {
            auto * node = static_cast<ExecutingGraph::Node *>(task.data);
            executor_contexts[task.thread_num]->async_tasks.push(node);
            executor_contexts[task.thread_num]->has_async_tasks = true;
            ++num_waiting_async_tasks;

            if (threads_queue.has(task.thread_num))
            {
                threads_queue.pop(task.thread_num);
                wakeUpExecutor(task.thread_num);
            }
        }
    }
#endif
}

}
