#include <Processors/Executors/ExecutorTasks.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int QUOTA_EXPIRED;
    extern const int QUERY_WAS_CANCELLED;
}

void ExecutionThreadContext::wait(std::atomic_bool & finished)
{
    std::unique_lock lock(mutex);

    condvar.wait(lock, [&]
    {
        return finished || wake_flag;
    });

    wake_flag = false;
}

void ExecutionThreadContext::wakeUp()
{
    std::lock_guard guard(mutex);
    wake_flag = true;
    condvar.notify_one();
}

static bool checkCanAddAdditionalInfoToException(const DB::Exception & exception)
{
    /// Don't add additional info to limits and quota exceptions, and in case of kill query (to pass tests).
    return exception.code() != ErrorCodes::TOO_MANY_ROWS_OR_BYTES
           && exception.code() != ErrorCodes::QUOTA_EXPIRED
           && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED;
}

static void executeJob(IProcessor * processor)
{
    try
    {
        processor->work();
    }
    catch (Exception & exception)
    {
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + processor->getName());
        throw;
    }
}

bool ExecutionThreadContext::executeTask()
{
#ifndef NDEBUG
    Stopwatch execution_time_watch;
#endif

    try
    {
        executeJob(node->processor);

        ++node->num_executed_jobs;
    }
    catch (...)
    {
        node->exception = std::current_exception();
    }

#ifndef NDEBUG
    context->execution_time_ns += execution_time_watch.elapsed();
#endif

    return node->exception != nullptr;
}

void ExecutionThreadContext::rethrowExceptionIfHas()
{
    if (exception)
        std::rethrow_exception(exception);
}

ExecutingGraph::Node * ExecutionThreadContext::tryPopAsyncTask()
{
    ExecutingGraph::Node * task = nullptr;

    if (!async_tasks.empty())
    {
        task = async_tasks.front();
        async_tasks.pop();

        if (async_tasks.empty())
            has_async_tasks = false;
    }

    return task;
}

void ExecutionThreadContext::pushAsyncTask(ExecutingGraph::Node * async_task)
{
    async_tasks.push(async_task);
    has_async_tasks = true;
}

bool ExecutorTasks::runExpandPipeline(size_t thread_number, std::function<bool()> callback)
{
    auto & task = executor_contexts[thread_number]->addExpandPipelineTask(std::move(callback));
    ExecutionThreadContext::ExpandPipelineTask * desired = &task;
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
        context->wakeUp();
}

void ExecutorTasks::rethrowFirstThreadException()
{
    for (auto & executor_context : executor_contexts)
        executor_context->rethrowExceptionIfHas();
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

void ExecutorTasks::tryGetTask(ExecutionThreadContext & context)
{
    {
        std::unique_lock lock(task_queue_mutex);

        if (auto * async_task = context.tryPopAsyncTask())
        {
            context.setTask(async_task);
            --num_waiting_async_tasks;
        }
        else if (!task_queue.empty())
            context.setTask(task_queue.pop(context.thread_number));

        if (context.hasTask())
        {
            if (!task_queue.empty() && !threads_queue.empty())
            {
                size_t next_thread = context.thread_number + 1 == num_threads ? 0 : (context.thread_number + 1);
                auto thread_to_wake = task_queue.getAnyThreadWithTasks(next_thread);

                if (threads_queue.has(thread_to_wake))
                    threads_queue.pop(thread_to_wake);
                else
                    thread_to_wake = threads_queue.popAny();

                lock.unlock();
                executor_contexts[thread_to_wake]->wakeUp();
            }

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
                throw Exception("Empty task was returned from async task queue", ErrorCodes::LOGICAL_ERROR);

            context.setTask(static_cast<ExecutingGraph::Node *>(res.data));
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
        std::unique_lock lock(task_queue_mutex);

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

        if (!threads_queue.empty() && !task_queue.empty() && !finished)
        {
            size_t next_thread = context.thread_number + 1 == num_threads ? 0 : (context.thread_number + 1);
            auto thread_to_wake = task_queue.getAnyThreadWithTasks(next_thread);

            if (threads_queue.has(thread_to_wake))
                threads_queue.pop(thread_to_wake);
            else
                thread_to_wake = threads_queue.popAny();

            lock.unlock();

            executor_contexts[thread_to_wake]->wakeUp();
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
            executor_contexts.emplace_back(std::make_unique<ExecutionThreadContext>(i));
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
