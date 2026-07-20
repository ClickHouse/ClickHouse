#include <Processors/Executors/ExecutorTasks.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

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

    freeCPU();

    std::lock_guard guard(executor_contexts_mutex);

    for (auto & context : executor_contexts)
        context->wakeUp();
}

void ExecutorTasks::freeCPU()
{
    SlotAllocationPtr slots;
    {
        std::lock_guard lock(mutex);
        slots = std::exchange(cpu_slots, nullptr);
    }
    if (!slots)
        return;
    slots->free();
}

void ExecutorTasks::rethrowFirstThreadException()
{
    for (auto & executor_context : executor_contexts)
        executor_context->rethrowExceptionIfHas();
}

ExecutorTasks::SpawnStatus ExecutorTasks::tryWakeUpAnyOtherThreadWithTasks(ExecutionThreadContext & self, std::unique_lock<std::mutex> & lock)
{
    if (!threads_queue.empty() && !finished)
    {
        // Task execution priority is take into account:
        // We try first wake up thread to do fast tasks and only then to do regular tasks
        if (!fast_task_queue.empty())
            return tryWakeUpAnyOtherThreadWithTasksInQueue(self, fast_task_queue, lock);
        else if (!task_queue.empty())
            return tryWakeUpAnyOtherThreadWithTasksInQueue(self, task_queue, lock);
    }
    return SHOULD_SPAWN; // There is no idle threads - we'd like to have more threads
}

ExecutorTasks::SpawnStatus ExecutorTasks::tryWakeUpAnyOtherThreadWithTasksInQueue(ExecutionThreadContext & self, TaskQueue<ExecutingGraph::Node> & queue, std::unique_lock<std::mutex> & lock)
{
    size_t next_thread = self.thread_number + 1 >= use_threads ? 0 : (self.thread_number + 1);
    auto thread_to_wake = queue.getAnyThreadWithTasks(next_thread);

    if (threads_queue.has(thread_to_wake))
        threads_queue.pop(thread_to_wake);
    else
        thread_to_wake = threads_queue.popAny();

    if (thread_to_wake >= use_threads)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-empty queue without allocated thread");

    // Do not spawn new threads if there are already a lot of idle threads
    SpawnStatus result = threads_queue.size() <= TOO_MANY_IDLE_THRESHOLD ? SHOULD_SPAWN : DO_NOT_SPAWN;

    lock.unlock();
    executor_contexts[thread_to_wake]->wakeUp();
    return result;
}

void ExecutorTasks::tryGetTask(ExecutionThreadContext & context)
{
    {
        std::unique_lock lock(mutex);

    #if defined(OS_LINUX)
        if (num_threads == 1)
        {
            if (auto res = async_task_queue.tryGetReadyTask(lock))
            {
                context.setTask(static_cast<ExecutingGraph::Node *>(res.data));
                return;
            }
        }
    #endif

        /// Try get async task assigned to this thread or any other task from queue.
        if (!fast_task_queue.empty())
        {
            context.setTask(fast_task_queue.pop(context.thread_number));
            if (fast_task_queue.empty())
                has_fast_tasks = false;
        }
        else if (!task_queue.empty())
        {
            context.setTask(task_queue.pop(context.thread_number));
        }

        /// Task found.
        if (context.hasTask())
        {
            /// We have to wake up at least one thread if there are pending tasks.
            /// That thread will wake up other threads during its `tryGetTask()` call if any.
            tryWakeUpAnyOtherThreadWithTasks(context, lock);
            return;
        }

        /// This thread has no tasks to do and is going to wait.
        /// Finish execution if this was the last active thread.
        chassert(task_queue.empty() && fast_task_queue.empty());
        if (threads_queue.size() + 1 == total_slots && async_task_queue.empty())
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty task was returned from async task queue");
            }

            context.setTask(static_cast<ExecutingGraph::Node *>(res.data));
            return;
        }
    #endif

        /// Enqueue thread into stack of waiting threads.
        threads_queue.push(context.thread_number);
    }

    context.wait(finished);
}

ExecutorTasks::SpawnStatus ExecutorTasks::pushTasks(Queue & queue, Queue & async_queue, ExecutionThreadContext & context)
{
    /// Take local task from queue if has one.
    if (!queue.empty() && !has_fast_tasks.load(std::memory_order_relaxed)
        && context.num_scheduled_local_tasks < ExecutionThreadContext::max_scheduled_local_tasks)
    {
        ++context.num_scheduled_local_tasks;
        context.setTask(queue.front());
        queue.pop();
    }
    else
    {
        context.num_scheduled_local_tasks = 0;
        context.setTask(nullptr);
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

        /// Wake up at least one thread that will wake up other threads if required
        return tryWakeUpAnyOtherThreadWithTasks(context, lock);
    }
    return DO_NOT_SPAWN; // No new tasks -- no need for new threads
}

void ExecutorTasks::init(size_t num_threads_, size_t use_threads_, const SlotAllocationPtr & cpu_slots_, bool profile_processors, bool trace_processors, ReadProgressCallback * callback)
{
    num_threads = num_threads_;
    use_threads = use_threads_;
    threads_queue.init(num_threads);
    task_queue.init(num_threads);
    fast_task_queue.init(num_threads);

    {
        std::lock_guard lock(mutex); // In case finish() is executed concurrently with init() due to exception
        cpu_slots = cpu_slots_;
    }

    // Initialize slot counters with zeros up to max_threads
    slot_count.resize(num_threads, 0);

    {
        std::lock_guard guard(executor_contexts_mutex);

        executor_contexts.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            executor_contexts.emplace_back(std::make_unique<ExecutionThreadContext>(i, profile_processors, trace_processors, callback));
    }
}

void ExecutorTasks::fill(Queue & queue, [[maybe_unused]] Queue & async_queue)
{
    std::lock_guard lock(mutex);

    size_t next_thread = 0;
#if defined(OS_LINUX)
    while (!async_queue.empty())
    {
        int fd = async_queue.front()->processor->schedule();
        async_task_queue.addTask(next_thread, async_queue.front(), fd);
        async_queue.pop();

        ++next_thread;

        /// It is important to keep queues empty for threads that are not started yet.
        /// Otherwise that thread can be selected by `tryWakeUpAnyOtherThreadWithTasks()`, leading to deadlock.
        if (next_thread >= use_threads)
            next_thread = 0;
    }
#endif

    while (!queue.empty())
    {
        task_queue.push(queue.front(), next_thread);
        queue.pop();

        ++next_thread;

        /// It is important to keep queues empty for threads that are not started yet.
        /// Otherwise that thread can be selected by `tryWakeUpAnyOtherThreadWithTasks()`, leading to deadlock.
        if (next_thread >= use_threads)
            next_thread = 0;
    }
}

ExecutorTasks::SpawnStatus ExecutorTasks::upscale(size_t slot_id)
{
    std::lock_guard lock(mutex);

    chassert(slot_id < slot_count.size());
    ++slot_count[slot_id];
    ++total_slots;
    use_threads = std::max(use_threads, slot_id + 1);

    return threads_queue.size() <= TOO_MANY_IDLE_THRESHOLD ? SHOULD_SPAWN : DO_NOT_SPAWN;
}

void ExecutorTasks::downscale(size_t slot_id)
{
    std::lock_guard lock(mutex);

    if (slot_id >= slot_count.size() || slot_count[slot_id] == 0)
        return;
    --slot_count[slot_id];

    if (slot_id + 1 == use_threads)
    {
        // The highest slot was downscaled, find new highest
        for (size_t i = use_threads; i > 0; --i)
        {
            if (slot_count[i - 1] > 0)
            {
                use_threads = i;
                break;
            }
        }
    }
}

void ExecutorTasks::preempt(size_t slot_id)
{
    std::unique_lock lock(mutex);
    --total_slots;

    /// We should make sure that preempted thread has no local task inside context.
    /// It is allowed to have tasks in `task_queue` or `fast_task_queue` because they can be stealed by other threads.
    auto & context = executor_contexts[slot_id];
    if (auto * task = context->popTask())
    {
        task_queue.push(task, slot_id);
        /// Wake up at least one thread to avoid deadlocks (all other threads maybe idle)
        tryWakeUpAnyOtherThreadWithTasks(*context, lock); // this releases the lock if it wakes up a thread
    }
    else if (task_queue.empty() && fast_task_queue.empty() && async_task_queue.empty() && threads_queue.size() == total_slots)
    {
        /// Finish pipeline if preempted thread was the last non-idle thread executed the last task of the whole pipeline
        lock.unlock();
        finish();
    }
}

void ExecutorTasks::resume(size_t)
{
    std::lock_guard lock(mutex);
    ++total_slots;
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
            node->processor->onAsyncJobReady();

            if (fast_task_queue.empty())
                has_fast_tasks = true;
            fast_task_queue.push(node, task.thread_num);

            /// Wake up a thread to process the task, preferably thread that should process this task
            if (!threads_queue.empty() && !finished)
            {
                auto thread_to_wake = fast_task_queue.getAnyThreadWithTasks(task.thread_num);

                if (threads_queue.has(thread_to_wake))
                    threads_queue.pop(thread_to_wake);
                else
                    thread_to_wake = threads_queue.popAny();

                if (thread_to_wake >= use_threads)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-empty queue without allocated thread");

                executor_contexts[thread_to_wake]->wakeUp();
            }
        }
    }
#endif
}

String ExecutorTasks::dump()
{
    std::scoped_lock lock(mutex);

    WriteBufferFromOwnString buffer;

    buffer << "ExecutorTasks:\n";
    buffer << "  num_threads: " << num_threads << "\n";
    buffer << "  use_threads: " << use_threads << "\n";
    buffer << "  total_slots: " << total_slots << "\n";
    buffer << "  finished: " << static_cast<int>(finished) << "\n";
    buffer << "  has_fast_tasks: " << has_fast_tasks.load(std::memory_order_relaxed) << "\n";

    for (size_t i = 0; i < slot_count.size(); ++i)
        buffer << "  slot_count[" << i << "]: " << slot_count[i] << "\n";

    // Dump task queues
    buffer << "  task_queue size: " << task_queue.size() << "\n";
    buffer << "  fast_task_queue size: " << fast_task_queue.size() << "\n";
    buffer << "  async_task_queue size: " << async_task_queue.size() << "\n";
    buffer << "  threads_queue size: " << threads_queue.size() << "\n";

    return buffer.str();
}

}
