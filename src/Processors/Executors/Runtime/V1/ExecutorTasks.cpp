#include <Processors/Executors/Runtime/V1/ExecutorTasks.h>
#include <Processors/IProcessor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Runtime::V1
{

void ExecutorTasks::notifySpillRequested()
{
    std::lock_guard lock(mutex);
    if (finished)
        return;
    spill_requested.store(true, std::memory_order_release);
#if defined(OS_LINUX) || defined(OS_DARWIN)
    if (num_threads == 1)
        async_task_queue.notify(PollingQueue::Notification::Spill);
#endif
    if (!threads_queue.empty())
        executor_contexts[threads_queue.popAny()]->wakeUp();
}

void ExecutorTasks::pushSpillTasks(Queue & queue, size_t thread_number)
{
    std::unique_lock lock(mutex);
    while (!queue.empty() && !finished)
    {
        fast_task_queue.push(queue.front(), thread_number);
        queue.pop();
        has_fast_tasks = true;
    }
    tryWakeUpAnyOtherThreadWithTasks(*executor_contexts[thread_number], lock);
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

void ExecutorTasks::rethrowFirstThreadException(const LoggerPtr & log)
{
    /// Log all of the LOGICAL_ERROR exceptions.
    for (auto & executor_context : executor_contexts)
        if (auto exception = executor_context->getException())
            if (getExceptionErrorCode(exception) == ErrorCodes::LOGICAL_ERROR)
                tryLogException(exception, log);

    /// Rethrow the first exception.
    for (auto & executor_context : executor_contexts)
        executor_context->rethrowExceptionIfHas();
}

void ExecutorTasks::tryWakeUpAnyOtherThreadWithTasks(ExecutionThreadContext & self, std::unique_lock<std::mutex> & lock)
{
    if (!threads_queue.empty() && !finished)
    {
        // Task execution priority is take into account:
        // We try first wake up thread to do fast tasks and only then to do regular tasks
        if (!fast_task_queue.empty())
            tryWakeUpAnyOtherThreadWithTasksInQueue(self, fast_task_queue, lock);
        else if (!task_queue.empty())
            tryWakeUpAnyOtherThreadWithTasksInQueue(self, task_queue, lock);
    }
}

void ExecutorTasks::tryWakeUpAnyOtherThreadWithTasksInQueue(ExecutionThreadContext & self, TaskQueue<IProcessor> & queue, std::unique_lock<std::mutex> & lock)
{
    size_t next_thread = self.thread_number + 1 >= use_threads ? 0 : (self.thread_number + 1);
    auto thread_to_wake = queue.getAnyThreadWithTasks(next_thread);

    if (threads_queue.has(thread_to_wake))
        threads_queue.pop(thread_to_wake);
    else
        thread_to_wake = threads_queue.popAny();

    if (thread_to_wake >= use_threads)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-empty queue without allocated thread");

    lock.unlock();
    executor_contexts[thread_to_wake]->wakeUp();
}

void ExecutorTasks::tryGetTask(ExecutionThreadContext & context)
{
    {
        std::unique_lock lock(mutex);

    #if defined(OS_LINUX) || defined(OS_DARWIN)
        if (num_threads == 1)
        {
            auto res = async_task_queue.tryGetReadyTask(lock);
            if (res)
            {
                context.setTask(static_cast<IProcessor *>(res.data));
                return;
            }
            if (res.hasNotification(PollingQueue::Notification::Finish) || res.hasNotification(PollingQueue::Notification::Spill))
                return;
        }
    #endif

        if (spill_requested.load(std::memory_order_acquire))
            return;

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

    #if defined(OS_LINUX) || defined(OS_DARWIN)
        if (num_threads == 1)
        {
            /// If we execute in single thread, wait for async tasks here.
            auto res = async_task_queue.wait(lock);
            if (!res)
            {
                if (res.hasNotification(PollingQueue::Notification::Finish) || res.hasNotification(PollingQueue::Notification::Spill))
                    return;
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty task was returned from async task queue");
            }

            context.setTask(static_cast<IProcessor *>(res.data));
            return;
        }
    #endif

        /// Enqueue thread into stack of waiting threads.
        threads_queue.push(context.thread_number);
    }

    context.wait(finished);
}

size_t ExecutorTasks::pushTasks(Queue & queue, Queue & async_queue, ExecutionThreadContext & context)
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

        /// Measure the parallelism of the push: how many threads could run the new tasks
        /// concurrently. Used by `PipelineExecutor` to size the slot-allocation ceiling via
        /// `ISlotAllocation::setMax` on the pipeline's `cpu_slots` handle.
        const size_t pushed = queue.size() + async_queue.size();

#if defined(OS_LINUX) || defined(OS_DARWIN)
        while (!async_queue.empty() && !finished)
        {
            auto [fd, events, timeout_ms] = async_queue.front()->scheduleForEvent();
            async_task_queue.addTask(context.thread_number, async_queue.front(), fd, events, timeout_ms);
            async_queue.pop();
        }
#endif

        while (!queue.empty() && !finished)
        {
            task_queue.push(queue.front(), context.thread_number);
            queue.pop();
        }

        /// Request a thread for every pushed task that idle threads can't cover, within the pipeline width
        /// left after the started threads. `PipelineExecutor` adds this delta to the `cpu_slots->setMax(...)`
        /// target so the slot allocator grows in step with demand, and spawns the requested threads once
        /// their slots are granted. It is computed before `tryWakeUpAnyOtherThreadWithTasks`, which may
        /// unlock the mutex.
        const size_t uncovered = pushed - std::min(pushed, threads_queue.size());
        const size_t requested = std::min(uncovered, widthLeft());
        addThreadsToSpawn(requested);

        /// Wake up at least one thread that will wake up other threads if required
        tryWakeUpAnyOtherThreadWithTasks(context, lock);

        return requested;
    }
    return 0; // No new tasks -- no need for new threads
}

size_t ExecutorTasks::widthLeft() const
{
    return num_threads > total_slots ? num_threads - total_slots : 0;
}

void ExecutorTasks::addThreadsToSpawn(size_t threads)
{
    /// Requests that wait for a CPU slot are not dropped, but they can't exceed the pipeline width.
    const size_t requested = threads_to_spawn.load(std::memory_order_relaxed);
    const size_t new_requested = std::min(requested + threads, widthLeft());
    if (new_requested != requested)
        threads_to_spawn.store(new_requested, std::memory_order_relaxed);
}

void ExecutorTasks::requestThreads(size_t threads)
{
    std::lock_guard lock(mutex);
    addThreadsToSpawn(threads);
}

void ExecutorTasks::init(size_t num_threads_, size_t use_threads_, const SlotAllocationPtr & cpu_slots_, bool profile_processors,
    bool trace_processors, ReadProgressCallback * callback, StepProfiler * step_profiler, QueryStatus * process_list_element)
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
            executor_contexts.emplace_back(std::make_unique<ExecutionThreadContext>(
                i, profile_processors, trace_processors, callback, step_profiler, process_list_element));
    }
}

size_t ExecutorTasks::fill(Queue & queue, [[maybe_unused]] Queue & async_queue)
{
    std::lock_guard lock(mutex);

    const size_t pushed = queue.size() + async_queue.size();
    size_t next_thread = 0;
#if defined(OS_LINUX) || defined(OS_DARWIN)
    while (!async_queue.empty())
    {
        auto [fd, events, timeout_ms] = async_queue.front()->scheduleForEvent();
        async_task_queue.addTask(next_thread, async_queue.front(), fd, events, timeout_ms);
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
    return pushed;
}

size_t ExecutorTasks::upscale(size_t slot_id)
{
    std::lock_guard lock(mutex);

    chassert(slot_id < slot_count.size());
    ++slot_count[slot_id];
    ++total_slots;
    use_threads = std::max(use_threads, slot_id + 1);

    /// Concurrent spawners may acquire more slots than requested; such slots are granted anyway, so they get a thread.
    if (const size_t requested = threads_to_spawn.load(std::memory_order_relaxed))
        threads_to_spawn.store(requested - 1, std::memory_order_relaxed);

    // Remaining pipeline-width capacity. Callers spawn one at a time and re-call upscale.
    // Too many idle threads can already cover incoming work — don't spawn more.
    const size_t capacity = num_threads > total_slots && threads_queue.size() <= TOO_MANY_IDLE_THRESHOLD ? num_threads - total_slots : 0;
    if (capacity == 0 && threads_to_spawn.load(std::memory_order_relaxed))
        threads_to_spawn.store(0, std::memory_order_relaxed);
    return capacity;
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

    /// The thread stopped because its slot was not granted in time. The slot may be granted later.
    addThreadsToSpawn(1);
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
    else if (!spill_requested.load(std::memory_order_acquire)
        && task_queue.empty() && fast_task_queue.empty() && async_task_queue.empty() && threads_queue.size() == total_slots)
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
#if defined(OS_LINUX) || defined(OS_DARWIN)
    {
        /// Wait for async tasks.
        std::unique_lock lock(mutex);
        while (true)
        {
            auto task = async_task_queue.wait(lock);
            if (task.hasNotification(PollingQueue::Notification::Finish))
                break;
            if (task.hasNotification(PollingQueue::Notification::Spill))
                continue;
            if (!task)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty task was returned from async task queue");

            auto * processor = static_cast<IProcessor *>(task.data);
            processor->onAsyncJobReady();

            if (fast_task_queue.empty())
                has_fast_tasks = true;
            fast_task_queue.push(processor, task.thread_num);

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

}
