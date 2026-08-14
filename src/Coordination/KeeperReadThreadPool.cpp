#include <Coordination/KeeperReadThreadPool.h>

#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>

namespace CurrentMetrics
{
    extern const Metric KeeperReadThreads;
    extern const Metric KeeperReadThreadsActive;
    extern const Metric KeeperReadThreadsScheduled;
}

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 parallel_read_threads;
    extern const CoordinationSettingsUInt64 parallel_read_chunk_size;
    extern const CoordinationSettingsUInt64 parallel_read_min_batch;
}


void KeeperReadThreadPool::shutdown()
{
    {
        std::lock_guard lock(mutex);
        chassert(busy_threads.load() == 0); // no concurrent execute; otherwise it may get stuck
        target_num_threads = 0;
    }
    wake_cv.notify_all();
    if (pool)
        pool->wait();
}

KeeperReadThreadPool::~KeeperReadThreadPool()
{
    shutdown();
}

void KeeperReadThreadPool::execute(size_t count, const CoordinationSettings & coordination_settings, ExecuteRequestsFunction func)
{
    std::unique_lock lock(mutex);

    /// Allow changing parameters on the fly, just to make benchmarking more enjoyable.

    target_num_threads = coordination_settings[CoordinationSetting::parallel_read_threads];
    if (target_num_threads > 0 && !pool.has_value())
    {
        pool.emplace(
            CurrentMetrics::KeeperReadThreads, CurrentMetrics::KeeperReadThreadsActive, CurrentMetrics::KeeperReadThreadsScheduled,
            /*max_threads_=*/ ThreadPool::MAX_THEORETICAL_THREAD_COUNT,
            /*max_free_threads_=*/ 0,
            /*queue_size_=*/ 0);
    }
    while (running_threads < target_num_threads)
    {
        ++running_threads;
        try
        {
            pool->scheduleOrThrowOnError([this, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::KEEPER_READ);
                    threadFunction();
                });
        }
        catch (...)
        {
            tryLogCurrentException("KeeperReadThreadPool", "Failed to create thread", LogsLevel::fatal);
            std::abort();
        }
    }

    /// If thread pool is disabled, execute right here.
    if (target_num_threads == 0 || count < coordination_settings[CoordinationSetting::parallel_read_min_batch])
    {
        func(0, count);
        return;
    }

    chunk_size = coordination_settings[CoordinationSetting::parallel_read_chunk_size];
    chunk_size = std::max(chunk_size, size_t(1));

    /// Start the batch.

    chassert(request_count == 0); // no concurrent `execute` calls
    next_request_idx.store(0);
    busy_threads.store(target_num_threads);
    request_count = count;
    batch_idx += 1;
    first_exception = {};
    execute_requests = func;

    wake_cv.notify_all();

    /// Wait for all threads to see this batch and finish working on it.
    ///
    /// (Note: it may seem that instead of waiting for all threads we could wait for all requests to
    ///  complete, regardless of how many threads participated. But then there would be a race:
    ///  a thread may pick up request_count value of one batch, then compare it to next_request_idx
    ///  value of the next batch. Maybe this particular race can be avoided by counting down instead
    ///  of up, but still that setup seems generally more fragile and hard to think about than the
    ///  setup where all threads are "joined" after each request.)

    done_cv.wait(lock, [&] { return busy_threads.load() == 0; });

    request_count = 0;
    if (first_exception)
        std::rethrow_exception(std::exchange(first_exception, {}));
}

void KeeperReadThreadPool::threadFunction()
{
    try
    {
        std::unique_lock lock(mutex);

        size_t seen_batch_idx = 0;
        while (true)
        {
            if (running_threads > target_num_threads)
            {
                running_threads -= 1;
                return;
            }
            if (batch_idx == seen_batch_idx)
            {
                /// No new work. Wait for something to change.
                wake_cv.wait(lock);
                continue;
            }

            seen_batch_idx = batch_idx;

            lock.unlock();

            try
            {
                /// Pick up and execute ranges of requests.
                while (true)
                {
                    size_t start_idx = next_request_idx.fetch_add(chunk_size, std::memory_order_relaxed);
                    if (start_idx >= request_count)
                        break;
                    size_t end_idx = std::min(request_count, start_idx + chunk_size);
                    execute_requests(start_idx, end_idx);
                }
            }
            catch (...)
            {
                std::lock_guard lock2(mutex);
                if (!first_exception)
                    first_exception = std::current_exception();
            }

            /// Report that this thread is finished with this batch.
            size_t prev_busy_threads = busy_threads.fetch_sub(1, std::memory_order_release);
            chassert(prev_busy_threads > 0);
            if (prev_busy_threads == 1)
                done_cv.notify_all();

            lock.lock();
        }
    }
    catch (...)
    {
        tryLogCurrentException("KeeperReadThreadPool", "Unexpected exception in keeper read request scheduling");
        std::abort();
    }
}

}
