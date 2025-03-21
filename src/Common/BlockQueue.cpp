#include <Common/BlockQueue.h>

#include <Common/OpenTelemetryTraceContext.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace CurrentMetrics
{
extern const Metric BlockQueueThreads;
extern const Metric BlockQueueThreadsActive;
extern const Metric BlockQueueThreadsScheduled;
}

namespace DB
{

BlockQueue::BlockQueue(size_t max_queue_size_, Job job_)
    : max_queue_size(max_queue_size_)
    , job(std::move(job_))
    , thread_pool(std::make_unique<ThreadPool>(
          CurrentMetrics::BlockQueueThreads,
          CurrentMetrics::BlockQueueThreadsActive,
          CurrentMetrics::BlockQueueThreadsScheduled,
          max_queue_size))
{
    try
    {
        for (size_t i = 0; i < max_queue_size; ++i)
        {
            thread_pool->scheduleOrThrow(
                [this, i, thread_group = CurrentThread::getGroup()]()
                {
                    // TODO(nickitat): attaching to the group makes some tests (on the threads usage) fail. We should respect these limits.
                    ThreadGroupSwitcher switcher(thread_group, fmt::format("BlockQ_{}", i).c_str());
                    work();
                });
        }
        max_queue_size = 1e18;
    }
    catch (...)
    {
        failure = true;
        pop_condition.notify_all();
        push_condition.notify_all();
        thread_pool->wait();
        throw;
    }
}

BlockQueue::~BlockQueue()
{
    no_more_input = true;
    pop_condition.notify_all();
    push_condition.notify_all();
    thread_pool->wait();

    // chassert(failure || to_be_processed.empty());
    // chassert(failure || processed.empty());
}

size_t BlockQueue::size() const
{
    std::unique_lock lock(mutex);
    return processed.size();
}

bool BlockQueue::enqueueForProcessing(const Block & block, bool wait)
{
    {
        std::unique_lock lock(mutex);

        auto predicate = [&]() { return failure || (processed.size() < max_queue_size); };

        if (!predicate() && !wait)
            return false;

        push_condition.wait(lock, predicate);

        if (first_exception)
            std::rethrow_exception(first_exception);

        std::promise<Block> promise;
        processed.push(promise.get_future());
        to_be_processed.emplace(block, std::move(promise));
    }
    pop_condition.notify_one();
    return true;
}

Block BlockQueue::dequeueNextProcessed(bool wait)
{
    std::future<Block> block;
    {
        std::unique_lock lock(mutex);

        auto predicate = [&]() { return failure || !processed.empty(); };

        if (!predicate() && !wait)
            return {};

        processed_condition.wait(lock, predicate);

        if (first_exception)
            std::rethrow_exception(first_exception);

        block = std::move(processed.front());
        processed.pop();
    }
    return block.get();
}

void BlockQueue::work()
{
    while (true)
    {
        Task task;
        {
            std::unique_lock lock(mutex);

            auto predicate = [&]() { return no_more_input || failure || !to_be_processed.empty(); };

            pop_condition.wait(lock, predicate);

            if (failure || (no_more_input && to_be_processed.empty()))
                break;

            task = std::move(to_be_processed.front());
            to_be_processed.pop();
        }
        push_condition.notify_one();

        try
        {
            OpenTelemetry::SpanHolder span("BlockQueue::work");
            auto processed_block = job(task.block);
            task.promise.set_value(std::move(processed_block));
        }
        catch (...)
        {
            {
                std::unique_lock lock(mutex);
                first_exception = std::current_exception();
                failure = true;
            }
            task.promise.set_exception(first_exception);
            pop_condition.notify_all();
            push_condition.notify_all();
        }
        processed_condition.notify_one();
    }
}

}
