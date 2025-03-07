#include <Common/BlockQueue.h>

namespace CurrentMetrics
{
extern const Metric AggregatorThreads;
extern const Metric AggregatorThreadsActive;
extern const Metric AggregatorThreadsScheduled;
}

namespace DB
{

BlockQueue::BlockQueue(size_t max_queue_size_, Job job_)
    : max_queue_size(max_queue_size_)
    , job(std::move(job_))
    , thread_pool(std::make_unique<ThreadPool>(
          CurrentMetrics::AggregatorThreads,
          CurrentMetrics::AggregatorThreadsActive,
          CurrentMetrics::AggregatorThreadsScheduled,
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
    // TODO(nickitat): an empty block is not always inserted at the end to signify eof (e.g. for insert queries we send only table structure).
    no_more_input = true;
    pop_condition.notify_all();
    push_condition.notify_all();
    thread_pool->wait();

    // chassert(failure || to_be_processed.empty());
    // chassert(failure || processed.empty());
}

size_t BlockQueue::size()
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

        //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        push_condition.wait(lock, predicate);

        if (first_exception)
            std::rethrow_exception(first_exception);

        //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        std::promise<Block> promise;
        processed.push(promise.get_future());
        to_be_processed.emplace(block, std::move(promise));
        //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
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

        //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        processed_condition.wait(lock, predicate);

        if (first_exception)
            std::rethrow_exception(first_exception);

        //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        block = std::move(processed.front());
        processed.pop();
    }
    //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
    return block.get();
}

void BlockQueue::work()
{
    while (true)
    {
        //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        Task task;
        {
            std::unique_lock lock(mutex);

            auto predicate = [&]() { return no_more_input || failure || !to_be_processed.empty(); };

            //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
            pop_condition.wait(lock, predicate);

            if (failure || (no_more_input && to_be_processed.empty()))
                break;

            //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
            task = std::move(to_be_processed.front());
            //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
            to_be_processed.pop();
        }
        push_condition.notify_one();

        //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
        try
        {
            auto processed_block = job(task.block);
            task.promise.set_value(std::move(processed_block));
            //LOG_DEBUG(&Poco::Logger::get("debug"), "__PRETTY_FUNCTION__={}, __LINE__={}", __PRETTY_FUNCTION__, __LINE__);
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
