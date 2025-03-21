#pragma once

#include <Core/Block.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <functional>
#include <future>

namespace DB
{

class BlockQueue
{
public:
    using Job = std::function<Block(const Block &)>;

    BlockQueue(size_t max_queue_size_, Job job_);

    ~BlockQueue();

    size_t size() const;

    bool enqueueForProcessing(const Block & block, bool wait = true);

    Block dequeueNextProcessed(bool wait = true);

    void work();

private:
    struct Task
    {
        Block block;
        std::promise<Block> promise;
    };

    size_t max_queue_size;
    Job job;
    std::unique_ptr<ThreadPool> thread_pool;

    std::atomic_bool no_more_input{false};
    std::atomic_bool failure{false};

    mutable std::mutex mutex;

    std::exception_ptr first_exception;

    std::condition_variable push_condition;
    std::condition_variable pop_condition;
    std::queue<Task> to_be_processed;

    std::condition_variable processed_condition;
    std::queue<std::future<Block>> processed;
};

}
