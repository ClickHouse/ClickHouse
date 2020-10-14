# pragma once
#include <functional>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <common/logger_useful.h>

namespace DB
{

enum PoolType
{
    MERGE_MUTATE,
    MOVE,
    FETCH,
};

struct MergeTreeBackgroundJob
{
    ThreadPool::Job job;
    PoolType execute_in_pool;

    MergeTreeBackgroundJob(ThreadPool::Job && job_, PoolType execute_in_pool_)
        : job(std::move(job_)), execute_in_pool(execute_in_pool_)
    {}

    void operator()()
    try
    {
        job();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
};
   
}
