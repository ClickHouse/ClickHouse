#pragma once

#include <Common/ThreadPool.h>
#include <future>


namespace DB
{

/// High-order function to run callbacks (functions with 'void()' signature) somewhere asynchronously.
template <typename Result>
using ThreadPoolCallbackRunner = std::function<std::future<Result>(std::function<Result()> &&, size_t priority)>;

/// Creates CallbackRunner that runs every callback with 'pool->scheduleOrThrow()'.
template <typename Result>
ThreadPoolCallbackRunner<Result> threadPoolCallbackRunner(ThreadPool & pool, const std::string & thread_name);

}
