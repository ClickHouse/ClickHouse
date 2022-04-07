#pragma once

#include <Common/ThreadPool.h>


namespace DB
{

/// High-order function to run callbacks (functions with 'void()' signature) somewhere asynchronously
using CallbackRunner = std::function<void(std::function<void()>)>;

/// Creates CallbackRunner that runs every callback with 'pool->scheduleOrThrow()'
CallbackRunner threadPoolCallbackRunner(ThreadPool & pool);

}
