#pragma once

#include <Common/ThreadPool.h>


namespace DB
{

using CallbackRunner = std::function<void(std::function<void()>)>;

CallbackRunner threadPoolCallbackRunner(ThreadPool & pool);

}
