#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Config.h>

#include <functional>

#if defined(EXECUTOR_SIM)
#    include <base/types.h>
#    include <memory>
#else
#    include <Common/ThreadPool.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(EXECUTOR_SIM)

class Thread
{
public:
    explicit Thread(std::function<void()> body);

    void join();

private:
    std::shared_ptr<UInt32> done;
};

#elif defined(EXECUTOR_FAULT_INJECTION)

class Thread
{
public:
    explicit Thread(std::function<void()> body);

    void join();

private:
    ThreadFromGlobalPool impl;
};

#else

using Thread = ThreadFromGlobalPool;

#endif

}
