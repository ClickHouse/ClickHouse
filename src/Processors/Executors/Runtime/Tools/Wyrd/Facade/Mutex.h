#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Config.h>
#include <base/defines.h>

#include <mutex>

#if defined(EXECUTOR_SIM) || defined(EXECUTOR_FAULT_INJECTION)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Facade/Atomic.h>
#    include <base/types.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(EXECUTOR_SIM) || defined(EXECUTOR_FAULT_INJECTION)

class TSA_CAPABILITY("mutex") Mutex
{
public:
    void lock() TSA_ACQUIRE();
    void unlock() TSA_RELEASE();

private:
    Atomic<UInt32> locked{0};
};

#else

using Mutex = std::mutex;

#endif

}
