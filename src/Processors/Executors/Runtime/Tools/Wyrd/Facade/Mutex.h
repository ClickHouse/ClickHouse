#pragma once

#include <base/defines.h>

#include <mutex>

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Facade/Atomic.h>
#    include <base/types.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

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
