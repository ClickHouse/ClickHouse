#pragma once

#include <base/defines.h>

#include <mutex>
#include <source_location>

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#    include <base/types.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

class TSA_CAPABILITY("mutex") Mutex
{
public:
    void lock(std::source_location location = std::source_location::current()) TSA_ACQUIRE();
    void unlock(std::source_location location = std::source_location::current()) TSA_RELEASE();

private:
    Atomic<UInt32> locked{0};
};

#else

using Mutex = std::mutex;

#endif

}
