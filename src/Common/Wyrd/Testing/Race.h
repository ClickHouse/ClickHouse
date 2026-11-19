#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>

#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <functional>

namespace DB::Runtime::Wyrd
{

/// Threads released at one instant by run
class Race
{
public:
    Race & add(size_t count, std::function<void(size_t)> body);
    void run();

private:
    Atomic<UInt32> started{0};
    VectorWithMemoryTracking<Thread> threads;
};

}
