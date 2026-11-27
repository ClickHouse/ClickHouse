#pragma once

#include <Common/Wyrd/Facade/Atomic.h>
#include <Common/Wyrd/Facade/Thread.h>

#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <functional>

namespace DB::Wyrd
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
