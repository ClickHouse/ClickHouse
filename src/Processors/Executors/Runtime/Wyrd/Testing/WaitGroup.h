#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>

#include <base/types.h>

#include <functional>
#include <vector>

namespace DB::Runtime::Wyrd
{

/// A group of threads that start together on join and are joined together; the body gets the thread's index.
class WaitGroup
{
public:
    WaitGroup & add(size_t count, std::function<void(size_t)> body);
    void join();

private:
    Atomic<UInt32> started{0};
    std::vector<Thread> threads;
};

}
