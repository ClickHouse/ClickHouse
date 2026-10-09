#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Common/AllocatorWithMemoryTracking.h>

#include <boost/container/devector.hpp>

#include <span>

namespace DB
{

namespace Runtime::V2
{

struct ExecutingProcessor;

class GlobalQueue
{
public:
    GlobalQueue();

    void pushManyFifo(std::span<ExecutingProcessor * const> many);
    size_t popManyFifo(std::span<ExecutingProcessor *> out, size_t sharers);
    size_t size() const;

private:
    Wyrd::Mutex mutex;
    boost::container::devector<ExecutingProcessor *, AllocatorWithMemoryTracking<ExecutingProcessor *>> entries;
    Wyrd::Atomic<size_t> count = 0;
};

}

}
