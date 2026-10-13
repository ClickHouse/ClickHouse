#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Common/DevectorWithMemoryTracking.h>

#include <span>

namespace DB
{

namespace Runtime::V2
{

class GlobalQueue
{
public:
    explicit GlobalQueue(size_t capacity);

    void pushManyFifo(std::span<ExecutingProcessor * const> many);
    size_t popManyFifo(std::span<ExecutingProcessor *> out, size_t sharers);
    size_t size() const;

private:
    Wyrd::Mutex mutex;
    DevectorWithMemoryTracking<ExecutingProcessor *> entries;
    Wyrd::Atomic<size_t> count = 0;
};

}

}
