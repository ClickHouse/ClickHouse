#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/Updates/RoundUpdates.h>
#include <Processors/Executors/Runtime/V2/Pipeline/Updates/UpdateInbox.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/IProcessor.h>

#include <mutex>
#include <optional>
#include <string>

namespace DB
{

namespace Runtime::V2
{

struct ExecutingProcessor
{
    enum class Status : UInt8
    {
        Idle,
        Prepare,
        Work,
        Async,
        AsyncReady,
        UpdatePipeline,
        Finished,
    };

    void setProcessor(IProcessor & processor_);
    std::unique_lock<Wyrd::Mutex> lockRound();

    IProcessor * processor = nullptr;
    Wyrd::Mutex round_mutex;
    Wyrd::Atomic<Status> status = Status::Idle;
    RoundUpdates round_updates;
    UpdateInbox incoming_updates;
    std::optional<IProcessor::Status> last_status;
    UInt64 work_ns = 0;
    size_t siblings = 1;
};

}

}
