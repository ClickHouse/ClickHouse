#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/Updates/RoundUpdates.h>
#include <Processors/Executors/Runtime/V2/Pipeline/Updates/UpdateInbox.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/IProcessor.h>

#include <cstdint>
#include <mutex>
#include <string>
#include <optional>

namespace DB
{

namespace Runtime::V2
{

class ProcessorLock
{
public:
    enum class Status : uint8_t
    {
        Idle,
        Executing,
        Finished,
    };

    void setName(std::string name);
    std::unique_lock<Wyrd::Mutex> lockRound();

    /// Decisions are made under the round lock; `isFinished` is a lock-free look.
    Status status() const;
    void setExecuting();
    void setIdle();
    void finish();
    bool isFinished() const;

private:
    Wyrd::Mutex mutex;
    Wyrd::Atomic<Status> value{Status::Idle};
};

struct ProcessorState
{
    IProcessor * processor = nullptr;
    ProcessorLock lock;
    RoundUpdates round_updates;
    UpdateInbox incoming_updates;
    std::optional<IProcessor::Status> last_status;
};

}

}
