#pragma once

#include <Processors/Executors/Runtime/Pipeline/RoundUpdates.h>
#include <Processors/Executors/Runtime/Pipeline/UpdateInbox.h>
#include <Processors/IProcessor.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>

namespace DB
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

    std::unique_lock<std::mutex> lockRound();

    /// Decisions are made under the round lock; `isFinished` is a lock-free look.
    Status status() const;
    void setExecuting();
    void setIdle();
    void finish();
    bool isFinished() const;

private:
    std::mutex mutex;
    std::atomic<Status> value{Status::Idle};
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
