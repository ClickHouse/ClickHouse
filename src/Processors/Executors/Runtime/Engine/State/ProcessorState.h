#pragma once

#include <Processors/Executors/Runtime/Engine/State/RoundUpdates.h>
#include <Processors/Executors/Runtime/Engine/State/UpdateInbox.h>
#include <Processors/IProcessor.h>

#include <atomic>
#include <cstdint>
#include <optional>

namespace DB
{

class ProcessorLock
{
public:
    /// Unlock fails if a notification arrived since the snapshot, then the owner runs another round.
    uint64_t snapshot() const;
    bool tryUnlock(uint64_t snapshot);
    bool tryLock();

    /// Unlocked, and no `tryLock` succeeds again.
    bool isFinished() const;
    void finish();

    /// Counts a pushed hint; the notifier then calls `tryLock` to see who runs `prepare`.
    void notify();

private:
    std::atomic<uint64_t> word{0};
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
