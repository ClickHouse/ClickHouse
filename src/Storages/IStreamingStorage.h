#pragma once

#include <Storages/IStorage.h>
#include <Storages/StreamingBackgroundControl.h>
#include <Interpreters/DatabaseCatalog.h>

#include <atomic>

namespace DB
{

namespace ActionLocks
{
    extern const StorageActionBlockType StreamConsume;
}

/// Shared implementation of background task controls for streaming storages
class IStreamingStorage : public IStorage
{
public:
    using IStorage::IStorage;

    bool isStreamingStorage() const final { return true; }

    /// STOP/PAUSE: block future cycles
    ActionLock getActionLock(StorageActionBlockType action_type) override
    {
        if (action_type == ActionLocks::StreamConsume)
            return stream_control.block();
        return {};
    }

    /// START: resume promptly rather than waiting for the next scheduled wake-up.
    void onActionLockRemove(StorageActionBlockType action_type) override
    {
        if (action_type == ActionLocks::StreamConsume)
            scheduleStreamingTasks();
    }

    /// REFRESH: run exactly one out-of-order cycle now, even while blocked.
    void refreshBackgroundActivity() override
    {
        if (DatabaseCatalog::instance().getDependentViews(getStorageID()).empty())
            return;
        stream_control.requestRefreshOnce();
        scheduleStreamingTasks();
    }

    /// CANCEL/STOP: abort the in-flight cycle before its durable boundary.
    void cancelBackgroundActivity() override
    {
        stream_control.requestCancel();
    }

    /// Called by the engine's source to observe the cancel epoch.
    UInt64 currentCancelEpoch() const { return stream_control.currentCancelEpoch(); }
    bool isConsumeCancelRequested(UInt64 epoch_snapshot) const { return stream_control.isCancelRequested(epoch_snapshot); }

protected:
    StreamingBackgroundControl stream_control;

    /// Set when storage starts shutting down, so background tasks finish asap.
    std::atomic<bool> shutdown_called{false};

private:
    /// Schedule engine streaming task(s) for one out-of-order run, unless shutting down.
    void scheduleStreamingTasks()
    {
        if (shutdown_called)
            return;
        scheduleStreamingTasksImpl();
    }

    /// Schedule engine background task holder(s). The only behavior that differs between engines.
    virtual void scheduleStreamingTasksImpl() = 0;
};

}
