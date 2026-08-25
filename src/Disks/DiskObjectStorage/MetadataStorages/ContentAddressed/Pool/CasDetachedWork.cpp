#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasDetachedWork.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>

namespace DB::Cas
{

bool DetachedStopToken::stopping() const
{
    std::lock_guard lock(state->mutex);
    return state->stopping;
}

struct DetachedTaskLease::Completion
{
    std::shared_ptr<Pool> owner;
    std::shared_ptr<DetachedRegistryState> state;
    std::function<void()> release_hook;
    bool armed = false;

    ~Completion()
    {
        /// 1. Drop the pool reference FIRST. A waiter that sees the count reach zero below must be
        /// able to conclude that no tracked task holds the pool any more.
        owner.reset();
        if (!armed)
            return;
        if (release_hook)
            release_hook();
        /// 2. Only then account for the completion.
        {
            std::lock_guard lock(state->mutex);
            --state->in_flight;
        }
        state->cv.notify_all();
    }
};

DetachedTaskLease::DetachedTaskLease(
    std::shared_ptr<Pool> owner, std::shared_ptr<DetachedRegistryState> state, std::function<void()> release_hook)
    : completion(std::make_shared<Completion>())
{
    completion->owner = std::move(owner);
    completion->state = std::move(state);
    completion->release_hook = std::move(release_hook);
}

void DetachedTaskLease::arm()
{
    completion->armed = true;
}

}
