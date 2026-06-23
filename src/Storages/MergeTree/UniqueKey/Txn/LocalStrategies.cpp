#include <Storages/MergeTree/UniqueKey/Txn/LocalStrategies.h>

#include <algorithm>
#include <utility>

namespace DB::UniqueKeyTxn
{

// ============================================================================
// LocalCommitCoordinator
// ============================================================================

LocalCommitCoordinator::LocalCommitCoordinator() = default;

LocalCommitCoordinator::PreparedCommitSnapshot LocalCommitCoordinator::readSnapshot()
{
    std::lock_guard lock(commit_lock);

    PreparedCommitSnapshot snap;
    snap.csn = csn;
    return snap;
}

CSN LocalCommitCoordinator::attemptCommit(PublishAction staging)
{
    std::lock_guard lock(commit_lock);

    const CSN tentative = csn + 1;

    if (staging)
        staging(tentative);

    csn = tentative;
    return tentative;
}

CSN LocalCommitCoordinator::currentCsn() const
{
    std::lock_guard lock(commit_lock);
    return csn;
}

void LocalCommitCoordinator::seedCsn(CSN floor)
{
    std::lock_guard lock(commit_lock);
    csn = std::max(csn, floor);
}

void LocalCommitCoordinator::withinSnapshotRegion(std::function<void(CSN)> fn)
{
    std::lock_guard lock(commit_lock);
    fn(csn);
}

// ============================================================================
// LocalPinRegistry
// ============================================================================

std::shared_ptr<IPinRegistry::PinHandle> LocalPinRegistry::acquire(CSN csn)
{
    {
        std::lock_guard lock(pins_mutex);
        ++pins[csn];
    }

    /// Capture a self-pointer for the release callback. The PinHandle
    /// owns the closure; when it's destroyed (last shared_ptr ref drops),
    /// the closure fires and decrements / removes the entry.
    auto release = [this, csn]
    {
        std::lock_guard lock(pins_mutex);
        auto it = pins.find(csn);
        if (it == pins.end())
            return; /// double-release guarded
        if (--it->second == 0)
            pins.erase(it);
    };

    return makeHandle(csn, std::move(release));
}

CSN LocalPinRegistry::clusterFloor()
{
    std::lock_guard lock(pins_mutex);
    if (pins.empty())
        return MAX_CSN;
    CSN floor = MAX_CSN;
    for (const auto & [csn, _] : pins)
        floor = std::min(floor, csn);
    return floor;
}

size_t LocalPinRegistry::totalPinCount() const
{
    std::lock_guard lock(pins_mutex);
    size_t total = 0;
    for (const auto & [_, count] : pins)
        total += count;
    return total;
}

}
