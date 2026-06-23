#pragma once

#include <Storages/MergeTree/UniqueKey/Txn/ICommitCoordinator.h>
#include <Storages/MergeTree/UniqueKey/Txn/SnapshotPinning.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>

#include <base/types.h>

#include <functional>
#include <mutex>
#include <unordered_map>

namespace DB::UniqueKeyTxn
{

/// Local MergeTree implementation: `commit_lock` + in-memory `csn`.
/// `attemptCommit` runs `staging(tentative_csn)` (engine-specific publish:
/// rename + fsync(active/)) under the mutex, then bumps `csn`. Pessimistic:
/// it always assigns a csn under the lock, never reporting a conflict.
///
/// `commit_lock` vs the storage-level `UniqueKeyPartitionMutex`: distinct
/// locks, both required. See `Txn/README.md` for the protocol.
class LocalCommitCoordinator : public ICommitCoordinator
{
public:
    LocalCommitCoordinator();

    PreparedCommitSnapshot readSnapshot() override;
    CSN attemptCommit(PublishAction staging) override;

    /// Diagnostic — current csn under the mutex. Tests use this.
    CSN currentCsn() const;

    /// Recovery seed: lift `partition.csn` to `max(creation_csn, last
    /// CSN-named bitmap version)` across active parts. Monotone — a `std::max`,
    /// so the seed never lowers the in-memory csn.
    void seedCsn(CSN floor);

    void withinSnapshotRegion(std::function<void(CSN)> fn) override;

private:
    /// Linearizes a commit's publish and a lock-free reader's snapshot capture;
    /// distinct from the storage-level `UniqueKeyPartitionMutex` that serializes
    /// whole writer statements.
    mutable std::mutex commit_lock;
    CSN csn = INVALID_CSN;
};

/// Local IPinRegistry. `std::unordered_map<CSN, UInt32>` under `pins_mutex`;
/// `clusterFloor` min-scans the keys.
class LocalPinRegistry : public IPinRegistry
{
public:
    LocalPinRegistry() = default;

    std::shared_ptr<PinHandle> acquire(CSN csn) override;
    CSN  clusterFloor() override;

    /// Diagnostic — active pin count. Tests use this.
    size_t totalPinCount() const;

private:
    mutable std::mutex pins_mutex;
    std::unordered_map<CSN, UInt32> pins;
};

}
