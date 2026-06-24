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

/// Local MergeTree implementation: `writer_mutex` + `commit_lock` + in-memory
/// `csn`. `attemptCommit` runs `staging(tentative_csn)` (engine-specific
/// publish: rename + fsync(active/)) under `commit_lock`, then bumps `csn`.
/// Pessimistic: it always assigns a csn under the lock, never reporting a
/// conflict.
///
/// Two distinct locks, both required (see `Txn/README.md` for the protocol):
///   - `writer_mutex` (OUTER, writers only): `lockForWrite` hands a held guard
///     to the caller, which holds it across the whole write statement so two
///     concurrent writers in the same partition can't both read `prev` bitmaps
///     before either publishes and lost-update.
///   - `commit_lock` (INNER): linearizes a single commit's publish and a
///     lock-free reader's snapshot capture (`withinSnapshotRegion`). Readers
///     take ONLY this lock, never `writer_mutex`.
class LocalCommitCoordinator : public ICommitCoordinator
{
public:
    LocalCommitCoordinator();

    PreparedCommitSnapshot readSnapshot() override;
    CSN attemptCommit(PublishAction staging) override;

    /// Recovery seed: lift `partition.csn` to `max(creation_csn, last
    /// CSN-named bitmap version)` across active parts. Monotone — a `std::max`,
    /// so the seed never lowers the in-memory csn.
    void seedCsn(CSN floor);

    void withinSnapshotRegion(std::function<void(CSN)> fn) override;

    std::unique_lock<std::mutex> lockForWrite() override { return std::unique_lock(writer_mutex); }

private:
    /// Per-partition writer lock (OUTER). Serializes whole write statements so
    /// the read-prev → publish window is not interleaved across writers.
    /// Distinct from `commit_lock`: held by the CALLER across the statement,
    /// never re-entered inside this class.
    std::mutex writer_mutex;

    /// Linearizes a single commit's publish and a lock-free reader's snapshot
    /// capture (INNER). Distinct from `writer_mutex`, which serializes whole
    /// writer statements.
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

private:
    mutable std::mutex pins_mutex;
    std::unordered_map<CSN, UInt32> pins;
};

}
