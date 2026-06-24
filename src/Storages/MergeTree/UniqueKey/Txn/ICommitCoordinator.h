#pragma once

#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>

#include <functional>

namespace DB::UniqueKeyTxn
{

/// Linearization primitive. Composes with `IBitmapStore` (durable PUTs run
/// before `attemptCommit`); `withinSnapshotRegion` exposes the same
/// linearization to the reader's `(parts, csn, pin)` capture.
///
/// The Local coordinator is pessimistic: it serializes through `commit_lock`,
/// so `attemptCommit` always assigns and returns a csn — it never reports a
/// conflict, only throws on durable I/O failure.
///
/// TODO(unique-key): an optimistic Shared coordinator (e.g. Keeper-CAS) will
/// reintroduce a prepared-snapshot parameter on `attemptCommit` (the CAS
/// input), a failure return to signal CAS loss, and a caller-side
/// read→prepare→retry loop in `PartitionTxnController::commit`. None of that is
/// needed today: the Local coordinator holds `commit_lock` across the publish
/// and never loses, so the interface is single-attempt and infallible-or-throw.
class ICommitCoordinator
{
public:
    virtual ~ICommitCoordinator() = default;

    /// Consistent-view token. Carries the csn observed; payloads are prepared
    /// against this csn before the publish.
    struct PreparedCommitSnapshot
    {
        CSN csn = INVALID_CSN;
    };

    /// Caller-supplied staging callable executed INSIDE the linearization.
    /// Performs the publish step appropriate to the engine: on Local,
    /// `rename(tmp → active) + fsync(active/)`; on Shared, embedded in the
    /// Keeper multi-op (no work in this lambda — the coordinator does it).
    /// MUST be exception-safe; a throw aborts the commit and the publish
    /// region's lock is released.
    using PublishAction = std::function<void(CSN tentative_csn)>;

    /// Capture the read csn for payload preparation. Cheap; safe to call
    /// outside any external lock.
    virtual PreparedCommitSnapshot readSnapshot() = 0;

    /// Linearize the publish step. The staging callable runs INSIDE the
    /// linearization; the assigned csn is returned. Throws on durable I/O
    /// failure (rename, fsync). Concurrent calls are safe; the coordinator
    /// serializes internally.
    virtual CSN attemptCommit(PublishAction staging) = 0;

    /// Run `fn(csn)` inside the same linearization region as `attemptCommit`'s
    /// publish + csn bump, so a reader reading `(parts, csn)` AND installing a
    /// pin under `fn` observes one commit boundary. `fn` MUST NOT re-enter the
    /// coordinator (the region is not reentrant).
    virtual void withinSnapshotRegion(std::function<void(CSN)> fn) = 0;
};

}
