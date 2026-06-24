#pragma once

#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>

#include <memory>
#include <utility>

namespace DB
{
    class DeleteBitmap;
}

namespace DB::UniqueKeyTxn
{

/// Versioned bitmap persistence + retrieval — the Local-vs-Shared strategy
/// seam. The transaction layer hands `(part, snapshot_csn)` and gets back
/// the bitmap visible at that snapshot; it hands `(part, csn, bitmap)` and
/// the bitmap is made durable. The Local implementation is
/// `MergeTreeBitmapStore`; a Shared (S3-backed) one is planned.
class IBitmapStore
{
public:
    virtual ~IBitmapStore() = default;

    /// Pick the bitmap visible at `snapshot_csn` on `part`: the file with
    /// `max csn ≤ snapshot_csn`, or `(empty non-null bitmap, 0)` if none.
    /// Returned csn is the chosen bitmap's csn. Bitmap is `const` (shared
    /// across readers; writers copy-mutate). Throws on storage failure.
    /// Thread safety: concurrent calls safe.
    virtual std::pair<ConstDeleteBitmapPtr, CSN>
        readBitmap(const PartName & part, CSN snapshot_csn) = 0;

    /// Install the cumulative bitmap (`prev ∪ new_kills`) for `target` at
    /// `csn`. Durable on return but NOT visible until `partition.csn` passes
    /// `csn`. `csn` MUST be strictly greater than every previously installed
    /// version for the part (monotonicity; LOGICAL_ERROR otherwise). On a
    /// failed commit the orphan is reclaimed by recovery's tmp-scan per the
    /// manifest's `bitmaps_created`.
    virtual void installBitmap(const PartName & target, CSN csn, const DeleteBitmap & bitmap) = 0;

    /// Precisely remove the `(target, csn)` bitmap. Idempotent. Invalidates
    /// any bitmap cache. Used by commit rollback + recovery tmp-scan to drop
    /// an orphan named in the aborted commit's `bitmaps_created`.
    virtual void removeBitmap(const PartName & target, CSN csn) = 0;
};

}
