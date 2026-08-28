#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>

#include <memory>
#include <utility>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;

/// Versioned bitmap persistence + retrieval. The transaction layer hands `(part, snapshot_csn)`
/// and gets back the bitmap visible at that snapshot; it hands `(part, csn, bitmap)` and the
/// bitmap is made durable. Parts are mostly addressed by `MergeTreePartInfo`.
class IBitmapStore
{
public:
    virtual ~IBitmapStore() = default;

    using BitmapAndVersion = std::pair<ConstDeleteBitmapPtr, CSN>;

    /// Read the bitmap whose csn <= snapshot_csn, or an empty bitmap if none exists.
    virtual BitmapAndVersion readBitmap(const MergeTreePartInfo & part, CSN snapshot_csn) const = 0;

    /// The newest committed bitmap
    virtual ConstDeleteBitmapPtr readLatestBitmap(const MergeTreePartInfo & part) const = 0;

    /// Drop each version whose adjacent successor is `<= oldest_snapshot_csn`; newest and staged stay
    virtual size_t removeObsoleteBitmaps(const MergeTreePartInfo & part, CSN oldest_snapshot_csn) = 0;

    /// Index the bitmap files in `part`'s own directory, and return the settled versions this call
    /// added -- announcing the same directory twice adds nothing the second time
    virtual std::vector<CSN> loadPart(const MergeTreePartInfo & part, const IDataPartStorage & storage) = 0;

    /// Record that `owner` holds a staged bitmap for each of `targets`, so reads of those find it
    virtual void registerStagedBitmaps(
        const MergeTreePartInfo & owner, const std::vector<MergeTreePartInfo> & targets) = 0;

    /// The targets `owner` is on the hook for, for a caller that has to know whether it owes a
    /// settle at all before it can decide what its own state means.
    virtual std::vector<MergeTreePartInfo> stagedTargetsOf(const MergeTreePartInfo & owner) const = 0;

    /// Undo the above, for a write whose transaction ROLLED BACK. Only then: the entry is keyed by
    /// target, so the owner's removal never reaches it, but forgetting a committed owner's entry
    /// hides kills that are durable.
    virtual void forgetStagedBitmaps(
        const MergeTreePartInfo & owner, const std::vector<MergeTreePartInfo> & targets) = 0;

    struct SettleReport
    {
        /// Published into the target, or unlinked because the target is provably gone. Nothing left.
        size_t settled = 0;
        /// Left staged: the target is missing while the part set is still loading, so "reclaimed"
        /// and "not loaded yet" are the same observation. Benign only where a retry is free.
        size_t deferred = 0;
        /// Left staged: the settle itself threw, or the file does not name a parseable part.
        size_t failed = 0;
        /// `owner`'s staged set could not be enumerated at all -- unresolvable part, or a failed
        /// listing -- so the three counts above say nothing about it. Its own field rather than a
        /// `failed` of unknown size, because that count would be a guess.
        bool owner_unresolved = false;

        /// Everything a retire path must refuse to proceed over.
        bool anyOutstanding() const { return owner_unresolved || deferred > 0 || failed > 0; }

        void add(const SettleReport & other)
        {
            settled += other.settled;
            deferred += other.deferred;
            failed += other.failed;
            owner_unresolved |= other.owner_unresolved;
        }
    };

    /// Publish exactly the bitmaps `owner` staged for `targets` into those targets at `csn`, and
    /// unlink each staged copy. For the caller that did the staging: settles overlap, and a caller
    /// that settled the whole owner would report work another settler did.
    virtual SettleReport settleStagedBitmaps(
        const MergeTreePartInfo & owner, const std::vector<MergeTreePartInfo> & targets, CSN csn) = 0;

    /// The same, for every target the index has for `owner`. For a caller holding a part and no
    /// staging record -- a retire path, which must drain the owner completely, and recovery, whose
    /// staging call is in a dead process.
    virtual SettleReport settleStagedBitmaps(const MergeTreePartInfo & owner, CSN csn) = 0;

    /// The files in `part`'s own directory, sorted by version. `system.parts` introspection, NOT a resolution
    virtual std::vector<DeleteBitmapFileOps::BitmapFile> listBitmaps(const MergeTreePartInfo & part) const = 0;

    /// Forget the index entry and the cached bitmaps of a part that is no longer in
    /// {Active, Outdated}.
    virtual void dropPart(const IMergeTreeDataPart & part) = 0;
};

using BitmapStorePtr = std::shared_ptr<IBitmapStore>;

}
