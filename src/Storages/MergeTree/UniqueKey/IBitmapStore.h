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
class DataPartsAnyLock;

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

    /// Index the bitmap files `part` holds. Announcing the same directory twice adds nothing the
    /// second time.
    virtual void loadPart(const MergeTreePartInfo & part, const IDataPartStorage & storage) = 0;

    /// Forget the index entry and the cached bitmaps of a part that is no longer in
    /// {Active, Outdated}.
    virtual void dropPart(const IMergeTreeDataPart & part) = 0;

    /// Record that `holder` holds a staged bitmap for each of `targets`, so reads of those find it
    virtual void registerStagedBitmaps(
        const MergeTreePartInfo & holder, const std::vector<MergeTreePartInfo> & targets) = 0;

    /// Undo the two registrations above, for a write whose transaction ROLLED BACK. Only then:
    /// the entry is keyed by target, so the holder's removal never reaches it, but forgetting a
    /// committed holder's entry hides kills that are durable.
    virtual void removeStagedBitmaps(
        const MergeTreePartInfo & holder, const std::vector<MergeTreePartInfo> & targets) = 0;

    /// One bitmap link: the part it kills into, and at which version. A bitmap sits in the part
    /// that WROTE it, which need not be the target.
    ///
    /// `csn == 0` means the holder wrote it, so the version is the holder's own `creation_csn` --
    /// unknown until the commit point, which is why the file name leaves it out. Non-zero means a
    /// merge carried it in, and the name records the version it arrived with.
    struct BitmapLink
    {
        MergeTreePartInfo target;
        CSN csn = 0;

        bool operator==(const BitmapLink & other) const = default;
    };

    /// One file a merge has to copy into its result: the link it will hold afterwards, and
    /// where the bytes are until then. `held_in` is a live handle on the source part, so the
    /// directory the copy reads from cannot be reclaimed while this is in hand.
    struct CarriedBitmap
    {
        BitmapLink link;
        std::shared_ptr<const IMergeTreeDataPart> held_in;
        DeleteBitmapFileOps::BitmapFile file;
    };

    /// The bitmaps a merge has to copy into its result: everything its retiring `sources` hold
    /// for a target that is NOT itself being merged. Throws if a source cannot be resolved -- it
    /// may hold kills, and a merge that silently skips them resurrects rows.
    virtual std::vector<CarriedBitmap> selectCarriedBitmaps(const std::vector<MergeTreePartInfo> & sources) const = 0;

    /// Record that `holder` now holds each of `links`, at the version each already had. Both
    /// directions; links already there are skipped.
    virtual void registerLinks(const MergeTreePartInfo & holder, const std::vector<BitmapLink> & links) = 0;

    /// Whether removing `part` would take the only copy of some other part's kills with it.
    /// Answering resolves parts, and `grabOldParts` asks while holding the parts lock, so the
    /// caller passes the lock it has rather than letting this take a second one.
    virtual bool isPinned(const IMergeTreeDataPart & part, const DataPartsAnyLock & lock) const = 0;

    /// The files in `part`'s own directory, sorted by version. `system.parts` introspection, NOT a resolution
    virtual std::vector<DeleteBitmapFileOps::BitmapFile> listBitmaps(const MergeTreePartInfo & part) const = 0;
};

using BitmapStorePtr = std::shared_ptr<IBitmapStore>;

}
