#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Common/Logger.h>
#include <Common/TransactionID.h>

#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

class IDataPartStorage;
class IMergeTreeDataPart;
class DataPartsAnyLock;
class MergeTreeData;

using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

/// MergeTree-backed bitmap store: one cumulative bitmap per part per commit, written into the part
/// whose transaction produced it and named for the part it kills. A read at `snapshot_csn` takes
/// the highest csn at or below it from an in-memory index, wherever the bytes happen to live.
class MergeTreeBitmapStore : public IBitmapStore
{
public:
    /// `data_` must outlive this; the table owns the store. `cache_` may be null.
    MergeTreeBitmapStore(const MergeTreeData & data_, DeleteBitmapCachePtr cache_);

    BitmapAndVersion readBitmap(const MergeTreePartInfo & part, CSN snapshot_csn) const override;
    ConstDeleteBitmapPtr readLatestBitmap(const MergeTreePartInfo & part) const override;
    std::vector<DeleteBitmapFileOps::BitmapFile> listBitmaps(const MergeTreePartInfo & part) const override;

    size_t removeObsoleteBitmaps(const MergeTreePartInfo & part, CSN oldest_snapshot_csn) override;

    void loadPart(const MergeTreePartInfo & part, const IDataPartStorage & storage) override;
    void dropPart(const IMergeTreeDataPart & part) override;

    void registerStagedBitmaps(const MergeTreePartInfo & holder, const std::vector<MergeTreePartInfo> & targets) override;
    void removeStagedBitmaps(const MergeTreePartInfo & holder, const std::vector<MergeTreePartInfo> & targets) override;

    bool isPinned(const IMergeTreeDataPart & part, const DataPartsAnyLock & lock) const override;
    std::vector<CarriedBitmap> selectCarriedBitmaps(const std::vector<MergeTreePartInfo> & sources) const override;
    void registerLinks(const MergeTreePartInfo & holder, const std::vector<BitmapLink> & links) override;

private:
    /// A bitmap file sits in the part that WROTE it, for good -- nothing moves it afterwards -- so
    /// the index links the two ends and a read follows the link to the bytes.
    ///
    ///     entries               PartEntry of all_1_1_0 (the target)     the files those rows name
    ///     +-----------+         +-----------------------------+
    ///     | all_1_1_0 |-------->| mutex   this part's lock    |
    ///     +-----------+         | outward  = []               |
    ///     | all_5_5_0 |         | inward   = [all_9_9_0]      |--+
    ///     +-----------+         +-----------------------------+  |      all_9_9_0/
    ///     | all_9_9_0 |--+                                       +->      delete_bitmap_for_all_1_1_0.rbm
    ///     +-----------+  |      PartEntry of all_9_9_0 (the holder) |
    ///                    |      +-----------------------------+     |
    ///                    +----->| outward  = [all_1_1_0]      |-----+
    ///                           | inward   = []               |
    ///                           +-----------------------------+

    /// The same link read from the target's end. One field differs from `BitmapLink` and it is
    /// the one that matters: the part named here HOLDS the bitmap. Two types rather than one
    /// alias, so the compiler refuses a holder where a target is wanted -- collapsing them cost
    /// a real bug once.
    struct HeldBy
    {
        MergeTreePartInfo holder;
        CSN csn = 0;
        bool operator==(const HeldBy & other) const = default;
    };

    /// Both vectors hold the same links, from the two ends: `outward` is what this part keeps
    /// for others, `inward` is who keeps this part's.
    struct PartEntry
    {
        std::mutex mutex;

        std::vector<BitmapLink> outward;
        std::vector<HeldBy> inward;
    };
    using PartEntryPtr = std::shared_ptr<PartEntry>;

    mutable std::mutex entries_mutex;
    mutable std::unordered_map<MergeTreePartInfo, PartEntryPtr> entries;

    /// Entry lookup
    PartEntryPtr getOrCreateEntry(const MergeTreePartInfo & part) const;
    PartEntryPtr findEntry(const MergeTreePartInfo & part) const;

    /// The part in {Active, Outdated}, or null. The returned pointer IS the pin on its directory.
    DataPartPtr findPart(const MergeTreePartInfo & info) const;

    /// Remove every link between two parts, or just the one version.
    void removeAllLinks(const MergeTreePartInfo & holder, const MergeTreePartInfo & target);
    void removeLink(const MergeTreePartInfo & holder, const MergeTreePartInfo & target, CSN csn);

    /// What `holder` holds for other parts, and who holds `target`'s.
    std::vector<BitmapLink> getOutwardLinks(const MergeTreePartInfo & holder) const;
    std::vector<HeldBy> getInwardLinks(const MergeTreePartInfo & target) const;

    /// Whether a committed part other than `holder` already carries version `csn` of `target`,
    /// which is what lets `holder` go without losing the kill.
    bool hasPublishedInwardLink(
        const MergeTreePartInfo & target,
        const MergeTreePartInfo & holder,
        CSN csn,
        const DataPartsAnyLock & lock) const;

    /// One version of one part, and which part's directory the bytes are in.
    struct Version
    {
        CSN csn = 0;
        DataPartPtr held_in;
        /// Which of the two names `held_in` filed it under. Recorded rather than derived from
        /// `csn != held_in->creation_csn`: the index is ours to shape here, and a resolver that
        /// guesses cannot tell a wrong name from a missing file.
        bool carried = false;

        /// Where the bytes are filed: a carried name records its version, a staged one leaves it
        /// to whoever holds the file.
        DeleteBitmapFileOps::BitmapFile fileFor(const String & target_name) const
        {
            return {carried ? csn : 0, target_name};
        }
    };

    /// The versions those links point at, csn-resolved.
    std::vector<Version> heldVersions(const std::vector<HeldBy> & links) const;

    /// The version a part's own writes have -- its `creation_csn` -- or nothing while that csn is
    /// not yet a committed one.
    static std::optional<Version> resolveOwnVersion(const DataPartPtr & holder);

    /// The highest version <= `snapshot_csn`
    std::optional<Version> versionAt(const MergeTreePartInfo & part, CSN snapshot_csn) const;

    /// Read one version's bytes, from whichever of the two names its writer filed it under.
    DeleteBitmapPtr readVersion(const IMergeTreeDataPart & part, const Version & version) const;

    LoggerPtr log;
    /// Co-owned with `Context`, so it cannot dangle; null when bitmap caching is off.
    DeleteBitmapCachePtr cache;
    /// Non-owning: the table owns this store.
    const MergeTreeData & data;
};

}
