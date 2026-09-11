#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Common/HashTable/Hash.h>
#include <Common/Logger.h>
#include <Common/TransactionID.h>

#include <absl/container/flat_hash_set.h>

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

    /// A link from the target's end. Two types rather than one alias, so the compiler refuses a
    /// holder where a target is wanted -- collapsing them cost a real bug once. Its `csn` gets
    /// resolved once the holder commits, so `carried` has to say the file name separately.
    struct HeldBy
    {
        MergeTreePartInfo holder;
        CSN csn = Tx::UnknownCSN;   /// the version, once `holder` has committed
        bool carried = false;       /// which of the two file names the bytes have

        bool operator==(const HeldBy & other) const = default;
    };

    /// `BitmapLink` is interface currency, so its hash lives with the index that needs one.
    struct HashBitmapLink
    {
        size_t operator()(const BitmapLink & link) const
        {
            return intHashCRC32(link.csn, std::hash<MergeTreePartInfo>{}(link.target));
        }
    };
    using OutwardLinks = absl::flat_hash_set<BitmapLink, HashBitmapLink>;

    struct ByCsn
    {
        /// A link with no csn yet sorts above every snapshot -- sorting it low would let
        /// `upper_bound` return a stale version instead. Deliberately `Tx::RolledBackCSN`, which
        /// is one above `UNBOUNDED_CSN` and is what a rolled-back holder resolves to anyway.
        static constexpr CSN UNKNOWN_CSN_ORDER = Tx::RolledBackCSN;
        static CSN orderOf(const HeldBy & link) { return link.csn == Tx::UnknownCSN ? UNKNOWN_CSN_ORDER : link.csn; }

        bool operator()(const HeldBy & a, const HeldBy & b) const
        {
            const CSN ka = orderOf(a);
            const CSN kb = orderOf(b);
            return ka != kb ? ka < kb : a.holder < b.holder;
        }
        bool operator()(const HeldBy & a, CSN b) const { return orderOf(a) < b; }
        bool operator()(CSN a, const HeldBy & b) const { return a < orderOf(b); }
    };

    /// The same links from the two ends: `inward` sorted by csn, `outward` keyed by the link
    /// itself -- the sweep removes one per obsolete version, and scanning for it there was quadratic.
    struct PartEntry
    {
        std::mutex mutex;

        OutwardLinks outward;
        std::vector<HeldBy> inward;
    };
    using PartEntryPtr = std::shared_ptr<PartEntry>;

    mutable std::mutex entries_mutex;
    mutable std::unordered_map<MergeTreePartInfo, PartEntryPtr> entries;

    /// Entry lookup
    PartEntryPtr getOrCreateEntry(const MergeTreePartInfo & part) const;
    PartEntryPtr findEntry(const MergeTreePartInfo & part) const;

    /// The part in {Active, Outdated}, or null. The returned pointer IS the pin on its directory.
    /// `lock` is the caller's parts lock where it holds one -- taking a second is a self-deadlock.
    DataPartPtr findPart(const MergeTreePartInfo & info, const DataPartsAnyLock * lock = nullptr) const;

    /// What a caller wants done with a link whose holder has left the part set. The read path
    /// says the index is corrupt; the pin says let go, because throwing would hold the part for
    /// good. Not derivable from `lock`: those two happen to differ there as well, today.
    enum class OnMissingHolder
    {
        Throw,
        Skip,
    };

    /// Give each link with no csn yet the one its holder committed at, and move it into place.
    /// Runs before every ordered search: a link left at the top is one a reader above its
    /// version misses.
    void resolveUnknownVersions(PartEntry & entry, const DataPartsAnyLock * lock, OnMissingHolder on_missing) const;

    static void addInwardLink(PartEntry & entry, const HeldBy & link);

    /// Every link between two parts. Scans `inward` by holder, which the csn order cannot answer.
    void removeAllLinks(const MergeTreePartInfo & holder, const MergeTreePartInfo & target);
    /// `csn` is what the outward side records: 0 for a staged link, resolved or not.
    void removeOutwardLink(const MergeTreePartInfo & holder, const MergeTreePartInfo & target, CSN csn);

    std::vector<BitmapLink> getOutwardLinks(const MergeTreePartInfo & holder) const;

    /// Whether a committed part other than `holder` already carries version `csn` of `target`,
    /// which is what lets `holder` go without losing the kill.
    bool hasPublishedInwardLink(
        const MergeTreePartInfo & target,
        const MergeTreePartInfo & holder,
        CSN csn,
        const DataPartsAnyLock & lock) const;

    /// One version of one part, and a live handle on the part holding the bytes. The handle is
    /// the point: `removePartsFinally` takes a part out of the set BEFORE `dropUniqueKeyBitmaps`
    /// drops its links, so a winner chosen without one can be retired before it is read.
    struct Version
    {
        CSN csn = Tx::UnknownCSN;
        DataPartPtr held_in;
        bool carried = false;

        /// Where the bytes are filed: a carried name records its version, a staged one leaves it
        /// to whoever holds the file.
        DeleteBitmapFileOps::BitmapFile fileFor(const String & target_name) const
        {
            return {carried ? csn : 0, target_name};
        }
    };

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
