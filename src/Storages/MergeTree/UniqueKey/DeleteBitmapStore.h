#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Common/HashTable/Hash.h>
#include <Common/Logger.h>
#include <Common/TransactionID.h>

#include <absl/container/flat_hash_set.h>

#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{

/// A part's version is settled once it can no longer change: either its transaction committed, or
/// it was never written in one. `Tx::isCommittedCSN` answers only the first -- it requires
/// `csn > MaxReservedCSN`, and `NonTransactionalCSN` is 1 -- so a bitmap held by an attached or
/// non-transactional part would never resolve.
inline bool isSettledCSN(CSN csn)
{
    return Tx::isCommittedCSN(csn) || csn == Tx::NonTransactionalCSN;
}

class IDataPartStorage;
class IMergeTreeDataPart;
class DataPartsAnyLock;
class MergeTreeData;

using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

/// Versioned persistence and lookup of the delete bitmaps of a unique-key table.
class DeleteBitmapStore
{
public:
    DeleteBitmapStore(const MergeTreeData & data_, DeleteBitmapCachePtr cache_);

    using BitmapAndVersion = std::pair<ConstDeleteBitmapPtr, CSN>;

    /// Read the bitmap whose csn <= snapshot_csn, or an empty bitmap if none exists.
    BitmapAndVersion readBitmap(const MergeTreePartInfo & part, CSN snapshot_csn) const;

    /// The newest committed bitmap
    ConstDeleteBitmapPtr readLatestBitmap(const MergeTreePartInfo & part) const;

    /// The files in `part`'s own directory, sorted by version. `system.parts` introspection, NOT a resolution
    std::vector<DeleteBitmapFileOps::BitmapFile> listBitmaps(const MergeTreePartInfo & part) const;

    /// Index the bitmap files `part` holds. Announcing the same directory twice adds nothing the
    /// second time.
    void loadPart(const MergeTreePartInfo & part, const IDataPartStorage & storage);

    /// Forget the index entry and the cached bitmaps of a part that is no longer in
    /// {Active, Outdated}.
    void dropPart(const IMergeTreeDataPart & part);

    /// Record that `holder` holds a staged bitmap for each of `targets`, so reads of those find it
    void registerStagedBitmaps(const MergeTreePartInfo & holder, const std::vector<MergeTreePartInfo> & targets);

    /// Undo the two registrations above, for a write whose transaction ROLLED BACK. Only then:
    /// the entry is keyed by target, so the holder's removal never reaches it, but forgetting a
    /// committed holder's entry hides kills that are durable.
    void removeStagedBitmaps(const MergeTreePartInfo & holder, const std::vector<MergeTreePartInfo> & targets);

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

    /// Whether removing `part` would take the only copy of some other part's kills with it.
    /// Answering resolves parts, and `grabOldParts` asks while holding the parts lock, so the
    /// caller passes the lock it has rather than letting this take a second one.
    bool isPinned(const IMergeTreeDataPart & part, const DataPartsAnyLock & lock) const;

    /// The bitmaps a merge has to copy into its result: everything its retiring `sources` hold
    /// for a target that is NOT itself being merged. Throws if a source cannot be resolved -- it
    /// may hold kills, and a merge that silently skips them resurrects rows.
    std::vector<CarriedBitmap> selectCarriedBitmaps(const std::vector<MergeTreePartInfo> & sources) const;

    /// Record that `holder` now holds each of `links`, at the version each already had. Both
    /// directions; links already there are skipped.
    void registerLinks(const MergeTreePartInfo & holder, const std::vector<BitmapLink> & links);

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

    /// `BitmapLink` is public currency, so its hash lives with the index that needs one.
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
    /// itself, so registering a link already there is a no-op instead of a duplicate.
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

    /// Every version <= `snapshot_csn`, oldest first. A version is a delta, so the read owes the
    /// union of all of them and not just the newest.
    std::vector<Version> versionsUpTo(const MergeTreePartInfo & part, CSN snapshot_csn) const;

    /// Read one version's bytes, from whichever of the two names its writer filed it under.
    DeleteBitmapPtr readVersion(const IMergeTreeDataPart & part, const Version & version) const;

    LoggerPtr log;
    /// Co-owned with `Context`, so it cannot dangle; null when bitmap caching is off.
    DeleteBitmapCachePtr cache;
    /// Non-owning: the table owns this store.
    const MergeTreeData & data;
};

using DeleteBitmapStorePtr = std::shared_ptr<DeleteBitmapStore>;

}
