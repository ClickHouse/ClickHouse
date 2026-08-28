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
class MergeTreeData;

using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

/// MergeTree-backed bitmap store: one cumulative bitmap per part per commit, settled in that part's
/// own directory as `delete_bitmap_<csn>.rbm`. A read at `snapshot_csn` takes the highest csn at or
/// below it from an in-memory index.
class MergeTreeBitmapStore : public IBitmapStore
{
public:
    /// `data_` must outlive this; the table owns the store. `cache_` may be null.
    MergeTreeBitmapStore(const MergeTreeData & data_, DeleteBitmapCachePtr cache_);

    BitmapAndVersion readBitmap(const MergeTreePartInfo & part, CSN snapshot_csn) const override;
    ConstDeleteBitmapPtr readLatestBitmap(const MergeTreePartInfo & part) const override;
    std::vector<DeleteBitmapFileOps::BitmapFile> listBitmaps(const MergeTreePartInfo & part) const override;

    size_t removeObsoleteBitmaps(const MergeTreePartInfo & part, CSN oldest_snapshot_csn) override;

    std::vector<CSN> loadPart(const MergeTreePartInfo & part, const IDataPartStorage & storage) override;
    void dropPart(const IMergeTreeDataPart & part) override;

    void registerStagedBitmaps(const MergeTreePartInfo & owner, const std::vector<MergeTreePartInfo> & targets) override;
    std::vector<MergeTreePartInfo> stagedTargetsOf(const MergeTreePartInfo & owner) const override;
    SettleReport settleStagedBitmaps(
        const MergeTreePartInfo & owner, const std::vector<MergeTreePartInfo> & targets, CSN csn) override;
    SettleReport settleStagedBitmaps(const MergeTreePartInfo & owner, CSN csn) override;
    void forgetStagedBitmaps(const MergeTreePartInfo & owner, const std::vector<MergeTreePartInfo> & targets) override;

private:
    /// A bitmap file stages at the owner part until it's settled, so we use bi-directional links to find it
    ///
    ///     entries               PartEntry of all_1_1_0 (target)         the files those rows name
    ///     +-----------+         +-----------------------------+
    ///     | all_1_1_0 |-------->| mutex   this part's lock    |         all_1_1_0/
    ///     +-----------+         | settled       = [7, 12] ----|-->        delete_bitmap_7.rbm
    ///     | all_5_5_0 |         |                             |-->        delete_bitmap_12.rbm
    ///     +-----------+         | staged_owners = [all_9_9_0] |--+
    ///     | all_9_9_0 |--+      | staged_for    = []          |  |      all_9_9_0/
    ///     +-----------+  |      +-----------------------------+  +->      delete_bitmap_for_all_1_1_0.rbm
    ///                    |                                       |
    ///                    |      PartEntry of all_9_9_0 (owner)   |
    ///                    |      +-----------------------------+  |
    ///                    +----->| settled       = []          |  |
    ///                           | staged_owners = []          |  |
    ///                           | staged_for    = [all_1_1_0] |--+
    ///                           +-----------------------------+
    struct PartEntry
    {
        std::mutex mutex;
        /// Ascending csns of the settled files in the part's own directory.
        std::vector<CSN> settled;

        std::vector<MergeTreePartInfo> staged_owners;
        std::vector<MergeTreePartInfo> staged_for;
    };
    using PartEntryPtr = std::shared_ptr<PartEntry>;

    mutable std::mutex entries_mutex;
    mutable std::unordered_map<MergeTreePartInfo, PartEntryPtr> entries;

    /// Entry lookup
    PartEntryPtr getOrCreateEntry(const MergeTreePartInfo & part) const;
    PartEntryPtr findEntry(const MergeTreePartInfo & part) const;

    void removeStagedFor(const MergeTreePartInfo & owner, const MergeTreePartInfo & target);
    void removeStagedOwner(const MergeTreePartInfo & owner, const MergeTreePartInfo & target);

    /// The part in {Active, Outdated}, or null. The returned pointer IS the pin on its directory.
    DataPartPtr findPart(const MergeTreePartInfo & info) const;

    /// One version of one part, and where its bytes are.
    struct Version
    {
        CSN csn = 0;
        /// Null: the part's own `delete_bitmap_<csn>.rbm`. Otherwise the part still holding this
        /// version staged, where it is named after the target instead.
        DataPartPtr staged_in;
    };

    /// The staged versions those owners hold, csn-resolved.
    std::vector<Version> stagedVersions(const std::vector<MergeTreePartInfo> & owners) const;

    /// The highest version <= `snapshot_csn`
    std::optional<Version> versionAt(const MergeTreePartInfo & part, CSN snapshot_csn) const;

    /// Read one version's bytes, following it if a settle moved them meanwhile.
    DeleteBitmapPtr readVersion(const IMergeTreeDataPart & part, const Version & version) const;

    /// Private, not on the interface: a caller of `IBitmapStore` sees only the `SettleReport` these
    /// collapse into.
    enum class SettleOutcome
    {
        Published,
        Unlinked,
        Deferred,
        Failed,
    };

    SettleOutcome settleOneStagedFile(
        const IMergeTreeDataPart & owner,
        const MergeTreePartInfo & target,
        CSN csn);

    /// The two halves of the above, each holding at most one entry lock at a time.
    SettleOutcome sweepOrphanTarget(
        const IMergeTreeDataPart & owner,
        const MergeTreePartInfo & target);

    SettleOutcome publishStagedFile(
        const IMergeTreeDataPart & owner,
        const IMergeTreeDataPart & target,
        CSN csn);

    LoggerPtr log;
    /// Co-owned with `Context`, so it cannot dangle; null when bitmap caching is off.
    DeleteBitmapCachePtr cache;
    /// Non-owning: the table owns this store.
    const MergeTreeData & data;
};

}
