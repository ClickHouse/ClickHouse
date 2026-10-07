#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

class DeleteBitmapStore;
class IMergeTreeDataPart;
class MergeTreeTransactionHolder;
struct RangesInDataParts;
struct StorageSnapshot;

/// What a read needs to resolve any part's delete bitmap: the reading transaction's snapshot
/// csn, the table's bitmap store, and the pin that keeps the GC off the versions that snapshot
/// can still resolve.
class ReadSnapshot
{
public:
    ReadSnapshot(
        const DeleteBitmapStore & store_,
        CSN csn_,
        TransactionID tid_ = Tx::EmptyTID,
        std::shared_ptr<const MergeTreeTransactionHolder> pin_ = {});

    /// Never null -- an empty bitmap means "no deletions". Highest version at or below the
    /// snapshot; an uncommitted one resolves to no csn and so is never selected. Every reader of
    /// this snapshot gets the same object for a part, whichever reader asked first.
    ConstDeleteBitmapPtr bitmapAt(const MergeTreePartInfo & part) const;

    /// `part`'s rows minus the ones its bitmap kills.
    size_t liveRows(const IMergeTreeDataPart & part) const;

    /// Drops the granules whose rows are all dead at this snapshot, and the parts left with none.
    void dropFullyDeadGranules(RangesInDataParts & parts) const;

    CSN snapshotCSN() const { return csn; }

    /// The reading transaction -- the query's or the read's own pin -- whose parts the read sees.
    TransactionID readerTID() const { return tid; }

private:
    /// Owned by the storage, which outlives every read it serves.
    const DeleteBitmapStore & store;
    CSN csn;
    TransactionID tid;
    std::shared_ptr<const MergeTreeTransactionHolder> pin;

    /// So index analysis and the read pool agree even if a version's csn resolves between their lookups.
    mutable std::mutex memo_mutex;
    mutable std::unordered_map<MergeTreePartInfo, ConstDeleteBitmapPtr> memo;
};

using ReadSnapshotPtr = std::shared_ptr<const ReadSnapshot>;

/// The read snapshot a MergeTree storage snapshot carries; null for a table without a unique key
/// and for a snapshot built without data.
const ReadSnapshot * tryGetUniqueKeyReadSnapshot(const StorageSnapshot & storage_snapshot);

}
