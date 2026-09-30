#include <Storages/MergeTree/UniqueKey/ReadSnapshot.h>

#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapStore.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

ReadSnapshot::ReadSnapshot(const DeleteBitmapStore & store_, CSN csn_, std::shared_ptr<const MergeTreeTransactionHolder> pin_)
    : store(store_), csn(csn_), pin(std::move(pin_))
{
}

ConstDeleteBitmapPtr ReadSnapshot::bitmapAt(const MergeTreePartInfo & part) const
{
    {
        std::lock_guard lock(memo_mutex);
        if (auto it = memo.find(part); it != memo.end())
            return it->second;
    }

    /// Read outside the lock: a miss may read the bitmap files. A concurrent miss on the
    /// same part keeps whichever result was published first.
    auto bitmap = store.readBitmap(part, csn).first;
    std::lock_guard lock(memo_mutex);
    return memo.try_emplace(part, std::move(bitmap)).first->second;
}

size_t ReadSnapshot::liveRows(const IMergeTreeDataPart & part) const
{
    return part.rows_count - bitmapAt(part.info)->cardinality();
}

const ReadSnapshot * tryGetUniqueKeyReadSnapshot(const StorageSnapshot & storage_snapshot)
{
    const auto * snapshot_data = dynamic_cast<const MergeTreeData::SnapshotData *>(storage_snapshot.data.get());
    if (!snapshot_data)
        return nullptr;
    return snapshot_data->uk_read_snapshot.get();
}

}
