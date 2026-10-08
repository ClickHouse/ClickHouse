#include <Storages/MergeTree/UniqueKey/ReadSnapshot.h>

#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapStore.h>
#include <Storages/StorageSnapshot.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event UniqueKeyBitmapGranulesSkipped;
}

namespace DB
{

namespace
{

/// `ranges` without the marks whose rows are all in `bitmap`; a run is split where a middle granule goes.
MarkRanges selectLiveMarkRanges(const MarkRanges & ranges, const MergeTreeIndexGranularity & granularity, const DeleteBitmap & bitmap)
{
    MarkRanges live;
    for (const auto & range : ranges)
    {
        for (size_t mark = range.begin; mark < range.end; ++mark)
        {
            const size_t rows = granularity.getMarkRows(mark);
            const UInt64 row_begin = granularity.getMarkStartingRow(mark);
            if (rows > 0 && bitmap.rangeCardinality(row_begin, row_begin + rows) == rows)
                continue;

            if (!live.empty() && live.back().end == mark)
                ++live.back().end;
            else
                live.emplace_back(mark, mark + 1);
        }
    }
    return live;
}

}

ReadSnapshot::ReadSnapshot(
    const DeleteBitmapStore & store_, CSN csn_, TransactionID tid_, std::shared_ptr<const MergeTreeTransactionHolder> pin_)
    : store(store_), csn(csn_), tid(tid_), pin(std::move(pin_))
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

void ReadSnapshot::dropFullyDeadGranules(RangesInDataParts & parts) const
{
    size_t granules_skipped = 0;
    for (auto & part : parts)
    {
        const ConstDeleteBitmapPtr bitmap = bitmapAt(part.data_part->info);
        if (bitmap->empty())
            continue;

        /// Exact ranges are not pruned: every path that computes them is guarded off unique-key tables.
        chassert(part.exact_ranges.empty(), fmt::format("part {} has exact ranges", part.data_part->name));

        const size_t marks_before = part.getMarksCount();
        part.ranges = selectLiveMarkRanges(part.ranges, *part.data_part->index_granularity, *bitmap);
        granules_skipped += marks_before - part.getMarksCount();
    }

    std::erase_if(parts, [](const RangesInDataPart & part) { return part.ranges.empty(); });
    ProfileEvents::increment(ProfileEvents::UniqueKeyBitmapGranulesSkipped, granules_skipped);
}

const ReadSnapshot * tryGetUniqueKeyReadSnapshot(const StorageSnapshot & storage_snapshot)
{
    const auto * snapshot_data = dynamic_cast<const MergeTreeData::SnapshotData *>(storage_snapshot.data.get());
    if (!snapshot_data)
        return nullptr;
    return snapshot_data->uk_read_snapshot.get();
}

}
