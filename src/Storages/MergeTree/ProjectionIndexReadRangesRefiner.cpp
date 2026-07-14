#include <Storages/MergeTree/ProjectionIndexReadRangesRefiner.h>

#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

#include <Common/Exception.h>

#include <algorithm>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

class ProjectionBitmapForwardCursor
{
public:
    explicit ProjectionBitmapForwardCursor(const ProjectionIndexBitmap & bitmap_)
        : bitmap(bitmap_)
    {
        if (bitmap.type == ProjectionIndexBitmap::BitmapType::Bitmap32)
        {
            roaring::api::roaring_iterator_init(bitmap.data.bitmap32, &iterator32);
            has_value = iterator32.has_value;
        }
        else
        {
            iterator64 = roaring::api::roaring64_iterator_create(bitmap.data.bitmap64);
            if (!iterator64)
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Failed to allocate roaring64 iterator");
            has_value = roaring::api::roaring64_iterator_has_value(iterator64);
        }
    }

    ProjectionBitmapForwardCursor(const ProjectionBitmapForwardCursor &) = delete;
    ProjectionBitmapForwardCursor & operator=(const ProjectionBitmapForwardCursor &) = delete;

    ~ProjectionBitmapForwardCursor()
    {
        if (iterator64)
            roaring::api::roaring64_iterator_free(iterator64);
    }

    bool seekAtLeast(UInt64 target)
    {
        if (!has_value)
            return false;

        if (value() >= target)
            return true;

        if (bitmap.type == ProjectionIndexBitmap::BitmapType::Bitmap32)
        {
            if (target > std::numeric_limits<UInt32>::max())
                return has_value = false;
            has_value = roaring::api::roaring_uint32_iterator_move_equalorlarger(&iterator32, static_cast<UInt32>(target));
        }
        else
        {
            has_value = roaring::api::roaring64_iterator_move_equalorlarger(iterator64, target);
        }
        return has_value;
    }

    bool advancePast(UInt64 lower_bound)
    {
        if (!has_value)
            return false;

        if (bitmap.type == ProjectionIndexBitmap::BitmapType::Bitmap32)
            has_value = roaring::api::roaring_uint32_iterator_advance(&iterator32);
        else
            has_value = roaring::api::roaring64_iterator_advance(iterator64);

        if (!has_value || value() >= lower_bound)
            return has_value;
        return seekAtLeast(lower_bound);
    }

    UInt64 value() const
    {
        chassert(has_value);
        if (bitmap.type == ProjectionIndexBitmap::BitmapType::Bitmap32)
            return iterator32.current_value;
        return roaring::api::roaring64_iterator_value(iterator64);
    }

private:
    const ProjectionIndexBitmap & bitmap;
    roaring::api::roaring_uint32_iterator_t iterator32{};
    roaring::api::roaring64_iterator_t * iterator64 = nullptr;
    bool has_value = false;
};

class ProjectionBitmapReverseCursor
{
public:
    explicit ProjectionBitmapReverseCursor(const ProjectionIndexBitmap & bitmap_)
        : bitmap(bitmap_)
    {
        if (bitmap.type == ProjectionIndexBitmap::BitmapType::Bitmap64)
        {
            iterator64 = roaring::api::roaring64_iterator_create(bitmap.data.bitmap64);
            if (!iterator64)
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Failed to allocate roaring64 iterator");
        }
    }

    ProjectionBitmapReverseCursor(const ProjectionBitmapReverseCursor &) = delete;
    ProjectionBitmapReverseCursor & operator=(const ProjectionBitmapReverseCursor &) = delete;

    ~ProjectionBitmapReverseCursor()
    {
        if (iterator64)
            roaring::api::roaring64_iterator_free(iterator64);
    }

    bool seekBefore(UInt64 upper_bound)
    {
        if (upper_bound == 0)
            return has_value = false;

        if (has_value && value() < upper_bound)
            return true;

        if (bitmap.type == ProjectionIndexBitmap::BitmapType::Bitmap32)
        {
            if (upper_bound > std::numeric_limits<UInt32>::max())
            {
                roaring::api::roaring_iterator_init_last(bitmap.data.bitmap32, &iterator32);
                return has_value = iterator32.has_value;
            }

            /// The 32-bit lower-bound operation can reposition an iterator in either direction.
            /// Move to the first value at or above the exclusive boundary, then step back to its
            /// predecessor. `previous` is also valid after a failed seek and then lands on the
            /// bitmap's last value.
            roaring::api::roaring_iterator_init(bitmap.data.bitmap32, &iterator32);
            roaring::api::roaring_uint32_iterator_move_equalorlarger(&iterator32, static_cast<UInt32>(upper_bound));
            has_value = roaring::api::roaring_uint32_iterator_previous(&iterator32);
        }
        else
        {
            /// The public 64-bit seek is only documented in the forward direction. Reinitialize
            /// the already-allocated iterator before the lower-bound search so each predecessor
            /// lookup is independent, then step back once.
            roaring::api::roaring64_iterator_reinit(bitmap.data.bitmap64, iterator64);
            roaring::api::roaring64_iterator_move_equalorlarger(iterator64, upper_bound);
            has_value = roaring::api::roaring64_iterator_previous(iterator64);
        }
        return has_value;
    }

    bool retreatBefore(UInt64 upper_bound)
    {
        if (!has_value)
            return false;

        if (bitmap.type == ProjectionIndexBitmap::BitmapType::Bitmap32)
            has_value = roaring::api::roaring_uint32_iterator_previous(&iterator32);
        else
            has_value = roaring::api::roaring64_iterator_previous(iterator64);

        if (!has_value || value() < upper_bound)
            return has_value;
        return seekBefore(upper_bound);
    }

    UInt64 value() const
    {
        chassert(has_value);
        if (bitmap.type == ProjectionIndexBitmap::BitmapType::Bitmap32)
            return iterator32.current_value;
        return roaring::api::roaring64_iterator_value(iterator64);
    }

private:
    const ProjectionIndexBitmap & bitmap;
    roaring::api::roaring_uint32_iterator_t iterator32{};
    roaring::api::roaring64_iterator_t * iterator64 = nullptr;
    bool has_value = false;
};

class ProjectionIndexReadRangesRefinementSession final : public IMergeTreeReadRangesRefinementSession
{
public:
    ProjectionIndexReadRangesRefinementSession(
        MergeTreeIndexBuildContextPtr index_build_context_,
        MergeTreeIndexReadResultPtr index_read_result_,
        const MergeTreeReadTaskInfo & info,
        MergeTreeReadRangesRefinementDirection direction_)
        : index_build_context(std::move(index_build_context_))
        , index_read_result(std::move(index_read_result_))
        , data_part(info.data_part)
        , index_granularity(*data_part->index_granularity)
        , part_index_in_query(info.part_index_in_query)
        , direction(direction_)
    {
        const auto & bitmap = *index_read_result->projection_index_read_result;
        if (direction == MergeTreeReadRangesRefinementDirection::Forward)
            forward_cursor = std::make_unique<ProjectionBitmapForwardCursor>(bitmap);
        else
            reverse_cursor = std::make_unique<ProjectionBitmapReverseCursor>(bitmap);
    }

    MarkRanges refine(MarkRanges ranges) override
    {
        if (ranges.empty())
            return ranges;

        size_t marks_before = ranges.getNumberOfMarks();
        MarkRanges result = direction == MergeTreeReadRangesRefinementDirection::Forward ? refineForward(ranges) : refineReverse(ranges);
        size_t marks_after = result.getNumberOfMarks();
        chassert(marks_after <= marks_before);

        accountForDroppedMarks(marks_before - marks_after);
        return result;
    }

private:
    size_t markForRowOffset(UInt64 offset) const
    {
        size_t mark = index_granularity.getMarkRangeForRowOffset(offset).begin;
        chassert(mark < index_granularity.getMarksCount());
        return std::min(mark, index_granularity.getMarksCount() - 1);
    }

    MarkRanges refineForward(const MarkRanges & ranges)
    {
        MarkRanges result;
        size_t range_pos = 0;

        if (!forward_cursor->seekAtLeast(index_granularity.getMarkStartingRow(ranges.front().begin)))
            return result;

        while (range_pos < ranges.size())
        {
            size_t mark = markForRowOffset(forward_cursor->value());
            auto range_it = ranges.begin() + range_pos;
            if (mark >= range_it->end)
            {
                range_it = std::lower_bound(
                    range_it + 1, ranges.end(), mark, [](const MarkRange & range, size_t value) { return range.end <= value; });
                if (range_it == ranges.end())
                    break;
                range_pos = static_cast<size_t>(range_it - ranges.begin());
            }

            if (mark < range_it->begin)
            {
                if (!forward_cursor->seekAtLeast(index_granularity.getMarkStartingRow(range_it->begin)))
                    break;
                continue;
            }

            chassert(mark < range_it->end);
            if (!result.empty() && result.back().end == mark)
                result.back().end = mark + 1;
            else
                result.emplace_back(mark, mark + 1);

            if (mark + 1 == index_granularity.getMarksCount()
                || !forward_cursor->advancePast(index_granularity.getMarkStartingRow(mark + 1)))
                break;
        }
        return result;
    }

    MarkRanges refineReverse(const MarkRanges & ranges)
    {
        MarkRanges result;
        size_t range_pos = ranges.size() - 1;

        if (!reverse_cursor->seekBefore(index_granularity.getMarkStartingRow(ranges.back().end)))
            return result;

        while (true)
        {
            size_t mark = markForRowOffset(reverse_cursor->value());
            auto range_it = ranges.begin() + range_pos;
            if (mark < range_it->begin)
            {
                range_it = std::upper_bound(
                    ranges.begin(), range_it, mark, [](size_t value, const MarkRange & range) { return value < range.begin; });
                if (range_it == ranges.begin())
                    break;
                --range_it;
                range_pos = static_cast<size_t>(range_it - ranges.begin());
            }

            if (mark >= range_it->end)
            {
                if (!reverse_cursor->seekBefore(index_granularity.getMarkStartingRow(range_it->end)))
                    break;
                continue;
            }

            chassert(mark >= range_it->begin);
            if (!result.empty() && mark + 1 == result.front().begin)
                result.front().begin = mark;
            else
                result.emplace_front(mark, mark + 1);

            if (!reverse_cursor->retreatBefore(index_granularity.getMarkStartingRow(mark)))
                break;
        }
        return result;
    }

    void accountForDroppedMarks(size_t dropped_marks)
    {
        if (dropped_marks == 0)
            return;

        auto & remaining_marks = index_build_context->part_remaining_marks.at(part_index_in_query).value;
        size_t marks_before = remaining_marks.load(std::memory_order_acquire);
        size_t marks_after = 0;
        do
        {
            if (marks_before < dropped_marks)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot account for {} marks dropped from part {}: only {} marks remain",
                    dropped_marks,
                    data_part->name,
                    marks_before);
            marks_after = marks_before - dropped_marks;
        } while (!remaining_marks.compare_exchange_weak(marks_before, marks_after, std::memory_order_acq_rel, std::memory_order_acquire));

        if (marks_after == 0)
            index_build_context->index_reader_pool->clear(data_part);
    }

    const MergeTreeIndexBuildContextPtr index_build_context;
    const MergeTreeIndexReadResultPtr index_read_result;
    const DataPartPtr data_part;
    const MergeTreeIndexGranularity & index_granularity;
    const size_t part_index_in_query;
    const MergeTreeReadRangesRefinementDirection direction;
    std::unique_ptr<ProjectionBitmapForwardCursor> forward_cursor;
    std::unique_ptr<ProjectionBitmapReverseCursor> reverse_cursor;
};
}

ProjectionIndexReadRangesRefiner::ProjectionIndexReadRangesRefiner(
    MergeTreeIndexBuildContextPtr index_build_context_, StorageMetadataPtr metadata_snapshot_)
    : index_build_context(std::move(index_build_context_))
    , metadata_snapshot(std::move(metadata_snapshot_))
{
    chassert(index_build_context);
}

MergeTreeReadRangesRefinementSessionPtr
ProjectionIndexReadRangesRefiner::createSession(const MergeTreeReadTaskInfo & info, MergeTreeReadRangesRefinementDirection direction) const
{
    auto projection_it = index_build_context->projection_read_ranges.find(info.part_index_in_query);
    if (projection_it == index_build_context->projection_read_ranges.end())
        return nullptr;

    const auto & part_ranges = index_build_context->read_ranges.at(info.part_index_in_query);
    const auto & all_updated_columns = info.alter_conversions->getAllUpdatedColumns();

    /// Built once per part; concurrent callers wait on a shared future inside the pool.
    /// The same cached result is later reused by `MergeTreeReaderIndex` for row-level filtering.
    auto index_read_result = index_build_context->index_reader_pool->getOrBuildIndexReadResult(
        part_ranges, projection_it->second, metadata_snapshot, all_updated_columns);

    /// The read may have been cancelled; refinement is an optimization, so just do nothing.
    if (!index_read_result || !index_read_result->projection_index_read_result)
        return nullptr;

    return std::make_unique<ProjectionIndexReadRangesRefinementSession>(index_build_context, std::move(index_read_result), info, direction);
}
}
