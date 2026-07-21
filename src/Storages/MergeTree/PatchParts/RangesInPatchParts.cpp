#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/ProfileEvents.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <base/range.h>

namespace ProfileEvents
{
    extern const Event AnalyzePatchRangesMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

MarkRanges optimizeRanges(const MarkRanges & ranges)
{
    if (ranges.empty())
        return MarkRanges{};

    MarkRanges result_ranges;
    result_ranges.push_back(ranges[0]);

    for (size_t i = 1; i < ranges.size(); ++i)
    {
        auto & last_range = result_ranges.back();
        if (ranges[i].begin < last_range.begin)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Ranges for patch parts must be sorted");

        if (ranges[i].begin > last_range.end)
            result_ranges.push_back(ranges[i]);
        else
            last_range.end = std::max(last_range.end, ranges[i].end);
    }

    return result_ranges;
}

/// Splits ranges into subranges with at most `max_granules_in_range` granules each.
MarkRanges splitRanges(const MarkRanges & ranges, size_t max_granules_in_range)
{
    MarkRanges split_ranges;

    for (const auto & range : ranges)
    {
        size_t begin = range.begin;

        while (begin < range.end)
        {
            size_t next = std::min<size_t>(range.end, begin + max_granules_in_range);
            split_ranges.emplace_back(begin, next);
            begin = next;
        }
    }

    return split_ranges;
}

MarkRanges getRangesInPatchPartMerge(const DataPartPtr & original_part, const PatchPartInfoForReader & patch, const MarkRanges & original_ranges)
{
    chassert(patch.mode == PatchMode::Merge);
    if (patch.source_parts.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Applying patch parts with mode {} requires only one part", PatchMode::Merge);

    if (patch.source_parts.front() != original_part->name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} source part, got: {}", original_part->name, patch.source_parts.front());

    MarkRanges patch_part_ranges;
    const auto & index_granularity = original_part->index_granularity;
    auto patch_index = patch.part->getIndexPtr();

    /// Index may be empty if part is empty.
    if (patch_index->empty())
        return {};

    if (patch_index->size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index of patch part must have 2 columns, got {}", patch_index->size());

    const auto & patch_name_column = assert_cast<const ColumnLowCardinality &>(*patch_index->at(0));
    const auto & patch_offset_data = assert_cast<const ColumnUInt64 &>(*patch_index->at(1)).getData();

    for (const auto & range : original_ranges)
    {
        size_t begin_row = index_granularity->getMarkStartingRow(range.begin);
        size_t end_row = index_granularity->getMarkStartingRow(range.end);

        auto [begin_range, end_range] = getPartNameOffsetRange(
            patch_name_column, patch_offset_data, original_part->name, begin_row, end_row);

        if (begin_range == patch_name_column.size() || end_range == 0)
            continue;

        if (begin_range != 0)
            --begin_range;

        patch_part_ranges.emplace_back(begin_range, end_range);
    }

    std::ranges::sort(patch_part_ranges, std::less{}, &MarkRange::begin);

    return optimizeRanges(patch_part_ranges);
}

MarkRanges getRangesInPatchPartJoin(const PatchPartInfoForReader & patch)
{
    chassert(patch.mode == PatchMode::Join);
    MarkRanges patch_part_ranges;
    auto patch_index = patch.part->getIndexPtr();

    /// Index may be empty if part is empty.
    if (patch_index->empty())
        return {};

    if (patch_index->size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index of patch part must have 2 columns, got {}", patch_index->size());

    const auto & patch_name_column = assert_cast<const ColumnLowCardinality &>(*patch_index->at(0));

    for (const auto & source_part_name : patch.source_parts)
    {
        auto [begin_range, end_range] = getPartNameRange(patch_name_column, source_part_name);

        if (begin_range == patch_name_column.size() || end_range == 0)
            continue;

        if (begin_range != 0)
            --begin_range;

        patch_part_ranges.emplace_back(begin_range, end_range);
    }

    std::sort(patch_part_ranges.begin(), patch_part_ranges.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs.begin < rhs.begin;
    });

    return optimizeRanges(patch_part_ranges);
}

/// Returns the ranges of a `MergeOnKey` (v2) patch part required to apply the patch to
/// `original_ranges` of the main part. For each main range binary-searches the patch's primary
/// index over the common prefix of the main and patch sorting keys. Falls back to the whole
/// patch part when bounds cannot be obtained (empty index, no common key prefix) —
/// only the pruning benefit is lost.
MarkRanges getRangesInPatchPartMergeOnKey(
    const DataPartPtr & original_part,
    const PatchPartInfoForReader & patch,
    const MarkRanges & original_ranges)
{
    chassert(patch.mode == PatchMode::MergeOnKey);
    const size_t patch_marks_count = patch.part->getIndexGranularity().getMarksCount();

    if (patch_marks_count == 0)
        return {};

    auto emit_all_patch_ranges = [&]() -> MarkRanges
    {
        MarkRanges all;
        all.emplace_back(0, patch_marks_count);
        return all;
    };

    if (original_ranges.empty())
        return {};

    const size_t main_marks_count = original_part->index_granularity->getMarksCount();
    auto main_index = original_part->getIndex();
    auto patch_index = patch.part->getIndexPtr();

    if (!main_index || main_index->empty() || !patch_index || patch_index->empty())
        return emit_all_patch_ranges();

    if (!patch.sorting_key)
        return emit_all_patch_ranges();

    const auto & reverse_flags = patch.sorting_key->reverse_flags;
    const size_t patch_sorting_key_prefix_size = patch.sorting_key->column_names.size();
    const size_t common_prefix_size = std::min(main_index->size(), patch_sorting_key_prefix_size);

    if (common_prefix_size == 0)
        return emit_all_patch_ranges();

    Columns main_sorting_key_columns(common_prefix_size);
    Columns patch_sorting_key_columns(common_prefix_size);

    for (size_t i = 0; i < common_prefix_size; ++i)
    {
        /// After an ALTER like LowCardinality(T) <-> T on a key column the main index is loaded
        /// with the current type while the patch index keeps the type the patch was written with.
        main_sorting_key_columns[i] = recursiveRemoveLowCardinality(removeSpecialRepresentations((*main_index)[i]->convertToFullColumnIfConst()));
        patch_sorting_key_columns[i] = recursiveRemoveLowCardinality(removeSpecialRepresentations((*patch_index)[i]->convertToFullColumnIfConst()));
    }

    auto compare_patch = [&](size_t patch_row, size_t main_row) -> int
    {
        for (size_t i = 0; i < common_prefix_size; ++i)
        {
            int cmp = patch_sorting_key_columns[i]->compareAt(patch_row, main_row, *main_sorting_key_columns[i], /*nan_direction_hint=*/ 1);
            if (cmp != 0)
                return (i < reverse_flags.size() && reverse_flags[i]) ? -cmp : cmp;
        }
        return 0;
    };

    MarkRanges patch_part_ranges;
    const auto patch_marks = collections::range(patch_marks_count);

    for (const auto & range : original_ranges)
    {
        if (range.begin >= main_marks_count)
            continue;

        const size_t main_end = std::min(range.end, main_marks_count);

        /// Find the first patch granule whose first-row key is >= the first key of the main range.
        /// Take one granule before it as well: the index stores only first-row keys, and the rows
        /// of the previous granule may contain the main key too.
        auto lower_it = std::lower_bound(
            patch_marks.begin(), patch_marks.end(), range.begin,
            [&](size_t patch_row, size_t main_row) { return compare_patch(patch_row, main_row) < 0; });

        const size_t lower_mark = lower_it - patch_marks.begin();
        const size_t patch_lo = lower_mark > 0 ? lower_mark - 1 : 0;
        size_t patch_hi = 0;

        if (main_end == main_marks_count)
        {
            /// There is no index entry after the last mark, take all the remaining patch granules.
            patch_hi = patch_marks_count;
        }
        else
        {
            /// Find the first patch granule whose first-row key is strictly greater than the key at
            /// main row `main_end`. Keys equal to it may still belong to the main range, so patch
            /// granules starting with that key are required.
            auto upper_it = std::upper_bound(
                patch_marks.begin() + patch_lo, patch_marks.end(), main_end,
                [&](size_t main_row, size_t patch_row) { return compare_patch(patch_row, main_row) > 0; });

            patch_hi = upper_it - patch_marks.begin();
        }

        if (patch_lo < patch_hi)
            patch_part_ranges.emplace_back(patch_lo, patch_hi);
    }

    std::ranges::sort(patch_part_ranges, std::less{}, &MarkRange::begin);
    return optimizeRanges(patch_part_ranges);
}

MarkRanges getRangesInPatchPart(const DataPartPtr & original_part, const PatchPartInfoForReader & patch, const MarkRanges & ranges)
{
    switch (patch.mode)
    {
        case PatchMode::Merge:
            return getRangesInPatchPartMerge(original_part, patch, ranges);
        case PatchMode::Join:
            return getRangesInPatchPartJoin(patch);
        case PatchMode::MergeOnKey:
            return getRangesInPatchPartMergeOnKey(original_part, patch, ranges);
    }
}

std::vector<MarkRanges> getRangesInPatchParts(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & ranges)
{
    std::vector<MarkRanges> patches_ranges;
    patches_ranges.reserve(patch_parts.size());

    for (const auto & patch_part : patch_parts)
    {
        auto patch_ranges = getRangesInPatchPart(original_part, patch_part, ranges);
        patches_ranges.push_back(std::move(patch_ranges));
    }

    return patches_ranges;
}

}

void RangesInPatchParts::addPart(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & original_ranges)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AnalyzePatchRangesMicroseconds);

    for (const auto & patch_part : patch_parts)
    {
        /// Ranges are accumulated only for `Join` patches: `PatchJoinCache` is keyed by the chunks built from them.
        /// For other modes `getRanges` builds tight per-task ranges without using `ranges_by_name`.
        if (patch_part.mode != PatchMode::Join)
            continue;

        auto patch_ranges = getRangesInPatchPart(original_part, patch_part, original_ranges);

        if (!patch_ranges.empty())
        {
            auto & current_ranges = ranges_by_name[patch_part.part->getPartName()];
            current_ranges.insert(current_ranges.end(), patch_ranges.begin(), patch_ranges.end());
        }
    }
}

void RangesInPatchParts::optimize()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AnalyzePatchRangesMicroseconds);

    for (auto & [_, ranges] : ranges_by_name)
    {
        std::sort(ranges.begin(), ranges.end(), [](const auto & lhs, const auto & rhs) { return lhs.begin < rhs.begin; });
        ranges = splitRanges(optimizeRanges(ranges), max_granules_in_range);
    }
}

std::vector<MarkRanges> RangesInPatchParts::getRanges(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & ranges) const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AnalyzePatchRangesMicroseconds);

    auto raw_ranges = getRangesInPatchParts(original_part, patch_parts, ranges);
    chassert(patch_parts.size() == raw_ranges.size());

    std::vector<MarkRanges> optimized_ranges(raw_ranges.size());

    for (size_t i = 0; i < raw_ranges.size(); ++i)
    {
        /// `Join` patches must use whole chunks of `ranges_by_name` because `PatchJoinCache` is
        /// keyed by them. Other modes have no caches shared between tasks and use the tight
        /// per-task ranges directly, because intersecting with the chunks would only widen them.
        if (patch_parts[i].mode == PatchMode::Join)
            optimized_ranges[i] = getIntersectingRanges(patch_parts[i].part->getPartName(), raw_ranges[i]);
        else
            optimized_ranges[i] = splitRanges(raw_ranges[i], max_granules_in_range);
    }

    return optimized_ranges;
}

MarkRanges RangesInPatchParts::getIntersectingRanges(const String & patch_name, const MarkRanges & ranges) const
{
    auto it = ranges_by_name.find(patch_name);
    if (it == ranges_by_name.end())
        return {};

    /// The result ranges must be sorted.
    std::set<MarkRange> res;
    const auto & patch_ranges = it->second;

    for (const auto & range : ranges)
    {
        const auto * left = std::lower_bound(patch_ranges.begin(), patch_ranges.end(), range.begin, [](const MarkRange & r, UInt64 value) { return r.end < value; });
        const auto * right = std::upper_bound(patch_ranges.begin(), patch_ranges.end(), range.end, [](UInt64 value, const MarkRange & r) { return value < r.begin; });

        res.insert(left, right);
    }

    return MarkRanges(res.begin(), res.end());
}

static std::pair<UInt64, UInt64> getMinMaxValues(const IMergeTreeIndexGranule & granule)
{
    const auto & minmax_granule = assert_cast<const MergeTreeIndexGranuleMinMax &>(granule);
    chassert(minmax_granule.hyperrectangle.size() == 1);

    UInt64 min = minmax_granule.hyperrectangle[0].left.safeGet<UInt64>();
    UInt64 max = minmax_granule.hyperrectangle[0].right.safeGet<UInt64>();

    return {min, max};
}

MaybeMinMaxStats getPatchMinMaxStats(const DataPartPtr & patch_part, const MarkRanges & ranges, const String & column_name, const MergeTreeReaderSettings & settings)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AnalyzePatchRangesMicroseconds);

    auto metadata_snapshot = patch_part->getMetadataSnapshot();
    const auto & secondary_indices = metadata_snapshot->getSecondaryIndices();

    auto it = std::ranges::find_if(
        secondary_indices,
        [&](const auto & index)
        { return index.isImplicitlyCreated() && index.name == IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX + column_name; });

    if (it == secondary_indices.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected minmax index for {} column", column_name);

    if (it->type != "minmax")
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected minmax index for {} column, got: {}", column_name, it->type);

    static const MergeTreeSettings default_settings;
    auto index_ptr = MergeTreeIndexFactory::instance().get(metadata_snapshot, *it, default_settings);
    /// Check that index exists in data part. It may be absent for parts created in earlier versions.
    if (!index_ptr->getDeserializedFormat(patch_part->checksums, index_ptr->getFileName(), &patch_part->getDataPartStorage()))
        return {};

    size_t total_marks_without_final = patch_part->index_granularity->getMarksCountWithoutFinal();
    MarkRanges index_mark_ranges = {{0, total_marks_without_final}};

    auto context = Context::getGlobalContextInstance();
    auto mark_cache = context->getIndexMarkCache();
    auto uncompressed_cache = context->getIndexUncompressedCache();

    MergeTreeIndexReader reader(
        index_ptr,
        patch_part,
        total_marks_without_final,
        index_mark_ranges,
        mark_cache.get(),
        uncompressed_cache.get(),
        /*vector_similarity_index_cache=*/ nullptr,
        settings);

    MergeTreeIndexGranulePtr granule = nullptr;
    MinMaxStats result(ranges.size());

    for (size_t i = 0; i < ranges.size(); ++i)
    {
        auto & stats = result[i];
        size_t last_mark = std::min(ranges[i].end, total_marks_without_final);

        if (ranges[i].begin == last_mark)
            continue;

        reader.read(ranges[i].begin, nullptr, granule, /*readable_ranges=*/ nullptr);
        std::tie(stats.min, stats.max) = getMinMaxValues(*granule);

        for (size_t j = ranges[i].begin + 1; j < last_mark; ++j)
        {
            reader.read(j, nullptr, granule, /*readable_ranges=*/ nullptr);
            auto [min, max] = getMinMaxValues(*granule);

            stats.min = std::min(stats.min, min);
            stats.max = std::max(stats.max, max);
        }
    }

    return result;
}

static bool intersects(const MinMaxStat & lhs, const MinMaxStat & rhs)
{
    return (lhs.min <= rhs.min && rhs.min <= lhs.max) || (rhs.min <= lhs.min && lhs.min <= rhs.max);
}

MarkRanges filterPatchRanges(const MarkRanges & ranges, const PatchStatsMap & patch_stats, const PatchStats & result_stats)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AnalyzePatchRangesMicroseconds);
    MarkRanges result;

    for (auto range : ranges)
    {
        auto it = patch_stats.find(range);

        if (it != patch_stats.end()
            && intersects(result_stats.block_number_stat, it->second.block_number_stat)
            && intersects(result_stats.block_offset_stat, it->second.block_offset_stat))
        {
            result.push_back(range);
        }
    }

    return result;
}

}
