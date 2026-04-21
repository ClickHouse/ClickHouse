#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

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

/// v2 patches. Find the patch mark ranges whose sort-key values intersect the sort-key range
/// covered by `original_ranges` on the main part.
///
/// For each main range, binary-search the patch's primary index for the patch granules that
/// cover `[main[range.begin], main[range.end]]`. The search compares over the full *common
/// prefix* of the sort keys (not just the leading column) so tables with compound sort keys
/// actually get a tight bound — comparing only column 0 would conflate all rows that share the
/// same leading value and degrade pruning to nothing when the cardinality of column 0 is low.
/// Per-range bounds are collected, sorted, and coalesced via `optimizeRanges` (same pattern
/// the v1 Merge/Join modes use).
/// Fall through to "every range" in the unusual cases where we can't get a bound (empty index,
/// no common prefix, etc.) — correctness is preserved, only the perf benefit is lost.
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

    const auto & reverse_flags = patch.sorting_key->reverse_flags;
    const size_t patch_sorting_key_prefix_size = patch.sorting_key ? patch.sorting_key->column_names.size() : 0;
    const size_t common_prefix_size = std::min(main_index->size(), patch_sorting_key_prefix_size);

    if (common_prefix_size == 0)
        return emit_all_patch_ranges();

    ColumnRawPtrs main_sorting_key_columns(common_prefix_size);
    ColumnRawPtrs patch_sorting_key_columns(common_prefix_size);

    for (size_t i = 0; i < common_prefix_size; ++i)
    {
        main_sorting_key_columns[i] = (*main_index)[i].get();
        patch_sorting_key_columns[i] = (*patch_index)[i].get();
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

    for (const auto & range : original_ranges)
    {
        if (range.begin >= main_marks_count)
            continue;

        const size_t main_end = std::min(range.end, main_marks_count);

        /// lower_bound: first patch granule whose first-row key is >= main[range.begin].
        /// Back up by 1 to include the patch granule whose rows *span* main[range.begin]
        /// (that granule has keys in `[patch[j-1], patch[j])` — the main key may fall inside).
        size_t lo = 0;
        size_t hi = patch_marks_count;

        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            if (compare_patch(mid, range.begin) < 0)
                lo = mid + 1;
            else
                hi = mid;
        }

        const size_t patch_lo = lo > 0 ? lo - 1 : 0;
        size_t patch_hi;

        /// upper_bound: first patch granule whose first-row sort-key is STRICTLY greater than
        /// main row `main_end`. For an interior mark that's the next granule's first row —
        /// a strict upper bound on the range's keys. For the FINAL mark, the writer stores
        /// the *last row's* value rather than a past-the-end sentinel; using `>=` would then
        /// drop patch granules whose first key equals that last-row value, even though main
        /// has a matching row. Always use strict `>` so equal values are kept. When the main
        /// range reaches the end of the part (`main_end == main_marks_count`) there is no
        /// index row available at all — fall back to the full patch suffix.

        if (main_end == main_marks_count)
        {
            patch_hi = patch_marks_count;
        }
        else
        {
            lo = patch_lo;
            hi = patch_marks_count;

            while (lo < hi)
            {
                size_t mid = lo + (hi - lo) / 2;
                if (compare_patch(mid, main_end) <= 0)
                    lo = mid + 1;
                else
                    hi = mid;
            }

            patch_hi = lo;
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
        MarkRanges split_ranges;

        std::sort(ranges.begin(), ranges.end(), [](const auto & lhs, const auto & rhs) { return lhs.begin < rhs.begin; });
        auto optimized_ranges = optimizeRanges(ranges);

        for (auto & range : optimized_ranges)
        {
            size_t num_full_splits = (range.end - range.begin) / max_granules_in_range;
            for (size_t i = 0; i < num_full_splits; ++i)
                split_ranges.emplace_back(range.begin + max_granules_in_range * i, range.begin + max_granules_in_range * (i + 1));

            if ((range.end - range.begin) % max_granules_in_range != 0)
               split_ranges.emplace_back(range.begin + max_granules_in_range * num_full_splits, range.end);
        }

        ranges = std::move(split_ranges);
    }
}

std::vector<MarkRanges> RangesInPatchParts::getRanges(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & ranges) const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AnalyzePatchRangesMicroseconds);

    auto raw_ranges = getRangesInPatchParts(original_part, patch_parts, ranges);
    chassert(patch_parts.size() == raw_ranges.size());

    std::vector<MarkRanges> optimized_ranges(raw_ranges.size());

    for (size_t i = 0; i < raw_ranges.size(); ++i)
        optimized_ranges[i] = getIntersectingRanges(patch_parts[i].part->getPartName(), raw_ranges[i]);

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

    auto index_ptr = MergeTreeIndexFactory::instance().get(*it);
    /// Check that index exists in data part. It may be absent for parts created in earlier versions.
    if (!index_ptr->getDeserializedFormat(patch_part->checksums, index_ptr->getFileName()))
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

        reader.read(ranges[i].begin, nullptr, granule);
        std::tie(stats.min, stats.max) = getMinMaxValues(*granule);

        for (size_t j = ranges[i].begin + 1; j < last_mark; ++j)
        {
            reader.read(j, nullptr, granule);
            auto [min, max] = getMinMaxValues(*granule);

            stats.min = std::min(stats.min, min);
            stats.max = std::max(stats.max, max);
        }
    }

    return result;
}

bool intersects(const MinMaxStat & lhs, const MinMaxStat & rhs)
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
