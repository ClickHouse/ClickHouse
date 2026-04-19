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

    /// Fall-back helper used when the patch's primary index can't be reliably consulted: emit every
    /// mark range so the caller at least reads the whole patch and applies it correctly, instead of
    /// either crashing or silently skipping the patch (which would miss updates).
    auto emit_all_patch_ranges = [&]() -> MarkRanges
    {
        const auto & patch_granularity = patch.part->getIndexGranularity();
        if (patch_granularity.getMarksCount() == 0)
            return {};
        MarkRanges all;
        all.emplace_back(0, patch_granularity.getMarksCount());
        return all;
    };

    /// Defensive: loadIndex on tables with `ORDER BY tuple()` can fail to populate the sparse primary
    /// index and leave its ColumnPtrs null. Emit all ranges rather than segfaulting inside
    /// `getPartNameOffsetRange` (which would then miss applying the patch entirely).
    if (!patch_index->at(0) || !patch_index->at(1))
        return emit_all_patch_ranges();

    const auto * patch_name_column_ptr = typeid_cast<const ColumnLowCardinality *>(patch_index->at(0).get());
    const auto * patch_offset_column_ptr = typeid_cast<const ColumnUInt64 *>(patch_index->at(1).get());
    if (!patch_name_column_ptr || !patch_offset_column_ptr)
    {
        /// Primary-index deserialization for patch parts whose base table has `ORDER BY tuple()`
        /// has been observed to produce column types that don't match the declared sort key schema
        /// (usually UInt64/UInt64 instead of LowCardinality(String)/UInt64). Suppress the crash
        /// and fall back to reading the whole patch so correctness is preserved.
        LOG_DEBUG(getLogger("getRangesInPatchPartMerge"),
            "Patch {} primary index has unexpected types: col0={}, col1={} — falling back to all ranges",
            patch.part->getPartName(),
            patch_index->at(0) ? patch_index->at(0)->getName() : "(null)",
            patch_index->at(1) ? patch_index->at(1)->getName() : "(null)");
        return emit_all_patch_ranges();
    }

    const auto & patch_name_column = *patch_name_column_ptr;
    const auto & patch_offset_data = patch_offset_column_ptr->getData();

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
/// Without this, every SELECT task would be handed *every* patch mark range and only prune them
/// at read time inside the streaming merge loop — which means fully reading and decompressing
/// patch blocks that can never contribute to the task's main rows. On a 200 M-row table with a
/// 100 M-row patch that produced ~50 B patch-row reads per `SELECT count()` (read amplification
/// ~500× because every task under `max_threads > 1` reads the whole patch independently).
///
/// Bound the patch read at range-enumeration time: pull `[min_sk, max_sk]` from the main part's
/// primary index over `original_ranges`, then binary-search the patch's primary index for the
/// granule range that covers `[min_sk, max_sk]`. Fall through to "every range" in the unusual
/// cases where we can't get a bound (empty index, mismatched column types, etc.) — correctness
/// is preserved, only the perf benefit is lost.
MarkRanges getRangesInPatchPartMergeOnKey(
    const DataPartPtr & original_part,
    const PatchPartInfoForReader & patch,
    const MarkRanges & original_ranges)
{
    chassert(patch.mode == PatchMode::MergeOnKey);
    const auto & patch_index_granularity = patch.part->getIndexGranularity();
    const size_t patch_marks_count = patch_index_granularity.getMarksCount();
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

    auto main_index = original_part->getIndex();
    auto patch_index = patch.part->getIndexPtr();
    if (!main_index || main_index->empty() || !patch_index || patch_index->empty())
        return emit_all_patch_ranges();

    /// Both indexes store the sort-key expression's *result* columns starting at position 0.
    /// For a plain `ORDER BY a` that's `a`; for `ORDER BY cityHash64(id)` it's the materialized
    /// `cityHash64(id)` column. Patch column 0 was built by `getPatchPartMetadataV2` so the
    /// types line up with the main side.
    if (main_index->empty() || patch_index->empty())
        return emit_all_patch_ranges();

    const IColumn & main_sk = *(*main_index)[0];
    const IColumn & patch_sk = *(*patch_index)[0];
    if (main_sk.empty() || patch_sk.empty())
        return emit_all_patch_ranges();

    /// Fold `original_ranges` to a single `[min_granule, max_granule_end)` span — the narrowest
    /// bound on the sort-key range the main side will produce.
    /// `min_granule` indexes the row in `main_sk` holding the lowest sort-key we'll read; the
    /// upper bound is harder to pin down because main's index only records *first-of-granule*
    /// values, so we use `main_sk[max_granule_end]` (i.e. the first row of the next granule) as
    /// a conservative strict upper bound, or fall through to the last index row if there is no
    /// next granule.
    size_t main_marks_count = main_sk.size();
    size_t min_granule = std::numeric_limits<size_t>::max();
    size_t max_granule_end = 0;
    for (const auto & range : original_ranges)
    {
        if (range.begin < min_granule)
            min_granule = range.begin;
        if (range.end > max_granule_end)
            max_granule_end = range.end;
    }
    if (min_granule >= main_marks_count)
        return {};
    if (max_granule_end > main_marks_count)
        max_granule_end = main_marks_count;

    const size_t n_patch = patch_sk.size();

    /// lower_bound by `compareAt`: first j where patch_sk[j] >= main_sk[min_granule].
    /// Then back up by 1 to include the granule whose rows span main's min (patch granule j-1
    /// has `a` values in [patch_sk[j-1], patch_sk[j]) which may include `main_sk[min_granule]`).
    auto is_less = [&](size_t patch_row, size_t main_row, const IColumn & main_col) -> bool
    {
        return patch_sk.compareAt(patch_row, main_row, main_col, /*nan_direction_hint=*/ 1) < 0;
    };
    size_t lo = 0;
    size_t hi = n_patch;
    while (lo < hi)
    {
        size_t mid = lo + (hi - lo) / 2;
        if (is_less(mid, min_granule, main_sk))
            lo = mid + 1;
        else
            hi = mid;
    }
    const size_t patch_lo = lo > 0 ? lo - 1 : 0;

    /// Upper bound: first patch granule whose first-row sort-key is STRICTLY greater than main's
    /// max sort-key. `main_sk[max_granule_end]` is the primary-index value at row
    /// `max_granule_end`. For an interior granule (not the final mark), that's the next granule's
    /// first row — a strict upper bound on the current granule's keys. For the FINAL mark, the
    /// writer stores `last_index_block.rows() - 1`, i.e. the *last row's* value — so the final
    /// mark equals the last granule's max key, not a past-the-end sentinel. Using a non-strict
    /// `>=` compare would then exclude patch granules whose first key equals that last-row value,
    /// even though main has a matching row. Always use STRICT `>` so equal values are kept; at
    /// worst we over-read one patch granule, and the apply loop filters per row.
    /// When main reads to the end of the part (`max_granule_end == main_marks_count`) there is
    /// no index row available at all — keep `patch_hi = n_patch` (full suffix).
    size_t patch_hi;
    if (max_granule_end == main_marks_count)
    {
        patch_hi = n_patch;
    }
    else
    {
        /// is_le: patch_sk[j] <= main_sk[upper_main_row]. We want first j where this is false
        /// (i.e. patch_sk[j] > main_sk[upper_main_row]).
        auto is_le = [&](size_t patch_row, size_t main_row, const IColumn & main_col) -> bool
        {
            return patch_sk.compareAt(patch_row, main_row, main_col, /*nan_direction_hint=*/ 1) <= 0;
        };
        lo = patch_lo;
        hi = n_patch;
        const size_t upper_main_row = max_granule_end;
        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            if (is_le(mid, upper_main_row, main_sk))
                lo = mid + 1;
            else
                hi = mid;
        }
        patch_hi = lo;
    }

    /// Translate patch index rows to patch mark-range bounds. Patch granule j corresponds to
    /// mark j. We need the mark range covering granules `[patch_lo, patch_hi)`.
    if (patch_lo >= patch_hi || patch_hi > patch_marks_count)
    {
        /// Nothing overlaps; return empty so the reader does zero work on this patch for
        /// this task.
        return {};
    }

    MarkRanges result;
    result.emplace_back(patch_lo, patch_hi);
    return result;
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
    {
        /// v2 `MergeOnKey` pruning already returns a sort-key-tight range per task
        /// (`getRangesInPatchPartMergeOnKey`). Intersecting that against the pre-split 8-mark
        /// chunks from `ranges_by_name` would widen the read back out to a chunk boundary: e.g.
        /// a 1-mark overlap would fetch the whole 8-mark chunk (~65 k rows) instead of the single
        /// ~8 k-row granule that actually matters. Split the tight range ourselves, capped at
        /// `max_granules_in_range`, so the reader still gets chunk-sized units without over-reading.
        if (patch_parts[i].mode == PatchMode::MergeOnKey)
        {
            MarkRanges split;
            for (const auto & r : raw_ranges[i])
            {
                size_t begin = r.begin;
                while (begin < r.end)
                {
                    size_t next = std::min<size_t>(r.end, begin + max_granules_in_range);
                    split.emplace_back(begin, next);
                    begin = next;
                }
            }
            optimized_ranges[i] = std::move(split);
        }
        else
        {
            optimized_ranges[i] = getIntersectingRanges(patch_parts[i].part->getPartName(), raw_ranges[i]);
        }
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
