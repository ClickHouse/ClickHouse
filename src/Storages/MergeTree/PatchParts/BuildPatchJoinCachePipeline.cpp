#include <Storages/MergeTree/PatchParts/BuildPatchJoinCachePipeline.h>
#include <Storages/MergeTree/PatchParts/BuildPatchJoinCacheSink.h>
#include <Storages/MergeTree/PatchParts/PatchJoinReadPool.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Processors/Port.h>
#include <set>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/ScatterByRangeTransform.h>

namespace DB
{

/// Returns true if `patch_stat` overlaps with any interval in the sorted `data_ranges`.
/// Assumes `data_ranges` is sorted by `.min`. Returns true if `data_ranges` is empty (no filtering).
static bool hasOverlapWithSortedRanges(const std::vector<MinMaxStat> & data_ranges, const MinMaxStat & patch_stat)
{
    if (data_ranges.empty())
        return true;

    auto upper = std::upper_bound(
        data_ranges.begin(), data_ranges.end(), patch_stat.max,
        [](UInt64 value, const MinMaxStat & stat) { return value < stat.min; });

    for (auto it = data_ranges.begin(); it != upper; ++it)
    {
        if (intersects(patch_stat, *it))
            return true;
    }

    return false;
}

/// Information about a single Join-mode patch part, needed during cache build.
struct JoinPatchInfo
{
    PatchPartInfoForReader patch_part;
    NamesAndTypesList columns;
};

/// Collects unique Join-mode patches from the given task infos.
static bool collectJoinPatches(
    const MergeTreeReadTaskInfo & info,
    absl::node_hash_map<String, JoinPatchInfo> & join_patches)
{
    bool has_patches = false;

    for (size_t patch_idx = 0; patch_idx < info.patch_parts.size(); ++patch_idx)
    {
        const auto & patch_part = info.patch_parts[patch_idx];
        if (patch_part.mode != PatchMode::Join)
            continue;

        has_patches = true;
        const auto & part_name = patch_part.part->getPartName();

        if (!join_patches.contains(part_name))
        {
            join_patches[part_name] = JoinPatchInfo
            {
                .patch_part = patch_part,
                .columns = info.task_columns.patch_columns[patch_idx],
            };
        }
    }

    return has_patches;
}

/// Precompute per-range minmax stats for each patch part.
/// Returns a map: patch_name -> (mark_range -> {block_number_stat, block_offset_stat}).
static absl::node_hash_map<String, PatchStatsMap> collectPatchStats(
    const RangesInPatchParts & ranges_in_patch_parts,
    const absl::node_hash_map<String, JoinPatchInfo> & join_patches,
    const MergeTreeReaderSettings & reader_settings)
{
    absl::node_hash_map<String, PatchStatsMap> patch_stats_by_name;

    for (const auto & [patch_name, patch_ranges] : ranges_in_patch_parts.getRanges())
    {
        auto patch_it = join_patches.find(patch_name);
        if (patch_it == join_patches.end())
            continue;

        const auto * loaded_part = dynamic_cast<const LoadedMergeTreeDataPartInfoForReader *>(patch_it->second.patch_part.part.get());
        if (!loaded_part)
            continue;

        auto block_number_stats = getPatchMinMaxStats(loaded_part->getDataPart(), patch_ranges, BlockNumberColumn::name, reader_settings);
        auto block_offset_stats = getPatchMinMaxStats(loaded_part->getDataPart(), patch_ranges, BlockOffsetColumn::name, reader_settings);

        if (block_number_stats && block_offset_stats)
        {
            auto & stats = patch_stats_by_name[patch_name];

            for (size_t i = 0; i < patch_ranges.size(); ++i)
            {
                auto & stat = stats[patch_ranges[i]];
                stat.block_number_stat = (*block_number_stats)[i];
                stat.block_offset_stat = (*block_offset_stats)[i];
            }
        }
    }

    return patch_stats_by_name;
}

/// Filter patch ranges by minmax overlap with a data part's stats.
/// Returns the subset of `patch_ranges` that overlap.
static std::set<MarkRange> filterPatchRangesByOverlap(
    const MarkRanges & patch_ranges,
    const PatchStatsMap * patch_stats,
    const std::vector<MinMaxStat> & data_block_number_ranges,
    const std::vector<MinMaxStat> & data_block_offset_ranges)
{
    std::set<MarkRange> result;

    for (const auto & range : patch_ranges)
    {
        if (patch_stats)
        {
            auto stat_it = patch_stats->find(range);

            if (stat_it != patch_stats->end())
            {
                if (!hasOverlapWithSortedRanges(data_block_number_ranges, stat_it->second.block_number_stat))
                    continue;

                if (!hasOverlapWithSortedRanges(data_block_offset_ranges, stat_it->second.block_offset_stat))
                    continue;
            }
        }

        result.insert(range);
    }

    return result;
}

std::shared_ptr<Processors> buildPatchJoinCachePipeline(
    PatchJoinCachePtr patch_join_cache,
    const RangesInPatchParts & ranges_in_patch_parts,
    const std::vector<MergeTreeReadTaskInfoPtr> & per_part_infos,
    const RangesInDataParts & parts_ranges,
    const MergeTreeReadTask::Extras & extras,
    const MergeTreeReaderSettings & reader_settings,
    size_t num_buckets,
    size_t num_threads)
{
    /// 1. Collect Join-mode patch info.
    absl::node_hash_map<String, JoinPatchInfo> join_patches;
    std::vector<size_t> part_indexes_with_patches;

    for (size_t part_idx = 0; part_idx < per_part_infos.size(); ++part_idx)
    {
        const auto & info = *per_part_infos[part_idx];

        if (collectJoinPatches(info, join_patches))
            part_indexes_with_patches.push_back(part_idx);
    }

    if (join_patches.empty())
        return nullptr;

    /// 2. Precompute per-range minmax stats for each patch part (read once, reused across data parts).
    auto patch_stats_by_name = collectPatchStats(ranges_in_patch_parts, join_patches, reader_settings);

    /// 3. For each data part, read its minmax stats and filter patch ranges by per-part overlap.
    ///    Track global `_block_number` range for bucket assignment.
    absl::node_hash_map<String, std::set<MarkRange>> filtered_ranges_by_patch;
    UInt64 global_min_block = std::numeric_limits<UInt64>::max();
    UInt64 global_max_block = 0;

    for (const auto & part_idx : part_indexes_with_patches)
    {
        const auto & info = *per_part_infos[part_idx];
        const auto & data_part = parts_ranges[part_idx].data_part;
        const auto & data_mark_ranges = parts_ranges[part_idx].ranges;

        std::vector<MinMaxStat> data_block_number_ranges;
        std::vector<MinMaxStat> data_block_offset_ranges;

        if (auto stats = getPatchMinMaxStats(data_part, data_mark_ranges, BlockNumberColumn::name, reader_settings))
        {
            for (const auto & stat : *stats)
            {
                data_block_number_ranges.push_back(stat);
                global_min_block = std::min(global_min_block, stat.min);
                global_max_block = std::max(global_max_block, stat.max);
            }
        }

        if (auto stats = getPatchMinMaxStats(data_part, data_mark_ranges, BlockOffsetColumn::name, reader_settings))
        {
            for (const auto & stat : *stats)
                data_block_offset_ranges.push_back(stat);
        }

        std::ranges::sort(data_block_number_ranges, [](const auto & lhs, const auto & rhs) { return lhs.min < rhs.min; });
        std::ranges::sort(data_block_offset_ranges, [](const auto & lhs, const auto & rhs) { return lhs.min < rhs.min; });

        for (const auto & patch_part : info.patch_parts)
        {
            if (patch_part.mode != PatchMode::Join)
                continue;

            const auto & patch_name = patch_part.part->getPartName();
            auto ranges_it = ranges_in_patch_parts.getRanges().find(patch_name);

            if (ranges_it == ranges_in_patch_parts.getRanges().end())
                continue;

            auto stats_it = patch_stats_by_name.find(patch_name);
            const PatchStatsMap * patch_stats = stats_it != patch_stats_by_name.end() ? &stats_it->second : nullptr;

            auto filtered = filterPatchRangesByOverlap(
                ranges_it->second, patch_stats,
                data_block_number_ranges, data_block_offset_ranges);

            filtered_ranges_by_patch[patch_name].merge(filtered);
        }
    }

    if (global_min_block > global_max_block)
    {
        global_min_block = 0;
        global_max_block = 0;
    }

    if (filtered_ranges_by_patch.empty())
        return nullptr;

    /// 4. Initialize cache structure.
    patch_join_cache->init(ranges_in_patch_parts, num_buckets, global_min_block, global_max_block);

    /// 5. Build pipeline.
    auto processors = std::make_shared<Processors>();

    for (auto & [patch_name, filtered_ranges_set] : filtered_ranges_by_patch)
    {
        auto patch_it = join_patches.find(patch_name);
        if (patch_it == join_patches.end())
            continue;

        auto & patch_info = patch_it->second;
        std::vector<MarkRange> patch_ranges(filtered_ranges_set.begin(), filtered_ranges_set.end());

        /// Build MergeTreeReadTaskInfo for this patch part.
        const auto * loaded_part = dynamic_cast<const LoadedMergeTreeDataPartInfoForReader *>(patch_info.patch_part.part.get());
        if (!loaded_part)
            continue;

        auto task_info = std::make_shared<MergeTreeReadTaskInfo>();
        task_info->data_part = loaded_part->getDataPart();
        task_info->alter_conversions = std::make_shared<AlterConversions>();

        /// Use column types from the patch part itself, not from the current schema.
        /// This prevents performRequiredConversions from trying to cast between
        /// old and new types (e.g., String -> UInt64) which can fail.
        const auto & part_columns = loaded_part->getColumns();
        NamesAndTypesList resolved_columns;

        for (const auto & column : patch_info.columns)
        {
            auto part_column = part_columns.tryGetByName(column.name);
            resolved_columns.push_back(part_column ? *part_column : column);
        }

        task_info->task_columns.columns = resolved_columns;

        /// Compute header from resolved columns (using the part's actual types).
        Block patch_header;
        for (const auto & col : resolved_columns)
            patch_header.insert(ColumnWithTypeAndName(col.type->createColumn(), col.type, col.name));

        size_t block_number_pos = patch_header.getPositionByName(BlockNumberColumn::name);
        size_t num_sources = std::min(num_threads, patch_ranges.size());

        if (num_sources == 0)
            num_sources = 1;

        /// Create read pool for this patch.
        auto shared_header = std::make_shared<Block>(patch_header);

        MergeTreeReadTask::BlockSizeParams block_size_params =
        {
            .max_block_size_rows = std::numeric_limits<UInt64>::max(),
            .preferred_block_size_bytes = 0
        };

        auto pool = std::make_shared<PatchJoinReadPool>(patch_header, task_info, extras, std::move(patch_ranges), block_size_params);

        /// Create source processors.
        std::vector<IProcessor *> sources;
        for (size_t source_idx = 0; source_idx < num_sources; ++source_idx)
        {
            auto algorithm = std::make_unique<MergeTreeThreadSelectAlgorithm>(source_idx);

            auto processor = std::make_unique<MergeTreeSelectProcessor>(
                pool,
                std::move(algorithm),
                /*row_level_filter=*/ nullptr,
                /*prewhere_info=*/ nullptr,
                /*index_read_tasks=*/ IndexReadTasks{},
                ExpressionActionsSettings{},
                reader_settings);

            auto source = std::make_shared<MergeTreeSource>(std::move(processor), "PatchJoinCacheBuild");
            sources.push_back(source.get());
            processors->push_back(std::move(source));
        }

        const auto & entries = patch_join_cache->getEntries(patch_name);

        if (num_buckets <= 1)
        {
            /// No scatter needed -- resize num_sources->1 then sink.
            auto resize = std::make_shared<ResizeProcessor>(shared_header, num_sources, 1);
            {
                auto & inputs = resize->getInputs();
                auto input_it = inputs.begin();

                for (size_t source_idx = 0; source_idx < num_sources; ++source_idx, ++input_it)
                    connect(sources[source_idx]->getOutputs().front(), *input_it);
            }

            auto sink = std::make_shared<BuildPatchJoinCacheSink>(shared_header, entries[0]);
            connect(resize->getOutputs().front(), sink->getPort());
            processors->push_back(std::move(resize));
            processors->push_back(std::move(sink));
            continue;
        }

        /// Create scatter transforms.
        std::vector<IProcessor *> scatters;
        for (size_t source_idx = 0; source_idx < num_sources; ++source_idx)
        {
            auto scatter = std::make_shared<ScatterByRangeTransform>(
                shared_header,
                num_buckets,
                block_number_pos,
                global_min_block,
                global_max_block);

            connect(sources[source_idx]->getOutputs().front(), scatter->getInputs().front());
            scatters.push_back(scatter.get());
            processors->push_back(std::move(scatter));
        }

        /// Create resize processors + sinks per bucket.
        for (size_t bucket_idx = 0; bucket_idx < num_buckets; ++bucket_idx)
        {
            auto resize = std::make_shared<ResizeProcessor>(shared_header, num_sources, 1);
            auto & inputs = resize->getInputs();
            auto input_it = inputs.begin();

            for (size_t source_idx = 0; source_idx < num_sources; ++source_idx, ++input_it)
            {
                auto out_it = scatters[source_idx]->getOutputs().begin();
                std::advance(out_it, bucket_idx);
                connect(*out_it, *input_it);
            }

            auto sink = std::make_shared<BuildPatchJoinCacheSink>(shared_header, entries[bucket_idx]);
            connect(resize->getOutputs().front(), sink->getPort());

            processors->push_back(std::move(resize));
            processors->push_back(std::move(sink));
        }
    }

    if (processors->empty())
        return nullptr;

    return processors;
}

}
