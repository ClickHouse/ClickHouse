#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/ManualMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TrivialMergeSelector.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Processors/Transforms/ColumnGathererTransform.h>

#include <Common/MemoryTracker.h>
#include <Common/logger_useful.h>

#include <limits>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 max_bytes_to_merge_at_max_space_in_pool;
    extern const MergeTreeSettingsUInt64 max_parts_to_merge_at_once;
    extern const MergeTreeSettingsUInt64 merge_selector_blurry_base_scale_factor;
    extern const MergeTreeSettingsUInt64 merge_selector_window_size;
    extern const MergeTreeSettingsBool min_age_to_force_merge_on_partition_only;
    extern const MergeTreeSettingsUInt64 min_age_to_force_merge_seconds;
    extern const MergeTreeSettingsUInt64 min_partition_age_to_force_merge_seconds;
    extern const MergeTreeSettingsBool ttl_only_drop_parts;
    extern const MergeTreeSettingsUInt64 parts_to_throw_insert;
    extern const MergeTreeSettingsMergeSelectorAlgorithm merge_selector_algorithm;
    extern const MergeTreeSettingsBool merge_selector_enable_heuristic_to_remove_small_parts_at_right;
    extern const MergeTreeSettingsBool merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once;
    extern const MergeTreeSettingsUInt64 merge_selector_heuristic_to_lower_max_parts_to_merge_at_once_exponent;
    extern const MergeTreeSettingsFloat merge_selector_base;
    extern const MergeTreeSettingsUInt64 min_parts_to_merge_at_once;
    extern const MergeTreeSettingsBool apply_patches_on_merge;
    extern const MergeTreeSettingsUInt64 merge_memory_estimate_per_source_part_column;
    extern const MergeTreeSettingsUInt64 enable_vertical_merge_algorithm;
    extern const MergeTreeSettingsUInt64 vertical_merge_algorithm_min_rows_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_merge_algorithm_min_bytes_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_merge_algorithm_min_columns_to_activate;
    extern const MergeTreeSettingsBool vertical_merge_optimize_ttl_delete;
    extern const MergeTreeSettingsBool allow_vertical_merges_from_compact_to_wide_parts;
    extern const MergeTreeSettingsUInt64 min_bytes_for_wide_part;
    extern const MergeTreeSettingsUInt64 min_rows_for_wide_part;
    extern const MergeTreeSettingsUInt32 min_level_for_wide_part;
    extern const MergeTreeSettingsUInt64 min_bytes_for_full_part_storage;
    extern const MergeTreeSettingsUInt64 min_rows_for_full_part_storage;
    extern const MergeTreeSettingsUInt32 min_level_for_full_part_storage;
    extern const MergeTreeSettingsMergeTreePartMinMaxIndexColumns part_minmax_index_columns;
}

namespace
{

struct ChooseContext
{
    const PartsRanges & ranges;
    const PartitionsStatistics & partitions_stats;
    const IMergePredicate & predicate;
    const IMergeSelector::RangeFilter & range_filter;
    const StorageID & storage_id;
    const MergeConstraints & merge_constraints;
    const StorageInMemoryMetadata & metadata_snapshot;
    const MergeTreeSettings & merge_tree_settings;
    const MergeTreeData::MergingParams & merging_params;
    const PartitionIdToTTLs & next_delete_times;
    const PartitionIdToTTLs & next_recompress_times;
    const time_t current_time;
    const bool aggressive;
};

/// A merge keeps one block from every source part alive at the same time, and every column of every one
/// of those blocks costs at least a minimum allocation, even when the block holds a handful of rows. That
/// fixed cost is proportional to `source parts * columns` and does not shrink with the amount of data:
/// merging 100 parts of a two-thousand-column table needs a few hundred MiB before a single row is read
/// (`system.metric_log`, which has over two thousand columns, needs around 700 MiB).
///
/// On a server with little memory such a merge fails with `MEMORY_LIMIT_EXCEEDED`, its source parts stay
/// where they were, and the next selection round picks the same parts again - the table stops compacting
/// while its part count keeps growing. Narrow the merge instead, so the fixed cost stays a small share of
/// the memory limit. Ordinary tables, and even very wide tables on a large server, keep the configured
/// `max_parts_to_merge_at_once`.
///
/// `columns_alive_per_source_part` is how many columns of every source part the merge holds at the same
/// time: all of them for a horizontal merge, only the key columns for a vertical one (see
/// `predictVerticalMerge`).
/// Returns the memory-derived part of the cap alone, with 0 meaning "no cap" - for the selectors whose
/// width `max_parts_to_merge_at_once` deliberately does not constrain.
size_t getAffordablePartsToMergeAtOnce(const ChooseContext & ctx, size_t columns_alive_per_source_part)
{
    const size_t bytes_per_source_column = ctx.merge_tree_settings[MergeTreeSetting::merge_memory_estimate_per_source_part_column];

    const Int64 memory_limit = total_memory_tracker.getHardLimit();
    if (bytes_per_source_column == 0 || memory_limit <= 0)
        return 0;

    /// The fixed cost of one merge may take at most this share of the memory limit. Merges are background
    /// work that runs next to the queries and inserts the server is there for, so keep their share small.
    static constexpr size_t inverse_share_of_the_limit = 16;

    const size_t num_columns = std::max<size_t>(1, columns_alive_per_source_part);
    /// Divide by the two factors one after another instead of dividing by their product: the estimate is
    /// an unrestricted `UInt64` setting, so `num_columns * bytes_per_source_column` could wrap around and
    /// turn an absurdly large estimate into a wide (or division-by-zero) merge instead of a narrow one.
    /// A merge of fewer than two parts makes no progress, so never narrow below that.
    return std::max<size_t>(
        2, static_cast<size_t>(memory_limit) / inverse_share_of_the_limit / num_columns / bytes_per_source_column);
}

/// The cap for a merge priced as a horizontal one, which holds every column of every source part at once.
/// The selectors that cannot shrink a range they were handed (the TTL selectors and `Trivial`, see
/// `tryChooseTTLMerge` and `tryChooseRegularMerge`) take this cap even for the ranges that would in fact
/// merge vertically: over-pricing such a range costs some merge width, under-pricing it costs the merge.
size_t getAffordablePartsToMergeAtOnce(const ChooseContext & ctx)
{
    return getAffordablePartsToMergeAtOnce(ctx, ctx.metadata_snapshot.getColumns().size());
}

/// How many columns of every source part a vertical merge holds at the same time. Its horizontal stage
/// merges the key columns of all source parts together, and its vertical stage then gathers the other
/// columns one column at a time - so the peak is the horizontal stage. This mirrors the key columns of
/// `MergeTask::ExecuteAndFinalizeHorizontalPart::extractMergingAndGatheringColumns` as far as the table
/// metadata can tell: the sorting key, the columns the merging mode needs, the columns of the min-max index
/// (in case the merge has to recompute it), the columns of multi-column skip indexes and of projections
/// (which are rebuilt on the horizontal stage) and, when rows expire, the columns of the TTL expressions.
/// The remaining columns are the ones the vertical stage gathers. Over-counting here is harmless - it
/// prices a vertical merge a little higher and predicts vertical merges a little less often - so the
/// count leans that way whenever the exact set depends on the parts and not on the table.
size_t getColumnsMergedOnHorizontalStageOfVerticalMerge(const ChooseContext & ctx)
{
    const auto & metadata = ctx.metadata_snapshot;
    const auto & params = ctx.merging_params;

    NameSet key_columns;
    key_columns.insert_range(metadata.getColumnsRequiredForSortingKey());
    key_columns.insert_range(metadata.getColumnsRequiredForPartitionKey());

    /// With `part_minmax_index_columns = 'with_block_number_offset'` the min-max index a merge recomputes
    /// also covers `_block_number` and `_block_offset`, see `MergeTreeData::getMinMaxColumns`.
    if (ctx.merge_tree_settings[MergeTreeSetting::part_minmax_index_columns] >= MergeTreePartMinMaxIndexColumns::WITH_BLOCK_NUMBER_OFFSET)
    {
        key_columns.insert(BlockNumberColumn::name);
        key_columns.insert(BlockOffsetColumn::name);
    }

    if (!params.sign_column.empty())
        key_columns.insert(params.sign_column);
    if (!params.is_deleted_column.empty())
        key_columns.insert(params.is_deleted_column);
    if (!params.version_column.empty())
        key_columns.insert(params.version_column);

    for (const auto & index : metadata.getSecondaryIndices())
    {
        /// A skip index over a single column is computed on the vertical stage along with the column.
        if (index.column_names.size() > 1)
            key_columns.insert_range(index.column_names);
    }

    for (const auto & projection : metadata.getProjections())
        key_columns.insert_range(projection.getRequiredColumns());

    if (metadata.hasRowsTTL())
        for (const auto & column : metadata.getRowsTTL().expression_columns)
            key_columns.insert(column.name);
    for (const auto & where_ttl : metadata.getRowsWhereTTLs())
    {
        for (const auto & column : where_ttl.expression_columns)
            key_columns.insert(column.name);
        for (const auto & column : where_ttl.where_expression_columns)
            key_columns.insert(column.name);
    }

    /// The merge merges at least one column even when the key is empty (`ORDER BY tuple()`).
    return std::max<size_t>(1, key_columns.size());
}

/// Whether the table can merge vertically at all. The rest of the decision depends on the range itself,
/// see `predictVerticalMerge`.
bool tableCanMergeVertically(const ChooseContext & ctx)
{
    const auto & settings = ctx.merge_tree_settings;
    const auto & metadata = ctx.metadata_snapshot;

    if (settings[MergeTreeSetting::enable_vertical_merge_algorithm] == 0)
        return false;

    /// A vertical merge of compact source parts is possible, but whether the source parts are compact is
    /// not known here; if compact sources rule the algorithm out, take the pessimistic answer.
    if (!settings[MergeTreeSetting::allow_vertical_merges_from_compact_to_wide_parts])
        return false;

    using Mode = MergeTreeData::MergingParams;
    const auto mode = ctx.merging_params.mode;
    const bool supported_mode = mode == Mode::Ordinary || mode == Mode::Collapsing || mode == Mode::Replacing || mode == Mode::VersionedCollapsing;
    if (!supported_mode)
        return false;

    const size_t num_columns = metadata.getColumns().size();
    const size_t key_columns = getColumnsMergedOnHorizontalStageOfVerticalMerge(ctx);
    const size_t gathering_columns = num_columns > key_columns ? num_columns - key_columns : 0;
    return gathering_columns >= settings[MergeTreeSetting::vertical_merge_algorithm_min_columns_to_activate];
}

/// Whether a merge that also removes expired values may still run vertically, see
/// `MergeTask::canVerticalTTLDelete`. Whether the source parts carry lightweight deletes (which rule it out
/// too) is not known here.
bool tableCanMergeVerticallyWhileRemovingExpiredValues(const ChooseContext & ctx)
{
    const auto & metadata = ctx.metadata_snapshot;

    if (ctx.merging_params.mode != MergeTreeData::MergingParams::Ordinary)
        return false;
    if (!ctx.merge_tree_settings[MergeTreeSetting::vertical_merge_optimize_ttl_delete])
        return false;
    if (metadata.hasAnyGroupByTTL() || metadata.hasAnyColumnTTL())
        return false;
    return metadata.hasRowsTTL() || metadata.hasAnyRowsWhereTTL();
}

/// Whether a merge of `range` will remove expired values, following how
/// `MergeTask::ExecuteAndFinalizeHorizontalPart::prepare` sets `need_remove_expired_values`: some source part
/// has TTL values that were not calculated, or the earliest unfinished TTL of the merged part is due.
/// A merge that turns out to remove expired values although this says it does not is priced too low only
/// in the narrow window of a TTL that becomes due between the selection and the merge.
bool rangeRemovesExpiredValues(const ChooseContext & ctx, PartsRangeView range)
{
    const auto & metadata = ctx.metadata_snapshot;
    if (!metadata.hasAnyTTL())
        return false;

    /// A merge treats the finished `GROUP BY` TTLs of its source parts as unfinished again (see
    /// `MergeTreeDataPartTTLInfos::update`), so their due times are not in the `part_min_ttl` of the parts.
    if (metadata.hasAnyGroupByTTL())
        return true;

    time_t min_ttl = 0;
    for (const auto & part : range)
    {
        if (!part.all_ttl_calculated_if_any)
            return true;

        if (part.general_ttl_info)
        {
            const time_t part_min_ttl = part.general_ttl_info->part_min_ttl;
            if (part_min_ttl && (!min_ttl || part_min_ttl < min_ttl))
                min_ttl = part_min_ttl;
        }
    }

    return min_ttl && min_ttl <= ctx.current_time;
}

/// Whether a merge of `range` will run through `MergeAlgorithm::Vertical`, following the rules of
/// `MergeTask::ExecuteAndFinalizeHorizontalPart::chooseMergeAlgorithm` that depend on the range: the result
/// has to be a wide part in full storage, and the range has to carry enough rows and bytes. The part sizes
/// known here are the compressed sizes on disk, while the merge decides by the uncompressed ones; the
/// former are smaller, so this predicts a horizontal merge whenever the sizes are close to a threshold.
/// A false "horizontal" only keeps the pessimistic cap for that range.
bool predictVerticalMerge(const ChooseContext & ctx, PartsRangeView range)
{
    const auto & settings = ctx.merge_tree_settings;

    if (range.size() > RowSourcePart::MAX_PARTS)
        return false;

    size_t sum_rows = 0;
    size_t sum_bytes = 0;
    UInt32 max_level = 0;
    for (const auto & part : range)
    {
        sum_rows += part.rows;
        sum_bytes += part.size;
        max_level = std::max(max_level, part.info.level);
    }
    const UInt32 result_level = max_level + 1;

    /// See `MergeTreeData::choosePartFormat`.
    const bool wide_part = sum_bytes >= settings[MergeTreeSetting::min_bytes_for_wide_part]
        && sum_rows >= settings[MergeTreeSetting::min_rows_for_wide_part]
        && result_level >= settings[MergeTreeSetting::min_level_for_wide_part];
    const bool full_storage = sum_bytes >= settings[MergeTreeSetting::min_bytes_for_full_part_storage]
        && sum_rows >= settings[MergeTreeSetting::min_rows_for_full_part_storage]
        && result_level >= settings[MergeTreeSetting::min_level_for_full_part_storage];
    if (!wide_part || !full_storage)
        return false;

    if (rangeRemovesExpiredValues(ctx, range) && !tableCanMergeVerticallyWhileRemovingExpiredValues(ctx))
        return false;

    return sum_rows >= settings[MergeTreeSetting::vertical_merge_algorithm_min_rows_to_activate]
        && sum_bytes >= settings[MergeTreeSetting::vertical_merge_algorithm_min_bytes_to_activate];
}

/// The memory-derived cap for the selectors that can shrink a candidate range (`SimpleMergeSelector`
/// considers every sub-range of a partition), as a `RangeFilter`: a range may exceed the cap of a
/// horizontal merge when it will merge vertically and fits the cap of a vertical merge instead. Vertical
/// merges hold only the key columns of every source part at once, so a wide table on a small server keeps
/// its full merge width for the large merges that go vertical, and is narrowed only for the small
/// horizontal ones - whose fixed cost is the very thing this estimate is about.
/// Returns the caller's filter unchanged when there is nothing to cap.
IMergeSelector::RangeFilter capRangesByAffordableMemory(const ChooseContext & ctx, const IMergeSelector::RangeFilter & range_filter)
{
    const size_t affordable_horizontal = getAffordablePartsToMergeAtOnce(ctx);
    if (affordable_horizontal == 0)
        return range_filter;

    const size_t affordable_vertical = tableCanMergeVertically(ctx)
        ? getAffordablePartsToMergeAtOnce(ctx, getColumnsMergedOnHorizontalStageOfVerticalMerge(ctx))
        : affordable_horizontal;

    return [&ctx, range_filter, affordable_horizontal, affordable_vertical](PartsRangeView range)
    {
        if (range_filter && !range_filter(range))
            return false;
        if (range.size() <= affordable_horizontal)
            return true;
        return range.size() <= affordable_vertical && predictVerticalMerge(ctx, range);
    };
}

size_t getMaxPartsToMergeAtOnce(const ChooseContext & ctx)
{
    const size_t configured = ctx.merge_tree_settings[MergeTreeSetting::max_parts_to_merge_at_once];
    const size_t affordable = getAffordablePartsToMergeAtOnce(ctx);

    /// A value of 0 means "no limit" on either side.
    if (affordable == 0)
        return configured;
    if (configured == 0)
        return affordable;
    return std::min(configured, affordable);
}

MergeSelectorChoices pack(const ChooseContext & ctx, PartsRanges && ranges, MergeType type)
{
    auto create_choice = [&](PartsRange && parts, MergeType merge_type)
    {
        const bool apply_patch_parts = ctx.merge_tree_settings[MergeTreeSetting::apply_patches_on_merge];
        PartsRange patch_parts = apply_patch_parts ? ctx.predicate.getPatchesToApplyOnMerge(parts) : PartsRange{};
        return MergeSelectorChoice{std::move(parts), std::move(patch_parts), merge_type};
    };

    MergeSelectorChoices choices;
    choices.reserve(ranges.size());

    for (auto & range : ranges)
        choices.push_back(create_choice(std::move(range), type));

    return choices;
}

MergeSelectorChoices tryChooseTTLMerge(const ChooseContext & ctx)
{
    /// Drop parts - 1 priority
    if (!ctx.merge_constraints.empty())
    {
        /// The size of the completely expired part of TTL drop is not affected by the merge pressure and the size of the storage space.
        std::vector<MergeConstraint> ttl_constraints(ctx.merge_constraints.size(), {std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max()});
        /// A `TTLDrop` merge of a table with only an unconditional rows TTL builds no read pipeline at all
        /// (see `MergeTask::ExecuteAndFinalizeHorizontalPart::prepare`): the source parts are fully expired
        /// and nothing is read from them, so the per-(source part, column) block cost the memory estimate
        /// models does not exist. Capping it there would only stretch TTL cleanup of wide tables into tiny
        /// drop batches for no memory benefit. When any other TTL family is present the pipeline is built
        /// as usual, and the memory-derived cap applies like it does to the other merges.
        const size_t max_parts_to_drop_at_once = ctx.metadata_snapshot.hasOnlyRowsTTL()
            ? ctx.merge_tree_settings[MergeTreeSetting::max_parts_to_merge_at_once]
            : getMaxPartsToMergeAtOnce(ctx);
        TTLPartDropMergeSelector drop_ttl_selector(ctx.current_time, max_parts_to_drop_at_once);

        if (auto merge_ranges = drop_ttl_selector.select(ctx.ranges, ttl_constraints, ctx.range_filter); !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLDrop);
    }

    /// Delete rows - 2 priority
    if (!ctx.merge_constraints.empty() && !ctx.merge_tree_settings[MergeTreeSetting::ttl_only_drop_parts])
    {
        TTLRowDeleteMergeSelector delete_ttl_selector(ctx.next_delete_times, ctx.current_time, getAffordablePartsToMergeAtOnce(ctx));

        if (auto merge_ranges = delete_ttl_selector.select(ctx.ranges, ctx.merge_constraints, ctx.range_filter); !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLDelete);
    }

    /// Delete columns - 3 priority
    ///
    /// `ttl_only_drop_parts` trades the merges that delete expired rows for dropping whole parts once
    /// every row in them has expired. A column TTL has no such alternative - the only way to clear an
    /// expired column is to rewrite the part - so this selector runs regardless of that setting.
    if (!ctx.merge_constraints.empty() && ctx.metadata_snapshot.hasAnyColumnTTL())
    {
        TTLColumnDeleteMergeSelector delete_ttl_selector(ctx.next_delete_times, ctx.current_time, getAffordablePartsToMergeAtOnce(ctx));

        if (auto merge_ranges = delete_ttl_selector.select(ctx.ranges, ctx.merge_constraints, ctx.range_filter); !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLDelete);
    }

    /// Recompression - 4 priority
    if (!ctx.merge_constraints.empty() && ctx.metadata_snapshot.hasAnyRecompressionTTL())
    {
        TTLRecompressMergeSelector recompress_ttl_selector(ctx.next_recompress_times, ctx.current_time, getAffordablePartsToMergeAtOnce(ctx));

        if (auto merge_ranges = recompress_ttl_selector.select(ctx.ranges, ctx.merge_constraints, ctx.range_filter); !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLRecompress);
    }

    return {};
}

SimpleMergeSelector::Settings fillSimpleSettings(const ChooseContext & ctx)
{
    SimpleMergeSelector::Settings simple_merge_settings;

    simple_merge_settings.window_size = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_window_size];
    /// The memory-derived cap reaches this selector through the range filter, see `tryChooseRegularMerge`.
    simple_merge_settings.max_parts_to_merge_at_once = ctx.merge_tree_settings[MergeTreeSetting::max_parts_to_merge_at_once];
    simple_merge_settings.enable_heuristic_to_remove_small_parts_at_right = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_enable_heuristic_to_remove_small_parts_at_right];
    simple_merge_settings.base = static_cast<double>(ctx.merge_tree_settings[MergeTreeSetting::merge_selector_base]);
    simple_merge_settings.min_parts_to_merge_at_once = ctx.merge_tree_settings[MergeTreeSetting::min_parts_to_merge_at_once];

    simple_merge_settings.enable_heuristic_to_lower_max_parts_to_merge_at_once = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once];
    simple_merge_settings.heuristic_to_lower_max_parts_to_merge_at_once_exponent = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_heuristic_to_lower_max_parts_to_merge_at_once_exponent];
    simple_merge_settings.parts_to_throw_insert = ctx.merge_tree_settings[MergeTreeSetting::parts_to_throw_insert];
    simple_merge_settings.partitions_stats = &ctx.partitions_stats;

    if (!ctx.merge_tree_settings[MergeTreeSetting::min_age_to_force_merge_on_partition_only])
        simple_merge_settings.min_age_to_force_merge = ctx.merge_tree_settings[MergeTreeSetting::min_age_to_force_merge_seconds];

    simple_merge_settings.min_partition_age_to_force_merge = ctx.merge_tree_settings[MergeTreeSetting::min_partition_age_to_force_merge_seconds];

    if (ctx.aggressive)
        simple_merge_settings.base = 1;

    return simple_merge_settings;
}

SimpleMergeSelector::Settings fillSimpleStochasticSettings(const ChooseContext & ctx)
{
    auto simple_merge_settings = fillSimpleSettings(ctx);

    simple_merge_settings.parts_to_throw_insert = ctx.merge_tree_settings[MergeTreeSetting::parts_to_throw_insert];
    simple_merge_settings.blurry_base_scale_factor = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_blurry_base_scale_factor];
    simple_merge_settings.use_blurry_base = simple_merge_settings.blurry_base_scale_factor != 0;
    simple_merge_settings.enable_stochastic_sliding = true;

    return simple_merge_settings;
}

MergeSelectorChoices tryChooseRegularMerge(const ChooseContext & ctx)
{
    const auto algorithm = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_algorithm];

    MergeSelectorPtr selector;
    IMergeSelector::RangeFilter range_filter = ctx.range_filter;
    switch (algorithm.value)
    {
        /// The simple selectors weigh every sub-range of a partition, so they take the memory-derived cap
        /// as a range filter that tells the horizontal merges it narrows from the vertical ones it does
        /// not have to; the other selectors below can only drop a range the filter rejects, so they take
        /// the cap as a width instead.
        case MergeSelectorAlgorithm::SIMPLE:
            selector = std::make_shared<SimpleMergeSelector>(fillSimpleSettings(ctx));
            range_filter = capRangesByAffordableMemory(ctx, ctx.range_filter);
            break;
        case MergeSelectorAlgorithm::STOCHASTIC_SIMPLE:
            selector = std::make_shared<SimpleMergeSelector>(fillSimpleStochasticSettings(ctx));
            range_filter = capRangesByAffordableMemory(ctx, ctx.range_filter);
            break;
        case MergeSelectorAlgorithm::TRIVIAL:
        {
            /// The trivial selector merges a fixed number of parts and ignores the configured
            /// `max_parts_to_merge_at_once`, but the memory-derived cap must reach it too: otherwise a
            /// small server would keep selecting the same too-wide merge of a very wide table forever.
            TrivialMergeSelector::Settings trivial_merge_settings;
            if (const size_t affordable = getAffordablePartsToMergeAtOnce(ctx))
                trivial_merge_settings.num_parts_to_merge = std::min(trivial_merge_settings.num_parts_to_merge, affordable);
            selector = std::make_shared<TrivialMergeSelector>(trivial_merge_settings);
            break;
        }
        case MergeSelectorAlgorithm::MANUAL:
            selector = std::make_shared<ManualMergeSelector>(ctx.storage_id);
            break;
    }

    chassert(selector != nullptr);
    auto merge_ranges = selector->select(ctx.ranges, ctx.merge_constraints, range_filter);
    return pack(ctx, std::move(merge_ranges), MergeType::Regular);
}

}

MergeSelectorApplier::MergeSelectorApplier(
    std::vector<MergeConstraint> && merge_constraints_,
    bool merge_with_ttl_allowed_,
    bool aggressive_,
    IMergeSelector::RangeFilter range_filter_,
    StorageID storage_id_)
    : merge_constraints(std::move(merge_constraints_))
    , merge_with_ttl_allowed(merge_with_ttl_allowed_)
    , aggressive(aggressive_)
    , range_filter(std::move(range_filter_))
    , storage_id(std::move(storage_id_))
{
    chassert(!merge_constraints.empty(), "At least one merge constraint should be passed");

    chassert(std::ranges::is_sorted(
        merge_constraints,
        [](const auto & lhs, const auto & rhs) { return lhs.max_size_bytes > rhs.max_size_bytes; }),
        "Merge constraints must be sorted in desc order");
}

MergeSelectorChoices MergeSelectorApplier::chooseMergesFrom(
    const PartsRanges & ranges,
    const PartitionsStatistics & partitions_stats,
    const IMergePredicate & predicate,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeSettingsPtr & merge_tree_settings,
    const MergeTreeData::MergingParams & merging_params,
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    bool can_use_ttl_merges,
    time_t current_time) const
{
    ChooseContext ctx{
        .ranges = ranges,
        .partitions_stats = partitions_stats,
        .predicate = predicate,
        .range_filter = range_filter,
        .storage_id = storage_id,
        .merge_constraints = merge_constraints,
        .metadata_snapshot = *metadata_snapshot,
        .merge_tree_settings = *merge_tree_settings,
        .merging_params = merging_params,
        .next_delete_times = next_delete_times,
        .next_recompress_times = next_recompress_times,
        .current_time = current_time,
        .aggressive = aggressive,
    };

    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && can_use_ttl_merges)
        if (auto choices = tryChooseTTLMerge(ctx); !choices.empty())
            return choices;

    return tryChooseRegularMerge(ctx);
}

}
