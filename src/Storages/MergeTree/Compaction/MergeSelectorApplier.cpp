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
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/parseIdentifiersOrStringLiteralsWithSettings.h>
#include <DataTypes/NestedUtils.h>
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
    extern const MergeTreeSettingsUInt64 merge_selector_min_age_to_disable_right_tail_heuristic;
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
    extern const MergeTreeSettingsMergeTreePartMinMaxIndexColumns part_minmax_index_columns;
    extern const MergeTreeSettingsDeduplicateMergeProjectionMode deduplicate_merge_projection_mode;
    extern const MergeTreeSettingsBool materialize_projections_on_merge;
    extern const MergeTreeSettingsBool materialize_skip_indexes_on_merge;
    extern const MergeTreeSettingsString exclude_materialize_skip_indexes_on_merge;
    extern const MergeTreeSettingsUInt64 merge_selector_small_parts_threshold;
    extern const MergeTreeSettingsUInt64 merge_selector_small_parts_min_count;
    extern const MergeTreeSettingsUInt64 merge_selector_small_parts_max_age;
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
    /// `nullptr` when the storage is not `MergeTreeData`, see `MergeSelectorApplier::chooseMergesFrom`.
    const MergeTreeData * data;
    const MergeTreeData::MergingParams & merging_params;
    const PartitionIdToTTLs & next_delete_times;
    const PartitionIdToTTLs & next_recompress_times;
    const time_t current_time;
    const bool aggressive;
    /// `SYSTEM STOP TTL MERGES` is not in effect.
    const bool can_use_ttl_merges;
    /// The requested merge runs through `MergeAlgorithm::Horizontal` whatever the range, see
    /// `MergeSelectorApplier::merge_forced_horizontal`.
    const bool merge_forced_horizontal;
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
    if (bytes_per_source_column == 0 || memory_limit <= 0 || !ctx.data)
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

/// The cap of the parent merge priced as a horizontal one, which holds every column of every source part at once.
size_t getAffordablePartsToMergeHorizontally(const ChooseContext & ctx)
{
    return getAffordablePartsToMergeAtOnce(ctx, ctx.metadata_snapshot.getColumns().getAllPhysical().size());
}

/// The cap for a merge priced as a horizontal one, both the parent merge and the merges of the projection parts
/// that run after it (see `getAffordablePartsToMergeProjection`). The selectors that cannot shrink a range they
/// were handed (the TTL selectors and `Trivial`, see `tryChooseTTLMerge` and `tryChooseRegularMerge`) take this
/// cap even for the ranges that would in fact merge vertically: over-pricing such a range costs some merge
/// width, under-pricing it costs the merge.
size_t getAffordablePartsToMergeAtOnce(const ChooseContext & ctx)
{
    size_t max_columns = ctx.metadata_snapshot.getColumns().getAllPhysical().size();
    for (const auto & projection : ctx.metadata_snapshot.getProjections())
        max_columns = std::max(max_columns, projection.metadata->getColumns().getAllPhysical().size());
    return getAffordablePartsToMergeAtOnce(ctx, max_columns);
}

/// How many columns of every source part a vertical merge holds at the same time. Its horizontal stage
/// merges the key columns of all source parts together, and its vertical stage then gathers the other
/// columns one column at a time - so the peak is the horizontal stage. This mirrors the key columns of
/// `MergeTask::ExecuteAndFinalizeHorizontalPart::extractMergingAndGatheringColumns` as far as the table
/// metadata can tell: the sorting key, the columns the merging mode needs, the columns of multi-column skip
/// indexes and, when the merge removes expired values (`removes_expired_values`), the columns of the TTL
/// expressions. The columns of the min-max index and of the projections the merge rebuilds depend on the range
/// and are added per range, see `getMinMaxIndexColumns` and `getColumnsOfProjectionsRebuiltByMerge`. The
/// remaining columns are the ones the vertical stage gathers. Over-counting here is harmless - it prices a vertical merge a little higher and
/// predicts vertical merges a little less often - so the count leans that way whenever the exact set
/// depends on the parts and not on the table.
NameSet getColumnsMergedOnHorizontalStageOfVerticalMergeSet(const ChooseContext & ctx, bool removes_expired_values)
{
    const auto & metadata = ctx.metadata_snapshot;
    const auto & params = ctx.merging_params;

    NameSet key_columns;
    key_columns.insert_range(metadata.getColumnsRequiredForSortingKey());

    if (!params.sign_column.empty())
        key_columns.insert(params.sign_column);
    if (!params.is_deleted_column.empty())
        key_columns.insert(params.is_deleted_column);
    if (!params.version_column.empty())
        key_columns.insert(params.version_column);

    /// With an empty key (`ORDER BY tuple()`) the merge forces the first physical column onto the horizontal stage
    /// before it adds the columns of the skip indexes, the min-max index, the projections and the TTL expressions,
    /// so that column is merged there even when those other columns are.
    if (key_columns.empty())
    {
        const auto physical_columns = metadata.getColumns().getAllPhysical();
        if (!physical_columns.empty())
            key_columns.insert(physical_columns.front().name);
    }

    /// The merge does not compute the skip indexes of `exclude_materialize_skip_indexes_on_merge`, so their columns
    /// stay on the vertical stage, see `MergeTask::ExecuteAndFinalizeHorizontalPart::prepare`.
    std::unordered_set<String> exclude_index_names;
    if (ctx.merge_tree_settings[MergeTreeSetting::materialize_skip_indexes_on_merge])
    {
        auto exclude_indexes_string = ctx.merge_tree_settings[MergeTreeSetting::exclude_materialize_skip_indexes_on_merge].toString();
        if (!exclude_indexes_string.empty())
            exclude_index_names = parseIdentifiersOrStringLiteralsToSet(exclude_indexes_string, ctx.data->getContext()->getSettingsRef());
    }

    for (const auto & index : metadata.getSecondaryIndices())
    {
        if (exclude_index_names.contains(index.name))
            continue;

        /// A skip index over a single column is computed on the vertical stage along with the column.
        /// Count the source columns of the index expression, not its output columns (`column_names`):
        /// an index like `cityHash64(a, b)` has one output column but needs both `a` and `b`.
        auto index_columns = index.expression->getRequiredColumns();
        if (index_columns.size() > 1)
            key_columns.insert_range(index_columns);
    }

    /// A vertical merge that removes expired values merges the columns of the rows, move and recompression
    /// TTL expressions on the horizontal stage. A merge that does not remove them gathers those columns on
    /// the vertical stage like the others, see `MergeTask::extractMergingAndGatheringColumns`.
    if (!removes_expired_values)
        return key_columns;

    auto add_ttl_expression_columns = [&](const TTLDescription & ttl)
    {
        for (const auto & column : ttl.expression_columns)
            key_columns.insert(column.name);
        for (const auto & column : ttl.where_expression_columns)
            key_columns.insert(column.name);
    };

    if (metadata.hasRowsTTL())
        add_ttl_expression_columns(metadata.getRowsTTL());
    for (const auto & where_ttl : metadata.getRowsWhereTTLs())
        add_ttl_expression_columns(where_ttl);
    for (const auto & move_ttl : metadata.getMoveTTLs())
        add_ttl_expression_columns(move_ttl);
    for (const auto & recompression_ttl : metadata.getRecompressionTTLs())
        add_ttl_expression_columns(recompression_ttl);

    return key_columns;
}

/// The columns of the min-max index, which a vertical merge recomputes on its horizontal stage when the merge
/// may reduce rows (see `rangeMayReduceRows`) and otherwise merges from the indexes of the source parts, see
/// `MergeTask::ExecuteAndFinalizeHorizontalPart::extractMergingAndGatheringColumns`.
NameSet getMinMaxIndexColumns(const ChooseContext & ctx)
{
    NameSet columns;
    columns.insert_range(ctx.metadata_snapshot.getColumnsRequiredForPartitionKey());

    /// With `part_minmax_index_columns = 'with_block_number_offset'` the min-max index a merge recomputes
    /// also covers `_block_number` and `_block_offset`, see `MergeTreeData::getMinMaxColumns`.
    if (ctx.merge_tree_settings[MergeTreeSetting::part_minmax_index_columns] >= MergeTreePartMinMaxIndexColumns::WITH_BLOCK_NUMBER_OFFSET)
    {
        columns.insert(BlockNumberColumn::name);
        columns.insert(BlockOffsetColumn::name);
    }

    return columns;
}

/// The storage columns that `key_columns` keep on the horizontal stage, like `MergeTask` does: the alias and
/// ephemeral columns are not merged at all, and a key column that is a subcolumn keeps its whole storage column,
/// so several subcolumns of one column count once. The persistent virtual columns `_block_number` and
/// `_block_offset` of the min-max index are merged as they are, see `getMinMaxIndexColumns`. `metadata` is the one of
/// the table for the parent merge, or the one of a projection for the merge of its projection parts.
NameSet getKeyColumnsInStorage(const StorageInMemoryMetadata & metadata, const NameSet & key_columns)
{
    const auto physical_columns = metadata.getColumns().getAllPhysical().getNameSet();
    NameSet key_columns_in_storage;
    for (const auto & column : key_columns)
    {
        if (column == BlockNumberColumn::name || column == BlockOffsetColumn::name)
            key_columns_in_storage.insert(column);
        else if (auto name_in_storage = Nested::tryGetColumnNameInStorage(column, physical_columns))
            key_columns_in_storage.insert(std::move(*name_in_storage));
    }
    return key_columns_in_storage;
}

/// Whether the table can merge vertically at all when the horizontal stage merges `num_key_columns` storage
/// columns (see `getColumnsMergedOnHorizontalStageOfVerticalMergeSet` and `getKeyColumnsInStorage`): the more
/// columns move to the horizontal stage, the fewer are left to gather. The rest of the decision depends on the
/// range itself, see `predictVerticalMerge`.
/// `num_expired_columns` is the number of the other columns that the merge expires instead of gathering them,
/// see `getColumnsExpiredByMerge`.
bool tableCanMergeVertically(const ChooseContext & ctx, size_t num_key_columns, size_t num_expired_columns)
{
    const auto & settings = ctx.merge_tree_settings;
    const auto & metadata = ctx.metadata_snapshot;

    if (ctx.merge_forced_horizontal || settings[MergeTreeSetting::enable_vertical_merge_algorithm] == 0)
        return false;

    using Mode = MergeTreeData::MergingParams;
    const auto mode = ctx.merging_params.mode;
    const bool supported_mode = mode == Mode::Ordinary || mode == Mode::Collapsing || mode == Mode::Replacing || mode == Mode::VersionedCollapsing;
    if (!supported_mode)
        return false;

    /// Like `MergeTask`, split only the physical columns, and do not gather the expired ones.
    const size_t num_physical_columns = metadata.getColumns().getAllPhysical().size();
    const size_t num_not_gathered_columns = num_key_columns + num_expired_columns;
    const size_t gathering_columns = num_physical_columns > num_not_gathered_columns ? num_physical_columns - num_not_gathered_columns : 0;
    return gathering_columns >= settings[MergeTreeSetting::vertical_merge_algorithm_min_columns_to_activate];
}

/// Whether a merge that also removes expired values may still run vertically, following the checks of
/// `MergeTask::canVerticalTTLDelete` that depend on the table. The check on lightweight deletes depends on
/// the range, see `rangeCanMergeVerticallyWhileRemovingExpiredValues`.
bool tableCanMergeVerticallyWhileRemovingExpiredValues(const ChooseContext & ctx)
{
    const auto & metadata = ctx.metadata_snapshot;

    using Mode = MergeTreeData::MergingParams;
    switch (ctx.merging_params.mode)
    {
        case Mode::Ordinary:
        case Mode::Replacing:
        case Mode::Collapsing:
        case Mode::VersionedCollapsing:
            break;
        case Mode::Summing:
        case Mode::Aggregating:
        case Mode::Coalescing:
        case Mode::Graphite:
            return false;
    }

    if (!ctx.merge_tree_settings[MergeTreeSetting::vertical_merge_optimize_ttl_delete])
        return false;
    if (metadata.hasAnyGroupByTTL() || metadata.hasAnyColumnTTL())
        return false;
    return metadata.hasRowsTTL() || metadata.hasAnyRowsWhereTTL();
}

/// Whether a merge of `range` that removes expired values may still run vertically: on top of the checks of
/// `tableCanMergeVerticallyWhileRemovingExpiredValues`, no source part and no patch part applied on the merge
/// may carry a lightweight delete, see `MergeTask::hasLightweightDelete`. The properties of the patch parts
/// known here do not tell whether they carry one, so any patch part applied on the merge rules it out.
bool rangeCanMergeVerticallyWhileRemovingExpiredValues(const ChooseContext & ctx, PartsRangeView range)
{
    if (!tableCanMergeVerticallyWhileRemovingExpiredValues(ctx))
        return false;

    for (const auto & part : range)
        if (part.has_lightweight_delete)
            return false;

    if (ctx.merge_tree_settings[MergeTreeSetting::apply_patches_on_merge]
        && !ctx.predicate.getPatchesToApplyOnMerge(PartsRange(range.begin(), range.end())).empty())
        return false;

    return true;
}

/// Whether a merge of `range` will remove expired values, following how
/// `MergeTask::ExecuteAndFinalizeHorizontalPart::prepare` sets `need_remove_expired_values`: some source part
/// has TTL values that were not calculated, or the earliest unfinished TTL of the merged part is due.
/// A merge that turns out to remove expired values although this says it does not is priced too low only
/// in the narrow window of a TTL that becomes due between the selection and the merge.
/// While TTL merges are stopped, `prepare` runs every merge without removing expired values.
bool rangeRemovesExpiredValues(const ChooseContext & ctx, PartsRangeView range)
{
    const auto & metadata = ctx.metadata_snapshot;
    if (!metadata.hasAnyTTL() || !ctx.can_use_ttl_merges)
        return false;

    time_t min_ttl = 0;
    auto update_min_ttl = [&](time_t ttl)
    {
        if (ttl && (!min_ttl || ttl < min_ttl))
            min_ttl = ttl;
    };

    for (const auto & part : range)
    {
        if (!part.all_ttl_calculated_if_any)
            return true;

        if (part.general_ttl_info)
        {
            update_min_ttl(part.general_ttl_info->part_min_ttl);
            /// A merge treats the finished `GROUP BY` TTLs of its source parts as unfinished again (see
            /// `MergeTreeDataPartTTLInfos::update`), so their due times count for the merged part.
            update_min_ttl(part.general_ttl_info->group_by_min_ttl);
        }
    }

    return min_ttl && min_ttl <= ctx.current_time;
}

/// Whether a merge of `range` will run through `MergeAlgorithm::Vertical`, following the rules of
/// `MergeTask::ExecuteAndFinalizeHorizontalPart::chooseMergeAlgorithm` that depend on the range: the result
/// has to be a wide part in full storage, and the range has to carry enough rows and bytes. The bytes are
/// the uncompressed sizes of the columns, the same as `MergeTask` and `FutureMergedMutatedPart` decide by,
/// not the sizes on disk: those include marks and indexes and can exceed the uncompressed size of tiny or
/// poorly compressed parts, which would predict a vertical merge that then runs horizontally. Without
/// `allow_vertical_merges_from_compact_to_wide_parts` every source part has to be wide as well.
/// `removes_expired_values` is `rangeRemovesExpiredValues` of the range.
bool predictVerticalMerge(const ChooseContext & ctx, PartsRangeView range, bool removes_expired_values)
{
    const auto & settings = ctx.merge_tree_settings;

    if (range.size() > RowSourcePart::MAX_PARTS)
        return false;

    const bool allow_compact_sources = settings[MergeTreeSetting::allow_vertical_merges_from_compact_to_wide_parts];

    size_t sum_rows = 0;
    size_t sum_bytes = 0;
    UInt32 max_level = 0;
    MergeTreeDataPartType part_type;
    for (const auto & part : range)
    {
        if (!allow_compact_sources && part.part_type != MergeTreeDataPartType::Wide)
            return false;
        sum_rows += part.rows;
        sum_bytes += part.size_uncompressed;
        max_level = std::max(max_level, part.info.level);
        part_type = std::min(part_type, part.part_type);
    }

    /// The same choice as `FutureMergedMutatedPart::assign` makes for the result of the merge, including that
    /// the result is never of a more compact type than a source part.
    const auto part_format = ctx.data->choosePartFormat(sum_bytes, sum_rows, max_level + 1, /*projection=*/nullptr);
    part_type = std::min(part_type, part_format.part_type);
    if (part_type != MergeTreeDataPartType::Wide || part_format.storage_type != MergeTreeDataPartStorageType::Full)
        return false;

    if (removes_expired_values && !rangeCanMergeVerticallyWhileRemovingExpiredValues(ctx, range))
        return false;

    return sum_rows >= settings[MergeTreeSetting::vertical_merge_algorithm_min_rows_to_activate]
        && sum_bytes >= settings[MergeTreeSetting::vertical_merge_algorithm_min_bytes_to_activate];
}

/// Whether a merge of `range` may reduce rows, following how `MergeTask::ExecuteAndFinalizeHorizontalPart::prepare`
/// sets `merge_may_reduce_rows`. Deduplication and cleanup are requested only by `OPTIMIZE` and are not known here.
bool rangeMayReduceRows(const ChooseContext & ctx, PartsRangeView range, bool removes_expired_values)
{
    if (removes_expired_values || ctx.merging_params.mode != MergeTreeData::MergingParams::Ordinary)
        return true;
    for (const auto & part : range)
        if (part.has_lightweight_delete)
            return true;
    return ctx.merge_tree_settings[MergeTreeSetting::apply_patches_on_merge]
        && !ctx.predicate.getPatchesToApplyOnMerge(PartsRange(range.begin(), range.end())).empty();
}

/// The physical columns that a merge of `range` expires instead of merging them: those absent from every source
/// part and without a default expression, typically added by `ALTER TABLE ... ADD COLUMN` after every source part
/// was written, see `MergeTask::ExecuteAndFinalizeHorizontalPart::prepare` and `PartProperties::absent_columns`.
NameSet getColumnsExpiredByMerge(PartsRangeView range)
{
    NameSet columns;
    if (range.empty())
        return columns;

    for (const auto & column : range.front().absent_columns)
    {
        if (std::all_of(range.begin() + 1, range.end(), [&](const PartProperties & part) { return part.absent_columns.contains(column); }))
            columns.insert(column);
    }
    return columns;
}

/// The columns of the projections that a merge of `range` rebuilds on its horizontal stage, following
/// `MergeTask::ExecuteAndFinalizeHorizontalPart::prepareProjectionsToMergeAndRebuild`. A projection that every
/// source part already has is, in the common case, merged from the projection parts of the sources after the
/// parent merge and does not add its columns to the horizontal stage. Like the rest of the estimate, this leans
/// towards over-counting where the exact answer depends on more than the part properties: any column TTL over
/// a column of a projection is taken as expiring it, and so is a column absent from every source part (see
/// `PartProperties::absent_columns`). `may_reduce_rows` is `rangeMayReduceRows` of the range. The projections that
/// the merge keeps by merging their projection parts are added to `merged_projections`, see
/// `getAffordablePartsToMergeProjection`.
NameSet getColumnsOfProjectionsRebuiltByMerge(
    const ChooseContext & ctx,
    PartsRangeView range,
    bool removes_expired_values,
    bool may_reduce_rows,
    std::vector<const ProjectionDescription *> & merged_projections)
{
    NameSet columns;
    const auto & metadata = ctx.metadata_snapshot;
    const auto & projections = metadata.getProjections();
    if (projections.empty())
        return columns;

    const auto & settings = ctx.merge_tree_settings;
    const auto mode = settings[MergeTreeSetting::deduplicate_merge_projection_mode];
    const bool ordinary = ctx.merging_params.mode == MergeTreeData::MergingParams::Ordinary;

    /// Such a merge drops the projections instead of rebuilding them.
    if (!ordinary && (mode == DeduplicateMergeProjectionMode::THROW || mode == DeduplicateMergeProjectionMode::DROP))
        return columns;

    const auto & column_ttls = metadata.getColumnTTLs();

    /// A column is expired by the merge when it is absent from every source part.
    auto is_absent_from_all_parts = [&](const String & name)
    {
        return std::all_of(range.begin(), range.end(), [&](const PartProperties & part) { return part.absent_columns.contains(name); });
    };

    for (const auto & projection : projections)
    {
        const auto & required_columns = projection.getRequiredColumns();
        const bool is_special_projection = projection.with_parent_part_offset || projection.with_block_number || projection.with_block_offset;

        bool rebuild = false;
        if (may_reduce_rows && (mode != DeduplicateMergeProjectionMode::IGNORE || is_special_projection))
            rebuild = true;
        else if (removes_expired_values && mode != DeduplicateMergeProjectionMode::IGNORE
            && std::any_of(required_columns.begin(), required_columns.end(), [&](const String & name) { return column_ttls.contains(name); }))
            rebuild = true;
        else if (mode != DeduplicateMergeProjectionMode::IGNORE && !range.empty()
            && std::any_of(required_columns.begin(), required_columns.end(), is_absent_from_all_parts))
            rebuild = true;
        else if (mode != DeduplicateMergeProjectionMode::IGNORE
            && std::any_of(range.begin(), range.end(), [&](const PartProperties & part) { return part.projections_missing_columns.contains(projection.name); }))
            rebuild = true;
        else if (std::all_of(range.begin(), range.end(), [&](const PartProperties & part) { return part.projection_names.contains(projection.name); }))
        {
            rebuild = false;
            merged_projections.push_back(&projection);
        }
        else
        {
            /// Every source part has the projection either loaded or on disk but not loaded, and at least one has
            /// it unloaded: the merge rebuilds it from the parent part whatever `materialize_projections_on_merge` is.
            const bool rebuilds_unloaded = std::any_of(range.begin(), range.end(), [&](const PartProperties & part) { return part.unloaded_projection_names.contains(projection.name); })
                && std::all_of(range.begin(), range.end(), [&](const PartProperties & part)
                {
                    return part.projection_names.contains(projection.name) || part.unloaded_projection_names.contains(projection.name);
                });
            rebuild = projection.with_block_number || projection.with_block_offset || rebuilds_unloaded
                || settings[MergeTreeSetting::materialize_projections_on_merge];
        }

        if (rebuild)
            columns.insert_range(required_columns);
    }

    return columns;
}

/// The cap of a vertical merge whose horizontal stage merges `key_columns`, or `affordable_horizontal` when
/// so many columns are merged there, or expired (`expired_columns`, see `getColumnsExpiredByMerge`), that the
/// merge does not run vertically at all.
size_t getAffordablePartsToMergeVertically(
    const ChooseContext & ctx, const NameSet & key_columns, const NameSet & expired_columns, size_t affordable_horizontal)
{
    const NameSet key_columns_in_storage = getKeyColumnsInStorage(ctx.metadata_snapshot, key_columns);
    size_t num_expired_columns = 0;
    for (const auto & column : expired_columns)
        if (!key_columns_in_storage.contains(column))
            ++num_expired_columns;

    /// The merge merges at least one column even when the key is empty (`ORDER BY tuple()`), see
    /// `extractMergingAndGatheringColumns`.
    const size_t num_key_columns = std::max<size_t>(1, key_columns_in_storage.size());
    if (!tableCanMergeVertically(ctx, num_key_columns, num_expired_columns))
        return affordable_horizontal;
    return getAffordablePartsToMergeAtOnce(ctx, num_key_columns);
}

/// The cap of the child merge that merges the projection parts of `projection`, which a merge of `range` keeps
/// without rebuilding it (see `getColumnsOfProjectionsRebuiltByMerge`). The child merge runs after the parent
/// merge, so the price of the range is the higher of the two. It follows how
/// `MergeTask::MergeProjectionsStage::prepareProjections` sets up the child merge and how
/// `MergeTask::ExecuteAndFinalizeHorizontalPart::chooseMergeAlgorithm` chooses its algorithm: an aggregate
/// projection merges in the `Aggregating` mode, which always runs horizontally, and a normal projection merges
/// vertically by the sizes of its projection parts and the columns of its own sorting key.
size_t getAffordablePartsToMergeProjection(const ChooseContext & ctx, const ProjectionDescription & projection, PartsRangeView range)
{
    const auto & settings = ctx.merge_tree_settings;
    const auto & projection_metadata = *projection.metadata;

    const size_t num_columns = projection_metadata.getColumns().getAllPhysical().size();
    const size_t affordable_horizontal = getAffordablePartsToMergeAtOnce(ctx, num_columns);

    if (projection.type == ProjectionDescription::Type::Aggregate || ctx.merge_forced_horizontal
        || settings[MergeTreeSetting::enable_vertical_merge_algorithm] == 0
        || range.size() > RowSourcePart::MAX_PARTS)
        return affordable_horizontal;

    /// Several subcolumns of one column in the sorting key of the projection are merged as that one column.
    NameSet key_columns;
    key_columns.insert_range(projection_metadata.getColumnsRequiredForSortingKey());
    const size_t num_key_columns = std::max<size_t>(1, getKeyColumnsInStorage(projection_metadata, key_columns).size());
    if (num_columns < num_key_columns + settings[MergeTreeSetting::vertical_merge_algorithm_min_columns_to_activate])
        return affordable_horizontal;

    /// Like `FutureMergedMutatedPart::assign` on the projection parts: by their own levels and types, not those
    /// of the parent parts. Without `allow_vertical_merges_from_compact_to_wide_parts` every projection part has to
    /// be wide as well.
    const bool allow_compact_sources = settings[MergeTreeSetting::allow_vertical_merges_from_compact_to_wide_parts];
    size_t sum_rows = 0;
    size_t sum_bytes = 0;
    UInt32 max_level = 0;
    MergeTreeDataPartType part_type;
    for (const auto & part : range)
    {
        auto it = part.projection_sizes.find(projection.name);
        if (it == part.projection_sizes.end())
            return affordable_horizontal;
        if (!allow_compact_sources && it->second.part_type != MergeTreeDataPartType::Wide)
            return affordable_horizontal;
        sum_rows += it->second.rows;
        sum_bytes += it->second.size_uncompressed;
        max_level = std::max(max_level, it->second.level);
        part_type = std::min(part_type, it->second.part_type);
    }

    const auto part_format = ctx.data->choosePartFormat(sum_bytes, sum_rows, max_level + 1, &projection);
    part_type = std::min(part_type, part_format.part_type);
    if (part_type != MergeTreeDataPartType::Wide || part_format.storage_type != MergeTreeDataPartStorageType::Full)
        return affordable_horizontal;

    if (sum_rows < settings[MergeTreeSetting::vertical_merge_algorithm_min_rows_to_activate]
        || sum_bytes < settings[MergeTreeSetting::vertical_merge_algorithm_min_bytes_to_activate])
        return affordable_horizontal;

    return getAffordablePartsToMergeAtOnce(ctx, num_key_columns);
}

/// The memory-derived cap for the selectors that can shrink a candidate range (`SimpleMergeSelector`
/// considers every sub-range of a partition), as a `RangeFilter`: a range may exceed the cap of a
/// horizontal merge when it will merge vertically and fits the cap of a vertical merge instead. Vertical
/// merges hold only the key columns of every source part at once, so a wide table on a small server keeps
/// its full merge width for the large merges that go vertical, and is narrowed only for the small
/// horizontal ones - whose fixed cost is the very thing this estimate is about. Whether the columns of the
/// TTL expressions are merged on the horizontal stage depends on whether the range removes expired values,
/// so the cap of a vertical merge is taken per range from one of two precomputed values, and lowered further
/// for a range that recomputes the min-max index or rebuilds projections, whose columns are merged on the
/// horizontal stage as well, or that expires columns, which are not gathered and may leave too few columns to
/// merge vertically. The projections the range keeps by merging their projection parts are merged by child
/// merges after the parent one, and the range has to fit the cap of each of them as well.
/// Returns the caller's filter unchanged when there is nothing to cap.
IMergeSelector::RangeFilter capRangesByAffordableMemory(const ChooseContext & ctx, const IMergeSelector::RangeFilter & range_filter)
{
    /// The cap of every merge priced as a horizontal one, the parent merge and the merges of the projection parts.
    const size_t affordable_at_once = getAffordablePartsToMergeAtOnce(ctx);
    if (affordable_at_once == 0)
        return range_filter;

    const size_t affordable_horizontal = getAffordablePartsToMergeHorizontally(ctx);
    NameSet table_minmax_index_columns = getMinMaxIndexColumns(ctx);
    NameSet table_key_columns = getColumnsMergedOnHorizontalStageOfVerticalMergeSet(ctx, false);
    NameSet table_key_columns_removing_expired_values = getColumnsMergedOnHorizontalStageOfVerticalMergeSet(ctx, true);
    const size_t affordable_vertical = getAffordablePartsToMergeVertically(ctx, table_key_columns, {}, affordable_horizontal);
    const size_t affordable_vertical_removing_expired_values
        = getAffordablePartsToMergeVertically(ctx, table_key_columns_removing_expired_values, {}, affordable_horizontal);

    return [&ctx,
            range_filter,
            affordable_at_once,
            affordable_horizontal,
            key_columns = std::move(table_key_columns),
            key_columns_removing_expired_values = std::move(table_key_columns_removing_expired_values),
            minmax_index_columns = std::move(table_minmax_index_columns),
            affordable_vertical,
            affordable_vertical_removing_expired_values](PartsRangeView range)
    {
        if (range_filter && !range_filter(range))
            return false;
        if (range.size() <= affordable_at_once)
            return true;

        const bool removes_expired_values = rangeRemovesExpiredValues(ctx, range);
        const bool parent_merges_vertically = range.size() > affordable_horizontal;
        if (parent_merges_vertically)
        {
            /// The min-max index, projection and expired columns only lower the cap, so the cap without them is
            /// an upper bound.
            if (range.size() > (removes_expired_values ? affordable_vertical_removing_expired_values : affordable_vertical))
                return false;
            if (!predictVerticalMerge(ctx, range, removes_expired_values))
                return false;
        }

        const bool may_reduce_rows = rangeMayReduceRows(ctx, range, removes_expired_values);
        std::vector<const ProjectionDescription *> merged_projections;
        NameSet projection_columns = getColumnsOfProjectionsRebuiltByMerge(ctx, range, removes_expired_values, may_reduce_rows, merged_projections);

        /// The parent merge, when it does not fit the cap of a horizontal one and has to merge vertically.
        if (parent_merges_vertically)
        {
            const NameSet expired_columns = getColumnsExpiredByMerge(range);
            if (!projection_columns.empty() || (may_reduce_rows && !minmax_index_columns.empty()) || !expired_columns.empty())
            {
                NameSet range_key_columns = removes_expired_values ? key_columns_removing_expired_values : key_columns;
                if (may_reduce_rows)
                    range_key_columns.insert(minmax_index_columns.begin(), minmax_index_columns.end());
                range_key_columns.merge(projection_columns);
                if (range.size() > getAffordablePartsToMergeVertically(ctx, range_key_columns, expired_columns, affordable_horizontal))
                    return false;
            }
        }

        /// The merges of the projection parts, which run after the parent merge.
        for (const auto * projection : merged_projections)
            if (range.size() > getAffordablePartsToMergeProjection(ctx, *projection, range))
                return false;

        return true;
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
    simple_merge_settings.merge_selector_min_age_to_disable_right_tail_heuristic
        = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_min_age_to_disable_right_tail_heuristic];
    simple_merge_settings.base = static_cast<double>(ctx.merge_tree_settings[MergeTreeSetting::merge_selector_base]);
    simple_merge_settings.min_parts_to_merge_at_once = ctx.merge_tree_settings[MergeTreeSetting::min_parts_to_merge_at_once];

    simple_merge_settings.enable_heuristic_to_lower_max_parts_to_merge_at_once = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once];
    simple_merge_settings.heuristic_to_lower_max_parts_to_merge_at_once_exponent = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_heuristic_to_lower_max_parts_to_merge_at_once_exponent];
    simple_merge_settings.parts_to_throw_insert = ctx.merge_tree_settings[MergeTreeSetting::parts_to_throw_insert];
    simple_merge_settings.partitions_stats = &ctx.partitions_stats;

    simple_merge_settings.small_parts_threshold = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_small_parts_threshold];
    simple_merge_settings.small_parts_min_count = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_small_parts_min_count];
    simple_merge_settings.small_parts_max_age = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_small_parts_max_age];

    if (!ctx.merge_tree_settings[MergeTreeSetting::min_age_to_force_merge_on_partition_only])
        simple_merge_settings.min_age_to_force_merge = ctx.merge_tree_settings[MergeTreeSetting::min_age_to_force_merge_seconds];

    simple_merge_settings.min_partition_age_to_force_merge = ctx.merge_tree_settings[MergeTreeSetting::min_partition_age_to_force_merge_seconds];

    if (ctx.aggressive)
    {
        simple_merge_settings.base = 1;
        /// Aggressive selection (`OPTIMIZE` without `FINAL`) is an explicit user request that ignores
        /// part novelty, so the background anti-churn gate for fresh small parts must not apply to it.
        simple_merge_settings.small_parts_min_count = 0;
    }

    /// The memory-derived cap (see `capRangesByAffordableMemory`) rejects every range wider than it unless the
    /// range merges vertically, and the minimums reject every range narrower than them: a minimum above the cap
    /// would stop the table from compacting at all, or, for `small_parts_min_count`, until the small parts age.
    /// A range of `affordable` parts always passes the cap, so lower the minimums to it.
    if (const size_t affordable = getAffordablePartsToMergeAtOnce(ctx))
    {
        simple_merge_settings.min_parts_to_merge_at_once = std::min(simple_merge_settings.min_parts_to_merge_at_once, affordable);
        simple_merge_settings.small_parts_min_count = std::min(simple_merge_settings.small_parts_min_count, affordable);
    }

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
    StorageID storage_id_,
    bool merge_forced_horizontal_)
    : merge_constraints(std::move(merge_constraints_))
    , merge_with_ttl_allowed(merge_with_ttl_allowed_)
    , aggressive(aggressive_)
    , range_filter(std::move(range_filter_))
    , storage_id(std::move(storage_id_))
    , merge_forced_horizontal(merge_forced_horizontal_)
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
    const MergeTreeData * data,
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    bool can_use_ttl_merges,
    time_t current_time) const
{
    /// The merging mode of a storage that is not `MergeTreeData`: its merges keep every row.
    static const MergeTreeData::MergingParams no_merging_params{};

    ChooseContext ctx{
        .ranges = ranges,
        .partitions_stats = partitions_stats,
        .predicate = predicate,
        .range_filter = range_filter,
        .storage_id = storage_id,
        .merge_constraints = merge_constraints,
        .metadata_snapshot = *metadata_snapshot,
        .merge_tree_settings = *merge_tree_settings,
        .data = data,
        .merging_params = data ? data->merging_params : no_merging_params,
        .next_delete_times = next_delete_times,
        .next_recompress_times = next_recompress_times,
        .current_time = current_time,
        .aggressive = aggressive,
        .can_use_ttl_merges = can_use_ttl_merges,
        .merge_forced_horizontal = merge_forced_horizontal,
    };

    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && can_use_ttl_merges)
        if (auto choices = tryChooseTTLMerge(ctx); !choices.empty())
            return choices;

    return tryChooseRegularMerge(ctx);
}

}
