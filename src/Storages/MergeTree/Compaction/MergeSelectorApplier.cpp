#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/ManualMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TrivialMergeSelector.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <Common/logger_useful.h>

#include <algorithm>
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
    const PartitionIdToTTLs & next_delete_times;
    const PartitionIdToTTLs & next_recompress_times;
    const time_t current_time;
    const bool aggressive;
    /// See `rowTTLNeedsWholePartitionMerge`.
    const bool row_ttl_needs_whole_partition;
};

MergeSelectorChoice createChoice(const ChooseContext & ctx, PartsRange && parts, MergeType merge_type)
{
    const bool apply_patch_parts = ctx.merge_tree_settings[MergeTreeSetting::apply_patches_on_merge];
    PartsRange patch_parts = apply_patch_parts ? ctx.predicate.getPatchesToApplyOnMerge(parts) : PartsRange{};
    return MergeSelectorChoice{std::move(parts), std::move(patch_parts), merge_type};
}

MergeSelectorChoices pack(const ChooseContext & ctx, PartsRanges && ranges, MergeType type)
{
    MergeSelectorChoices choices;
    choices.reserve(ranges.size());

    for (auto & range : ranges)
        choices.push_back(createChoice(ctx, std::move(range), type));

    return choices;
}

MergeSelectorChoices tryChooseRecompressTTLMerge(const ChooseContext & ctx)
{
    if (!ctx.merge_constraints.empty() && ctx.metadata_snapshot.hasAnyRecompressionTTL())
    {
        TTLRecompressMergeSelector recompress_ttl_selector(ctx.next_recompress_times, ctx.current_time);

        if (auto merge_ranges = recompress_ttl_selector.select(ctx.ranges, ctx.merge_constraints, ctx.range_filter);
            !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLRecompress);
    }

    return {};
}

/// For a table where `rowTTLNeedsWholePartitionMerge` holds. `MergeTask` deletes rows by row TTL only in `TTLDrop` and
/// `TTLDelete` merges, so every such merge of the table, the ones for column TTL included, is assigned here, and only
/// for a range that holds every part of its partition.
MergeSelectorChoices tryChooseWholePartitionTTLMerge(const ChooseContext & ctx)
{
    struct Candidate
    {
        const PartsRange * range{nullptr};
        MergeType type{MergeType::TTLDelete};
        time_t due{0};
    };

    const bool ttl_only_drop_parts = ctx.merge_tree_settings[MergeTreeSetting::ttl_only_drop_parts];
    const size_t max_parts = ctx.merge_tree_settings[MergeTreeSetting::max_parts_to_merge_at_once];
    /// A `TTLDrop` merge of such parts deletes every row, so it writes nothing, see `MergeTask`.
    const bool drop_writes_nothing = ctx.metadata_snapshot.hasOnlyRowsTTL();

    const auto earliest = [](time_t current, time_t candidate) { return current ? std::min(current, candidate) : candidate; };

    std::vector<Candidate> candidates;
    for (const auto & range : ctx.ranges)
    {
        chassert(!range.empty());
        const String & partition_id = range.front().info.getPartitionId();

        /// The partition statistics count every part of the partition, the ones that are being merged or mutated
        /// included, so a range with as many parts holds all of them.
        const auto stats = ctx.partitions_stats.find(partition_id);
        if (stats == ctx.partitions_stats.end() || stats->second.part_count != range.size())
            continue;

        bool all_parts_expired{true};
        bool avoid_merges{false};
        time_t rows_due{0};
        time_t columns_due{0};
        for (const auto & part : range)
        {
            avoid_merges = avoid_merges || part.is_in_volume_where_merges_avoid;

            const auto & ttl = part.general_ttl_info;
            if (!ttl || !ttl->has_any_non_finished_ttls || !ttl->part_max_ttl || ttl->part_max_ttl > ctx.current_time)
                all_parts_expired = false;

            if (ttl && ttl->has_any_non_finished_row_ttls && ttl->part_min_ttl && ttl->part_min_ttl <= ctx.current_time)
                rows_due = earliest(rows_due, ttl->part_min_ttl);

            if (ttl && ttl->has_any_non_finished_column_ttls && ttl->column_min_ttl && ttl->column_min_ttl <= ctx.current_time)
                columns_due = earliest(columns_due, ttl->column_min_ttl);
        }

        /// Like `TTLPartDropMergeSelector`, not postponed, and not limited in size or number of parts if the merge
        /// writes nothing.
        if (all_parts_expired && (drop_writes_nothing || !max_parts || range.size() <= max_parts))
        {
            candidates.push_back({&range, MergeType::TTLDrop, earliest(rows_due, columns_due)});
            continue;
        }

        time_t due = columns_due;
        if (!ttl_only_drop_parts && rows_due)
            due = earliest(due, rows_due);

        if (!due || avoid_merges)
            continue;

        if (auto it = ctx.next_delete_times.find(partition_id);
            it != ctx.next_delete_times.end() && it->second > ctx.current_time)
            continue;

        if (max_parts && range.size() > max_parts)
            continue;

        candidates.push_back({&range, MergeType::TTLDelete, due});
    }

    /// The partition whose TTL expired first goes first, as in `ITTLMergeSelector`.
    std::ranges::stable_sort(candidates, {}, &Candidate::due);

    MergeSelectorChoices choices;
    for (const auto & candidate : candidates)
    {
        if (choices.size() == ctx.merge_constraints.size())
            break;

        const auto & range = *candidate.range;
        const String & partition_id = range.front().info.getPartitionId();

        if (!(candidate.type == MergeType::TTLDrop && drop_writes_nothing))
        {
            const auto & constraint = ctx.merge_constraints[choices.size()];
            size_t bytes{0};
            size_t rows{0};
            for (const auto & part : range)
            {
                bytes += part.size;
                rows += part.rows;
            }

            if (bytes > constraint.max_size_bytes || rows > constraint.max_size_rows)
            {
                LOG_INFO(LogFrequencyLimiter(getLogger("MergeSelectorApplier"), 600),
                    "TTL of partition {} of table {} is due, but a merge of the whole partition ({} parts, {} bytes, {} rows) "
                    "exceeds the current limit ({} bytes, {} rows), see the setting `replacing_ttl_whole_partition_only`",
                    partition_id, ctx.storage_id.getNameForLogs(), range.size(), bytes, rows, constraint.max_size_bytes, constraint.max_size_rows);

                continue;
            }
        }

        /// A replica may not have all parts of the partition yet.
        if (auto covered = ctx.predicate.checkRangeCoversPartition(range); !covered.has_value())
        {
            LOG_TRACE(LogFrequencyLimiter(getLogger("MergeSelectorApplier"), 60),
                "Cannot assign a TTL merge of the whole partition {} of table {}: {}",
                partition_id, ctx.storage_id.getNameForLogs(), covered.error().text);
            continue;
        }

        if (ctx.range_filter && !ctx.range_filter(range))
            continue;

        choices.push_back(createChoice(ctx, PartsRange(range), candidate.type));
    }

    return choices;
}

MergeSelectorChoices tryChooseTTLMerge(const ChooseContext & ctx)
{
    if (ctx.row_ttl_needs_whole_partition)
    {
        if (auto choices = tryChooseWholePartitionTTLMerge(ctx); !choices.empty())
            return choices;

        /// A recompression merge keeps the expired rows, see `MergeTask`.
        return tryChooseRecompressTTLMerge(ctx);
    }

    /// Drop parts - 1 priority
    if (!ctx.merge_constraints.empty())
    {
        /// The size of the completely expired part of TTL drop is not affected by the merge pressure and the size of the storage space.
        std::vector<MergeConstraint> ttl_constraints(ctx.merge_constraints.size(), {std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max()});
        TTLPartDropMergeSelector drop_ttl_selector(ctx.current_time, ctx.merge_tree_settings[MergeTreeSetting::max_parts_to_merge_at_once]);

        if (auto merge_ranges = drop_ttl_selector.select(ctx.ranges, ttl_constraints, ctx.range_filter); !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLDrop);
    }

    /// Delete rows - 2 priority
    if (!ctx.merge_constraints.empty() && !ctx.merge_tree_settings[MergeTreeSetting::ttl_only_drop_parts])
    {
        TTLRowDeleteMergeSelector delete_ttl_selector(ctx.next_delete_times, ctx.current_time);

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
        TTLColumnDeleteMergeSelector delete_ttl_selector(ctx.next_delete_times, ctx.current_time);

        if (auto merge_ranges = delete_ttl_selector.select(ctx.ranges, ctx.merge_constraints, ctx.range_filter); !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLDelete);
    }

    /// Recompression - 4 priority
    return tryChooseRecompressTTLMerge(ctx);
}

SimpleMergeSelector::Settings fillSimpleSettings(const ChooseContext & ctx)
{
    SimpleMergeSelector::Settings simple_merge_settings;

    simple_merge_settings.window_size = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_window_size];
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
    switch (algorithm.value)
    {
        case MergeSelectorAlgorithm::SIMPLE:
            selector = std::make_shared<SimpleMergeSelector>(fillSimpleSettings(ctx));
            break;
        case MergeSelectorAlgorithm::STOCHASTIC_SIMPLE:
            selector = std::make_shared<SimpleMergeSelector>(fillSimpleStochasticSettings(ctx));
            break;
        case MergeSelectorAlgorithm::TRIVIAL:
            selector = std::make_shared<TrivialMergeSelector>();
            break;
        case MergeSelectorAlgorithm::MANUAL:
            selector = std::make_shared<ManualMergeSelector>(ctx.storage_id);
            break;
    }

    chassert(selector != nullptr);
    auto merge_ranges = selector->select(ctx.ranges, ctx.merge_constraints, ctx.range_filter);
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
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    bool can_use_ttl_merges,
    bool row_ttl_needs_whole_partition,
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
        .next_delete_times = next_delete_times,
        .next_recompress_times = next_recompress_times,
        .current_time = current_time,
        .aggressive = aggressive,
        .row_ttl_needs_whole_partition = row_ttl_needs_whole_partition,
    };

    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && can_use_ttl_merges)
        if (auto choices = tryChooseTTLMerge(ctx); !choices.empty())
            return choices;

    return tryChooseRegularMerge(ctx);
}

}
