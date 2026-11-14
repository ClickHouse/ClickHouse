#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/ManualMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TrivialMergeSelector.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

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
    extern const MergeTreeSettingsUInt64 merge_selector_small_parts_threshold;
    extern const MergeTreeSettingsUInt64 merge_selector_small_parts_min_count;
    extern const MergeTreeSettingsUInt64 merge_selector_small_parts_max_age;
    extern const MergeTreeSettingsUInt64 merge_memory_estimate_per_source_part_column;
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
};

/// A merge keeps one block from every source part alive at the same time, and every column of every one
/// of those blocks costs at least a minimum allocation, even when the block holds a handful of rows. That
/// fixed cost is proportional to `source parts * columns` and does not shrink with the amount of data:
/// merging 100 parts of `system.metric_log`, which has over two thousand columns, takes about 700 MiB
/// before a single row is read.
///
/// On a server with little memory such a merge fails with `MEMORY_LIMIT_EXCEEDED`, its source parts stay
/// where they were, and the next selection round picks the same parts again - the table stops compacting
/// while its part count keeps growing. Narrow the merge instead, so that its fixed cost stays within a
/// sixteenth of the memory limit. Ordinary tables, and even very wide tables on a large server, keep the
/// configured `max_parts_to_merge_at_once`.
///
/// Every merge is priced as a horizontal one, which holds all columns of every source part at once: a
/// vertical merge holds only the key columns, so it is over-priced, which costs some merge width but never
/// the merge. The columns of the widest projection count as well, because its projection parts are merged
/// with the same number of sources.
///
/// Returns 0 when there is no cap.
size_t getAffordablePartsToMergeAtOnce(const ChooseContext & ctx)
{
    const size_t bytes_per_source_column = ctx.merge_tree_settings[MergeTreeSetting::merge_memory_estimate_per_source_part_column];
    if (bytes_per_source_column == 0)
        return 0;

    const Int64 memory_limit = total_memory_tracker.getHardLimit();
    if (memory_limit <= 0)
        return 0;

    size_t num_columns = ctx.metadata_snapshot.getColumns().getAllPhysical().size();
    for (const auto & projection : ctx.metadata_snapshot.getProjections())
        num_columns = std::max(num_columns, projection.metadata->getColumns().getAllPhysical().size());
    num_columns = std::max<size_t>(1, num_columns);

    /// Merges are background work that runs next to the queries and inserts the server is there for.
    static constexpr size_t inverse_share_of_the_limit = 16;

    /// Divide by the factors one after another instead of by their product, which could overflow for an
    /// absurdly large estimate. A merge of fewer than two parts makes no progress, so never narrow below that.
    return std::max<size_t>(
        2, static_cast<size_t>(memory_limit) / inverse_share_of_the_limit / num_columns / bytes_per_source_column);
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
    if (!ctx.merge_constraints.empty() && ctx.metadata_snapshot.hasAnyRecompressionTTL())
    {
        TTLRecompressMergeSelector recompress_ttl_selector(ctx.next_recompress_times, ctx.current_time);

        if (auto merge_ranges = recompress_ttl_selector.select(ctx.ranges, ctx.merge_constraints, ctx.range_filter); !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLRecompress);
    }

    return {};
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

    if (const size_t affordable = getAffordablePartsToMergeAtOnce(ctx))
    {
        /// A value of 0 means "no limit".
        const bool narrowed = simple_merge_settings.max_parts_to_merge_at_once == 0
            || simple_merge_settings.max_parts_to_merge_at_once > affordable;
        if (narrowed)
            simple_merge_settings.max_parts_to_merge_at_once = affordable;

        /// A minimum above the cap would stop the table from compacting at all, or, for
        /// `small_parts_min_count`, until the small parts age.
        simple_merge_settings.min_parts_to_merge_at_once = std::min(simple_merge_settings.min_parts_to_merge_at_once, affordable);
        simple_merge_settings.small_parts_min_count = std::min(simple_merge_settings.small_parts_min_count, affordable);

        /// The selector lowers the base for old parts and for partitions with many parts, but not below 2. A merge
        /// is accepted when the sum of its parts is at least the base times the largest of them, and for a few
        /// parts of unequal size that ratio stays below 2: two parts give less than 2, and parts that grow
        /// geometrically, like `[1 KiB, 16 MiB, 1 KiB, 64 MiB]` repeated, give less than 1.5 with up to four parts.
        /// The narrowed merge cannot take the many small parts that would raise the ratio, so no merge would ever
        /// be accepted in the background, and the table would accumulate parts until inserts are rejected.
        /// When the merge width is narrowed, lower the floor to 1, which accepts any merge, so that old parts
        /// and partitions with many parts are eventually merged regardless of how uneven the parts are.
        if (narrowed)
            simple_merge_settings.min_lowered_base = 1;
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
    switch (algorithm.value)
    {
        case MergeSelectorAlgorithm::SIMPLE:
            selector = std::make_shared<SimpleMergeSelector>(fillSimpleSettings(ctx));
            break;
        case MergeSelectorAlgorithm::STOCHASTIC_SIMPLE:
            selector = std::make_shared<SimpleMergeSelector>(fillSimpleStochasticSettings(ctx));
            break;
        case MergeSelectorAlgorithm::TRIVIAL:
        {
            /// The trivial selector merges a fixed number of parts and ignores `max_parts_to_merge_at_once`,
            /// but the memory-derived cap has to reach it too: otherwise it would keep selecting the same
            /// too-wide merge of a very wide table on a small server.
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
    };

    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && can_use_ttl_merges)
        if (auto choices = tryChooseTTLMerge(ctx); !choices.empty())
            return choices;

    return tryChooseRegularMerge(ctx);
}

}
