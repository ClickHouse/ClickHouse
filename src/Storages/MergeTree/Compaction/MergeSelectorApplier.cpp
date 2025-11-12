#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

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
    extern const MergeTreeSettingsBool ttl_only_drop_parts;
    extern const MergeTreeSettingsUInt64 parts_to_throw_insert;
    extern const MergeTreeSettingsMergeSelectorAlgorithm merge_selector_algorithm;
    extern const MergeTreeSettingsBool merge_selector_enable_heuristic_to_remove_small_parts_at_right;
    extern const MergeTreeSettingsFloat merge_selector_base;
    extern const MergeTreeSettingsUInt64 min_parts_to_merge_at_once;
    extern const MergeTreeSettingsBool apply_patches_on_merge;
}

namespace
{

struct ChooseContext
{
    const PartsRanges & ranges;
    const IMergePredicate & predicate;
    const IMergeSelector::RangeFilter & range_filter;
    const IMergeSelector::MergeSizes & max_merge_sizes;
    const StorageInMemoryMetadata & metadata_snapshot;
    const MergeTreeSettings & merge_tree_settings;
    const PartitionIdToTTLs & next_delete_times;
    const PartitionIdToTTLs & next_recompress_times;
    const time_t current_time;
    const bool aggressive;
    const bool choose_ttl_only_drop_parts;
};

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
    /// Delete parts - 1 priority
    if (!ctx.max_merge_sizes.empty())
    {
        /// The size of the completely expired part of TTL drop is not affected by the merge pressure and the size of the storage space
        std::vector<size_t> max_sizes(ctx.max_merge_sizes.size(), std::numeric_limits<size_t>::max());
        TTLPartDeleteMergeSelector drop_ttl_selector(ctx.next_delete_times, ctx.current_time);

        if (auto merge_ranges = drop_ttl_selector.select(ctx.ranges, max_sizes, ctx.range_filter); !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLDelete);
    }

    if (ctx.choose_ttl_only_drop_parts)
        return {};

    /// Delete rows - 2 priority
    if (!ctx.max_merge_sizes.empty() && !ctx.merge_tree_settings[MergeTreeSetting::ttl_only_drop_parts])
    {
        TTLRowDeleteMergeSelector delete_ttl_selector(ctx.next_delete_times, ctx.current_time);

        if (auto merge_ranges = delete_ttl_selector.select(ctx.ranges, ctx.max_merge_sizes, ctx.range_filter); !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLDelete);
    }

    /// Recompression - 3 priority
    if (!ctx.max_merge_sizes.empty() && ctx.metadata_snapshot.hasAnyRecompressionTTL())
    {
        TTLRecompressMergeSelector recompress_ttl_selector(ctx.next_recompress_times, ctx.current_time);

        if (auto merge_ranges = recompress_ttl_selector.select(ctx.ranges, ctx.max_merge_sizes, ctx.range_filter); !merge_ranges.empty())
            return pack(ctx, std::move(merge_ranges), MergeType::TTLRecompress);
    }

    return {};
}

MergeSelectorChoices tryChooseRegularMerge(const ChooseContext & ctx)
{
    const auto algorithm = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_algorithm];

    std::any merge_settings;
    if (algorithm == MergeSelectorAlgorithm::SIMPLE || algorithm == MergeSelectorAlgorithm::STOCHASTIC_SIMPLE)
    {
        SimpleMergeSelector::Settings simple_merge_settings;
        /// Override value from table settings
        simple_merge_settings.window_size = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_window_size];
        simple_merge_settings.max_parts_to_merge_at_once = ctx.merge_tree_settings[MergeTreeSetting::max_parts_to_merge_at_once];
        simple_merge_settings.enable_heuristic_to_remove_small_parts_at_right = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_enable_heuristic_to_remove_small_parts_at_right];
        simple_merge_settings.base = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_base];
        simple_merge_settings.min_parts_to_merge_at_once = ctx.merge_tree_settings[MergeTreeSetting::min_parts_to_merge_at_once];

        if (!ctx.merge_tree_settings[MergeTreeSetting::min_age_to_force_merge_on_partition_only])
            simple_merge_settings.min_age_to_force_merge = ctx.merge_tree_settings[MergeTreeSetting::min_age_to_force_merge_seconds];

        if (ctx.aggressive)
            simple_merge_settings.base = 1;

        if (algorithm == MergeSelectorAlgorithm::STOCHASTIC_SIMPLE)
        {
            simple_merge_settings.parts_to_throw_insert = ctx.merge_tree_settings[MergeTreeSetting::parts_to_throw_insert];
            simple_merge_settings.blurry_base_scale_factor = ctx.merge_tree_settings[MergeTreeSetting::merge_selector_blurry_base_scale_factor];
            simple_merge_settings.use_blurry_base = simple_merge_settings.blurry_base_scale_factor != 0;
            simple_merge_settings.enable_stochastic_sliding = true;
        }

        merge_settings = simple_merge_settings;
    }

    auto merge_ranges = MergeSelectorFactory::instance().get(algorithm, merge_settings)->select(ctx.ranges, ctx.max_merge_sizes, ctx.range_filter);
    return pack(ctx, std::move(merge_ranges), MergeType::Regular);
}

}

MergeSelectorApplier::MergeSelectorApplier(
    std::vector<size_t> && max_merge_sizes_,
    bool merge_with_ttl_allowed_,
    bool aggressive_,
    IMergeSelector::RangeFilter range_filter_)
    : max_merge_sizes(std::move(max_merge_sizes_))
    , merge_with_ttl_allowed(merge_with_ttl_allowed_)
    , aggressive(aggressive_)
    , range_filter(std::move(range_filter_))
{
    chassert(!max_merge_sizes.empty(), "At least one merge size constraint should be passed");
    chassert(std::is_sorted(max_merge_sizes.rbegin(), max_merge_sizes.rend()), "Merge size constraints must be sorted in desc order");
}

MergeSelectorChoices MergeSelectorApplier::chooseMergesFrom(
    const PartsRanges & ranges,
    const IMergePredicate & predicate,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeSettingsPtr & merge_tree_settings,
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    bool can_use_ttl_merges,
    time_t current_time,
    bool choose_ttl_only_drop_parts) const
{
    ChooseContext ctx{
        .ranges = ranges,
        .predicate = predicate,
        .range_filter = range_filter,
        .max_merge_sizes = max_merge_sizes,
        .metadata_snapshot = *metadata_snapshot,
        .merge_tree_settings = *merge_tree_settings,
        .next_delete_times = next_delete_times,
        .next_recompress_times = next_recompress_times,
        .current_time = current_time,
        .aggressive = aggressive,
        .choose_ttl_only_drop_parts = choose_ttl_only_drop_parts,
    };

    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && can_use_ttl_merges)
        if (auto choices = tryChooseTTLMerge(ctx); !choices.empty() || choose_ttl_only_drop_parts)
            return choices;

    return tryChooseRegularMerge(ctx);
}

}
