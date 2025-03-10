#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
}

namespace
{

MergeSelectorChoices pack(PartsRanges && ranges, MergeType type)
{
    MergeSelectorChoices choices;
    choices.reserve(ranges.size());

    for (auto & range : ranges)
        choices.emplace_back(std::move(range), type);

    return choices;
}

struct ChooseContext
{
    const PartsRanges & ranges;
    const IMergeSelector::RangeFilter & range_filter;
    const IMergeSelector::MergeSizes & max_merge_sizes;
    const StorageInMemoryMetadata & metadata_snapshot;
    const MergeTreeSettings & merge_tree_settings;
    const PartitionIdToTTLs & next_delete_times;
    const PartitionIdToTTLs & next_recompress_times;
    const time_t current_time;
    const bool aggressive;
};

MergeSelectorChoices tryChooseTTLMerge(const ChooseContext ctx)
{
    /// Delete parts - 1 priority
    if (!ctx.max_merge_sizes.empty())
    {
        std::vector<size_t> max_sizes(ctx.max_merge_sizes.size(), ctx.merge_tree_settings[MergeTreeSetting::max_bytes_to_merge_at_max_space_in_pool]);
        TTLPartDeleteMergeSelector drop_ttl_selector(ctx.next_delete_times, ctx.current_time);

        /// The size of the completely expired part of TTL drop is not affected by the merge pressure and the size of the storage space
        if (auto merge_ranges = drop_ttl_selector.select(ctx.ranges, max_sizes, ctx.range_filter); !merge_ranges.empty())
            return pack(std::move(merge_ranges), MergeType::TTLDelete);
    }

    /// Delete rows - 2 priority
    if (!ctx.max_merge_sizes.empty() && !ctx.merge_tree_settings[MergeTreeSetting::ttl_only_drop_parts])
    {
        TTLRowDeleteMergeSelector delete_ttl_selector(ctx.next_delete_times, ctx.current_time);

        if (auto merge_ranges = delete_ttl_selector.select(ctx.ranges, ctx.max_merge_sizes, ctx.range_filter); !merge_ranges.empty())
            return pack(std::move(merge_ranges), MergeType::TTLDelete);
    }

    /// Recompression - 3 priority
    if (!ctx.max_merge_sizes.empty() && ctx.metadata_snapshot.hasAnyRecompressionTTL())
    {
        TTLRecompressMergeSelector recompress_ttl_selector(ctx.next_recompress_times, ctx.current_time);

        if (auto merge_ranges = recompress_ttl_selector.select(ctx.ranges, ctx.max_merge_sizes, ctx.range_filter); !merge_ranges.empty())
            return pack(std::move(merge_ranges), MergeType::TTLRecompress);
    }

    return {};
}

MergeSelectorChoices tryChooseRegularMerge(const ChooseContext ctx)
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
    return pack(std::move(merge_ranges), MergeType::Regular);
}

}

MergeSelectorChoices MergeSelectorApplier::chooseMergesFrom(
    const PartsRanges & ranges,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeSettingsPtr & merge_tree_settings,
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    bool can_use_ttl_merges,
    time_t current_time) const
{
    ChooseContext ctx{
        .ranges = ranges,
        .range_filter = range_filter,
        .max_merge_sizes = max_merge_sizes,
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
