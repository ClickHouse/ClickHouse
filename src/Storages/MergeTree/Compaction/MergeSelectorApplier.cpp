#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>

#include <Common/logger_useful.h>

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

std::optional<MergeSelectorChoice> tryChooseTTLMerge(
    const MergeSelectorApplier & applier,
    const PartsRanges & ranges,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeSettingsPtr & data_settings,
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    time_t current_time)
{
    /// Delete parts - 1 priority
    {
        const size_t max_size = (*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_max_space_in_pool];
        TTLPartDeleteMergeSelector drop_ttl_selector(next_delete_times, current_time);

        /// The size of the completely expired part of TTL drop is not affected by the merge pressure and the size of the storage space
        if (auto parts = drop_ttl_selector.select(ranges, max_size, applier.range_filter); !parts.empty())
            return MergeSelectorChoice{std::move(parts), MergeType::TTLDelete};
    }

    /// Delete rows - 2 priority
    if (!(*data_settings)[MergeTreeSetting::ttl_only_drop_parts])
    {
        TTLRowDeleteMergeSelector delete_ttl_selector(next_delete_times, current_time);

        if (auto parts = delete_ttl_selector.select(ranges, applier.max_total_size_to_merge, applier.range_filter); !parts.empty())
            return MergeSelectorChoice{std::move(parts), MergeType::TTLDelete};
    }

    /// Recompression - 3 priority
    if (metadata_snapshot->hasAnyRecompressionTTL())
    {
        TTLRecompressMergeSelector recompress_ttl_selector(next_recompress_times, current_time);

        if (auto parts = recompress_ttl_selector.select(ranges, applier.max_total_size_to_merge, applier.range_filter); !parts.empty())
            return MergeSelectorChoice{std::move(parts), MergeType::TTLRecompress};
    }

    return std::nullopt;
}

std::optional<MergeSelectorChoice> tryChooseRegularMerge(
    const MergeSelectorApplier & applier,
    const PartsRanges & ranges,
    const MergeTreeSettingsPtr & data_settings)
{
    const auto algorithm = (*data_settings)[MergeTreeSetting::merge_selector_algorithm];

    std::any merge_settings;
    if (algorithm == MergeSelectorAlgorithm::SIMPLE || algorithm == MergeSelectorAlgorithm::STOCHASTIC_SIMPLE)
    {
        SimpleMergeSelector::Settings simple_merge_settings;
        /// Override value from table settings
        simple_merge_settings.window_size = (*data_settings)[MergeTreeSetting::merge_selector_window_size];
        simple_merge_settings.max_parts_to_merge_at_once = (*data_settings)[MergeTreeSetting::max_parts_to_merge_at_once];
        simple_merge_settings.enable_heuristic_to_remove_small_parts_at_right = (*data_settings)[MergeTreeSetting::merge_selector_enable_heuristic_to_remove_small_parts_at_right];
        simple_merge_settings.base = (*data_settings)[MergeTreeSetting::merge_selector_base];
        simple_merge_settings.min_parts_to_merge_at_once = (*data_settings)[MergeTreeSetting::min_parts_to_merge_at_once];

        if (!(*data_settings)[MergeTreeSetting::min_age_to_force_merge_on_partition_only])
            simple_merge_settings.min_age_to_force_merge = (*data_settings)[MergeTreeSetting::min_age_to_force_merge_seconds];

        if (applier.aggressive)
            simple_merge_settings.base = 1;

        if (algorithm == MergeSelectorAlgorithm::STOCHASTIC_SIMPLE)
        {
            simple_merge_settings.parts_to_throw_insert = (*data_settings)[MergeTreeSetting::parts_to_throw_insert];
            simple_merge_settings.blurry_base_scale_factor = (*data_settings)[MergeTreeSetting::merge_selector_blurry_base_scale_factor];
            simple_merge_settings.use_blurry_base = simple_merge_settings.blurry_base_scale_factor != 0;
            simple_merge_settings.enable_stochastic_sliding = true;
        }

        merge_settings = simple_merge_settings;
    }

    auto parts = MergeSelectorFactory::instance().get(algorithm, merge_settings)->select(ranges, applier.max_total_size_to_merge, applier.range_filter);

    /// Do not allow to "merge" part with itself for regular merges, unless it is a TTL-merge where it is ok to remove some values with expired ttl
    if (parts.size() == 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge selector returned only one part to merge");

    if (!parts.empty())
        return MergeSelectorChoice{std::move(parts), MergeType::Regular};

    return std::nullopt;
}

}

std::optional<MergeSelectorChoice> MergeSelectorApplier::chooseMergeFrom(
    const PartsRanges & ranges,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeSettingsPtr & data_settings,
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    bool can_use_ttl_merges,
    time_t current_time) const
{
    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && can_use_ttl_merges)
        if (auto choice = tryChooseTTLMerge(*this, ranges, metadata_snapshot, data_settings, next_delete_times, next_recompress_times, current_time))
            return choice;

    if (auto choice = tryChooseRegularMerge(*this, ranges, data_settings))
        return choice;

    return std::nullopt;
}

}
