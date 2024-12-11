#pragma once

#include <Common/logger_useful.h>

#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>

#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>

namespace ProfileEvents
{
    extern const Event MergerMutatorSelectPartsForMergeElapsedMicroseconds;
    extern const Event MergerMutatorSelectRangePartsCount;
}

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
}

std::optional<MergeSelectorChoice> MergeSelectorApplier::tryChooseTTLMerge(
    const PartsRanges & ranges,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeSettingsPtr & data_settings,
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    time_t current_time) const
{
    const size_t max_size = (*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_max_space_in_pool];
    TTLPartDeleteMergeSelector drop_ttl_selector(next_delete_times, current_time);

    /// The size of the completely expired part of TTL drop is not affected by the merge pressure and the size of the storage space
    if (auto parts = drop_ttl_selector.select(ranges, max_size); !parts.empty())
        return MergeSelectorChoice{std::move(parts), MergeType::TTLDelete};

    if (!(*data_settings)[MergeTreeSetting::ttl_only_drop_parts])
    {
        TTLRowDeleteMergeSelector delete_ttl_selector(next_delete_times, current_time);

        if (auto parts = delete_ttl_selector.select(ranges, max_total_size_to_merge); !parts.empty())
            return MergeSelectorChoice{std::move(parts), MergeType::TTLDelete};
    }

    if (metadata_snapshot->hasAnyRecompressionTTL())
    {
        TTLRecompressMergeSelector recompress_ttl_selector(next_recompress_times, current_time, metadata_snapshot->getRecompressionTTLs());

        if (auto parts = recompress_ttl_selector.select(ranges, max_total_size_to_merge); !parts.empty())
            return MergeSelectorChoice{std::move(parts), MergeType::TTLRecompress};
    }

    return std::nullopt;
}

std::optional<MergeSelectorChoice> MergeSelectorApplier::tryChooseRegularMerge(
    const PartsRanges & ranges,
    const MergeTreeSettingsPtr & data_settings) const
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

        if (aggressive)
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

    auto parts = MergeSelectorFactory::instance().get(algorithm, merge_settings)->select(ranges, max_total_size_to_merge);

    /// Do not allow to "merge" part with itself for regular merges, unless it is a TTL-merge where it is ok to remove some values with expired ttl
    if (parts.size() == 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge selector returned only one part to merge");

    return std::nullopt;
}

std::optional<MergeSelectorChoice> MergeSelectorApplier::chooseMergeFromImpl(
        const PartsRanges & ranges,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeSettingsPtr & data_settings,
        const PartitionIdToTTLs & next_delete_times,
        const PartitionIdToTTLs & next_recompress_times,
        bool can_use_ttl_merges,
        time_t current_time) const
{
    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && can_use_ttl_merges)
        if (auto choice = tryChooseTTLMerge(ranges, metadata_snapshot, data_settings, next_delete_times, next_recompress_times, current_time))
            return choice;

    if (auto choice = tryChooseRegularMerge(ranges, data_settings))
        return choice;

    return std::nullopt;
}

std::optional<MergeSelectorChoice> MergeSelectorApplier::chooseMergeFrom(
        const PartsRanges & ranges,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeSettingsPtr & data_settings,
        const PartitionIdToTTLs & next_delete_times,
        const PartitionIdToTTLs & next_recompress_times,
        bool can_use_ttl_merges,
        time_t current_time,
        LoggerPtr & log,
        PreformattedMessage & out_disable_reason) const
{
    Stopwatch select_parts_from_ranges_timer;

    auto choice = chooseMergeFromImpl(
        ranges, metadata_snapshot, data_settings, next_delete_times, next_recompress_times,
        can_use_ttl_merges, current_time);

    if (choice.has_value())
    {
        LOG_DEBUG(
            log,
            "Selected {} parts from {} to {} in {}ms",
            choice->range.size(),
            choice->range.front().part_info.getPartNameForLogs(),
            choice->range.back().part_info.getPartNameForLogs(),
            select_parts_from_ranges_timer.elapsedMicroseconds() / 1000);

        ProfileEvents::increment(ProfileEvents::MergerMutatorSelectRangePartsCount, choice->range.size());
        ProfileEvents::increment(ProfileEvents::MergerMutatorSelectPartsForMergeElapsedMicroseconds, select_parts_from_ranges_timer.elapsedMicroseconds());
    }
    else
    {
        out_disable_reason = PreformattedMessage::create("Did not find any parts to merge (with usual merge selectors) in {}ms", select_parts_from_ranges_timer.elapsedMicroseconds() / 1000);
        LOG_DEBUG(log, out_disable_reason);

        ProfileEvents::increment(ProfileEvents::MergerMutatorSelectPartsForMergeElapsedMicroseconds, select_parts_from_ranges_timer.elapsedMicroseconds());
    }

    return choice;
}

}
