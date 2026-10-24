#pragma once

#include <Parsers/ASTViewTargets.h>

#include <algorithm>
#include <array>


namespace DB
{

/// All target kinds of a TimeSeries table, in the canonical order.
constexpr const auto & getTimeSeriesTargetKinds()
{
    static constexpr auto kinds = std::array{
        ViewTarget::Samples,
        ViewTarget::RecentSamples,
        ViewTarget::Tags,
        ViewTarget::TimeRanges,
        ViewTarget::MetricFamilies,
        ViewTarget::Histograms};

    return kinds;
}

/// Whether a target of the given kind can be absent:
/// - the recent samples target is a choice, disabled by the `recent_samples_ttl_seconds` setting for any version;
/// - the histograms target is mandatory from version `TimeSeriesVersion::MIN_WITH_HISTOGRAMS_TARGET` (which the
///   normalization enforces) and absent only in tables of the earlier versions.
constexpr bool isOptionalTimeSeriesTarget(ViewTarget::Kind target_kind)
{
    static constexpr auto optional_targets = std::array{
        ViewTarget::RecentSamples,
        ViewTarget::TimeRanges,
        ViewTarget::Histograms};

    return std::ranges::find(optional_targets, target_kind) != optional_targets.end();
}

}
