#include <Parsers/Prometheus/CreateQueryTimeSeriesSettings.h>

#include <Core/SettingsFields.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/Prometheus/TimeSeriesVersion.h>


namespace DB
{

namespace
{
    /// Returns the value of a setting from the SETTINGS clause of a CREATE query, or null if the query doesn't specify it.
    const Field * tryGetSettingValue(const ASTCreateQuery & query, std::string_view setting_name)
    {
        if (!query.storage || !query.storage->settings)
            return nullptr;
        return query.storage->settings->changes.tryGet(setting_name);
    }
}


UInt64 getTimeSeriesVersion(const ASTCreateQuery & query)
{
    const auto * value = tryGetSettingValue(query, "version");
    if (!value)
        return TimeSeriesVersion::LATEST;

    /// The same conversion as in the `version` setting itself, so that every value the setting accepts (e.g. a string literal) is recognized here too.
    return SettingFieldUInt64{*value}.value;
}


bool hasExplicitTimeSeriesVersion(const ASTCreateQuery & query)
{
    return tryGetSettingValue(query, "version") != nullptr;
}


void setTimeSeriesVersion(ASTCreateQuery & query, UInt64 version)
{
    if (!query.storage)
        query.set(query.storage, make_intrusive<ASTStorage>());

    if (!query.storage->settings)
    {
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        query.storage->set(query.storage->settings, settings_ast);
    }

    query.storage->settings->changes.setSetting("version", Field{version});
}


UInt64 getTimeSeriesRecentSamplesTTL(const ASTCreateQuery & query, bool for_restore)
{
    if (const auto * value = tryGetSettingValue(query, "recent_samples_ttl_seconds"))
    {
        /// The same conversion as in the `recent_samples_ttl_seconds` setting itself.
        return SettingFieldUInt64{*value}.value;
    }

    if (for_restore)
    {
        /// A backup without the setting was made before the setting existed, when the table had no recent samples table.
        return 0;
    }

    return detail::TIME_SERIES_RECENT_SAMPLES_TTL_SECONDS_DEFAULT;
}


bool hasExplicitTimeSeriesRecentSamplesTTL(const ASTCreateQuery & query)
{
    return tryGetSettingValue(query, "recent_samples_ttl_seconds") != nullptr;
}


bool isTimeSeriesRecentSamplesTargetEnabled(const ASTCreateQuery & query, bool for_restore)
{
    /// The stored definition of an existing table declares the target by its RECENT SAMPLES clauses.
    if (hasTimeSeriesTargetDefinition(query, ViewTarget::RecentSamples) && !hasExplicitTimeSeriesRecentSamplesTTL(query))
        return true;

    return getTimeSeriesRecentSamplesTTL(query, for_restore) != 0;
}


bool isTimeSeriesTimeRangesTargetEnabled(const ASTCreateQuery & query, bool for_restore)
{
    if (const auto * value = tryGetSettingValue(query, "store_time_ranges"))
    {
        /// The same conversion as in the `store_time_ranges` setting itself.
        return SettingFieldBool{*value}.value;
    }

    /// The stored definition of an existing table declares the target by its TIME RANGES clauses.
    if (hasTimeSeriesTargetDefinition(query, ViewTarget::TimeRanges))
        return true;

    if (getTimeSeriesVersion(query) < TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET)
        return false;

    /// A backup without the setting and without the clauses was made by a version without the time ranges table.
    if (for_restore)
        return false;

    return detail::TIME_SERIES_STORE_TIME_RANGES_DEFAULT;
}


bool hasTimeSeriesTargetDefinition(const ASTCreateQuery & query, ViewTarget::Kind kind)
{
    return (query.getTargetInnerColumns(kind) != nullptr) || (query.getTargetInnerEngine(kind) != nullptr) || query.hasTargetTableID(kind);
}


size_t countTimeSeriesInnerTables(const ASTCreateQuery & query, bool for_restore)
{
    /// An external target table exists already, an inner one is created by the storage.
    auto count_inner_table = [&](ViewTarget::Kind target_kind) -> size_t { return query.hasTargetTableID(target_kind) ? 0 : 1; };

    size_t result = count_inner_table(ViewTarget::Samples) + count_inner_table(ViewTarget::Tags) + count_inner_table(ViewTarget::MetricFamilies);
    if (isTimeSeriesRecentSamplesTargetEnabled(query, for_restore))
        result += count_inner_table(ViewTarget::RecentSamples);
    if (isTimeSeriesTimeRangesTargetEnabled(query, for_restore))
        result += count_inner_table(ViewTarget::TimeRanges);
    return result;
}

}
