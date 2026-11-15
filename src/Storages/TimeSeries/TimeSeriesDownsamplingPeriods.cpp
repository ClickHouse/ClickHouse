#include <Storages/TimeSeries/TimeSeriesDownsamplingPeriods.h>

#include <Common/Exception.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>

#include <cctype>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
}

namespace
{

std::string_view trim(std::string_view text)
{
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front())))
        text.remove_prefix(1);
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back())))
        text.remove_suffix(1);
    return text;
}

UInt64 parseDurationSeconds(std::string_view text, std::string_view level_text)
{
    PrometheusQueryParsingUtil::DurationType duration;
    String error;
    if (!PrometheusQueryParsingUtil::tryParseDuration(text, /*duration_scale=*/ 0, duration, &error))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Cannot parse duration '{}' in level '{}' of `samples_downsampling_periods`: {}", text, level_text, error);

    if (duration.value <= 0 || static_cast<UInt64>(duration.value) > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Duration '{}' in level '{}' of `samples_downsampling_periods` must be in the range [1s, {}s]",
            text, level_text, std::numeric_limits<UInt32>::max());

    return static_cast<UInt64>(duration.value);
}

}

TimeSeriesDownsamplingPeriods TimeSeriesDownsamplingPeriods::parse(std::string_view text)
{
    TimeSeriesDownsamplingPeriods result;
    text = trim(text);
    if (text.empty())
        return result;

    if (text.contains('{') || text.contains('}'))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "`samples_downsampling_periods` does not accept a series filter, got '{}'", text);

    while (!text.empty())
    {
        const auto comma = text.find(',');
        const auto level_text = trim(text.substr(0, comma));
        text = (comma == std::string_view::npos) ? std::string_view{} : text.substr(comma + 1);
        if (level_text.empty())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "`samples_downsampling_periods` contains an empty level");

        const auto colon = level_text.find(':');
        if (colon == std::string_view::npos || level_text.find(':', colon + 1) != std::string_view::npos)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Level '{}' of `samples_downsampling_periods` must have the form offset:interval", level_text);

        TimeSeriesDownsamplingLevel level;
        level.offset_seconds = parseDurationSeconds(trim(level_text.substr(0, colon)), level_text);
        level.interval_seconds = parseDurationSeconds(trim(level_text.substr(colon + 1)), level_text);

        if (!result.levels.empty())
        {
            const auto & previous = result.levels.back();
            if (level.offset_seconds <= previous.offset_seconds)
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Offsets in `samples_downsampling_periods` must increase, but {}s follows {}s",
                    level.offset_seconds, previous.offset_seconds);
            if (level.interval_seconds % previous.interval_seconds != 0)
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Interval {}s in `samples_downsampling_periods` must be a multiple of the previous interval {}s",
                    level.interval_seconds, previous.interval_seconds);
        }

        result.levels.push_back(level);
    }

    return result;
}

void TimeSeriesDownsamplingPeriods::validateForTable(UInt64 bucket_step_seconds, UInt64 min_offset_seconds) const
{
    if (levels.empty())
        return;

    if (levels.front().offset_seconds < min_offset_seconds)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "The first offset in `samples_downsampling_periods` must be at least {}s, the retention of the recent samples table, got {}s",
            min_offset_seconds, levels.front().offset_seconds);

    for (const auto & level : levels)
    {
        if (bucket_step_seconds % level.interval_seconds != 0)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Interval {}s in `samples_downsampling_periods` must divide `samples_bucket_step_seconds` ({}s)",
                level.interval_seconds, bucket_step_seconds);
    }
}

}
