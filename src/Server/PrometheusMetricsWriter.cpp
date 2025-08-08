#include "PrometheusMetricsWriter.h"

#include <Common/AsynchronousMetrics.h>
#include <Common/CurrentMetrics.h>
#include <Common/ErrorCodes.h>
#include <Common/re2.h>
#include <IO/WriteHelpers.h>

#include "config.h"


#if USE_NURAFT
namespace ProfileEvents
{
    extern const std::vector<Event> keeper_profile_events;
}

namespace CurrentMetrics
{
    extern const std::vector<Metric> keeper_metrics;
}
#endif


namespace
{

template <typename T>
void writeOutLine(DB::WriteBuffer & wb, T && val)
{
    DB::writeText(std::forward<T>(val), wb);
    DB::writeChar('\n', wb);
}

template <typename T, typename... TArgs>
void writeOutLine(DB::WriteBuffer & wb, T && val, TArgs &&... args)
{
    DB::writeText(std::forward<T>(val), wb);
    DB::writeChar(' ', wb);
    writeOutLine(wb, std::forward<TArgs>(args)...);
}

/// Returns false if name is not valid
bool replaceInvalidChars(std::string & metric_name)
{
    /// dirty solution:
    static const re2::RE2 regexp1("[^a-zA-Z0-9_:]");
    static const re2::RE2 regexp2("^[^a-zA-Z]*");
    re2::RE2::GlobalReplace(&metric_name, regexp1, "_");
    re2::RE2::GlobalReplace(&metric_name, regexp2, "");
    return !metric_name.empty();
}

void convertHelpToSingleLine(std::string & help)
{
    std::replace(help.begin(), help.end(), '\n', ' ');
}

constexpr auto profile_events_prefix = "ClickHouseProfileEvents_";
constexpr auto current_metrics_prefix = "ClickHouseMetrics_";
constexpr auto asynchronous_metrics_prefix = "ClickHouseAsyncMetrics_";
constexpr auto error_metrics_prefix = "ClickHouseErrorMetric_";

void writeEvent(DB::WriteBuffer & wb, ProfileEvents::Event event)
{
    const auto counter = ProfileEvents::global_counters[event].load(std::memory_order_relaxed);

    std::string metric_name{ProfileEvents::getName(static_cast<ProfileEvents::Event>(event))};
    std::string metric_doc{ProfileEvents::getDocumentation(static_cast<ProfileEvents::Event>(event))};

    convertHelpToSingleLine(metric_doc);

    if (!replaceInvalidChars(metric_name))
        return;

    std::string key{profile_events_prefix + metric_name};

    writeOutLine(wb, "# HELP", key, metric_doc);
    writeOutLine(wb, "# TYPE", key, "counter");
    writeOutLine(wb, key, counter);
}

void writeMetric(DB::WriteBuffer & wb, size_t metric)
{
    const auto value = CurrentMetrics::values[metric].load(std::memory_order_relaxed);

    std::string metric_name{CurrentMetrics::getName(static_cast<CurrentMetrics::Metric>(metric))};
    std::string metric_doc{CurrentMetrics::getDocumentation(static_cast<CurrentMetrics::Metric>(metric))};

    convertHelpToSingleLine(metric_doc);

    if (!replaceInvalidChars(metric_name))
        return;

    std::string key{current_metrics_prefix + metric_name};

    writeOutLine(wb, "# HELP", key, metric_doc);
    writeOutLine(wb, "# TYPE", key, "gauge");
    writeOutLine(wb, key, value);
}

void writeAsyncMetrics(DB::WriteBuffer & wb, const DB::AsynchronousMetricValues & values)
{
    for (const auto & name_value : values)
    {
        std::string key{asynchronous_metrics_prefix + name_value.first};

        if (!replaceInvalidChars(key))
            continue;

        auto value = name_value.second;

        std::string metric_doc{value.documentation};
        convertHelpToSingleLine(metric_doc);

        writeOutLine(wb, "# HELP", key, metric_doc);
        writeOutLine(wb, "# TYPE", key, "gauge");
        writeOutLine(wb, key, value.value);
    }
}

}


namespace DB
{

void PrometheusMetricsWriter::writeEvents(WriteBuffer & wb) const
{
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        writeEvent(wb, i);
}

void PrometheusMetricsWriter::writeMetrics(WriteBuffer & wb) const
{
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        writeMetric(wb, i);
}

void PrometheusMetricsWriter::writeAsynchronousMetrics(WriteBuffer & wb, const AsynchronousMetrics & async_metrics) const
{
    writeAsyncMetrics(wb, async_metrics.getValues());
}

void PrometheusMetricsWriter::writeErrors(WriteBuffer & wb) const
{
    size_t total_count = 0;

    for (size_t i = 0, end = ErrorCodes::end(); i < end; ++i)
    {
        const auto & error = ErrorCodes::values[i].get();
        std::string_view name = ErrorCodes::getName(static_cast<ErrorCodes::ErrorCode>(i));

        if (name.empty())
            continue;

        std::string key{error_metrics_prefix + toString(name)};
        std::string help = fmt::format("The number of {} errors since last server restart", name);

        writeOutLine(wb, "# HELP", key, help);
        writeOutLine(wb, "# TYPE", key, "counter");
        /// We are interested in errors which are happened only on this server.
        writeOutLine(wb, key, error.local.count);

        total_count += error.local.count;
    }

    /// Write the total number of errors as a separate metric
    std::string key{error_metrics_prefix + toString("ALL")};
    writeOutLine(wb, "# HELP", key, "The total number of errors since last server restart");
    writeOutLine(wb, "# TYPE", key, "counter");
    writeOutLine(wb, key, total_count);
}


void KeeperPrometheusMetricsWriter::writeEvents([[maybe_unused]] WriteBuffer & wb) const
{
#if USE_NURAFT
    for (auto event : ProfileEvents::keeper_profile_events)
        writeEvent(wb, event);
#endif
}

void KeeperPrometheusMetricsWriter::writeMetrics([[maybe_unused]] WriteBuffer & wb) const
{
#if USE_NURAFT
    for (auto metric : CurrentMetrics::keeper_metrics)
        writeMetric(wb, metric);
#endif
}

void KeeperPrometheusMetricsWriter::writeAsynchronousMetrics([[maybe_unused]] WriteBuffer & wb,
                                                             [[maybe_unused]] const AsynchronousMetrics & async_metrics) const
{
#if USE_NURAFT
    writeAsyncMetrics(wb, async_metrics.getValues());
#endif
}

void KeeperPrometheusMetricsWriter::writeErrors(WriteBuffer &) const
{
}

}
