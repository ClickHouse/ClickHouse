#include "PrometheusMetricsWriter.h"

#include <IO/WriteHelpers.h>
#include <Common/re2.h>
#include <algorithm>

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

}


namespace DB
{

PrometheusMetricsWriter::PrometheusMetricsWriter(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_name,
    const AsynchronousMetrics & async_metrics_)
    : async_metrics(async_metrics_)
    , send_events(config.getBool(config_name + ".events", true))
    , send_metrics(config.getBool(config_name + ".metrics", true))
    , send_asynchronous_metrics(config.getBool(config_name + ".asynchronous_metrics", true))
    , send_errors(config.getBool(config_name + ".errors", true))
{
}

void PrometheusMetricsWriter::write(WriteBuffer & wb) const
{
    if (send_events)
    {
        for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        {
            const auto counter = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);

            std::string metric_name{ProfileEvents::getName(static_cast<ProfileEvents::Event>(i))};
            std::string metric_doc{ProfileEvents::getDocumentation(static_cast<ProfileEvents::Event>(i))};

            convertHelpToSingleLine(metric_doc);

            if (!replaceInvalidChars(metric_name))
                continue;
            std::string key{profile_events_prefix + metric_name};

            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, "counter");
            writeOutLine(wb, key, counter);
        }
    }

    if (send_metrics)
    {
        for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        {
            const auto value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

            std::string metric_name{CurrentMetrics::getName(static_cast<CurrentMetrics::Metric>(i))};
            std::string metric_doc{CurrentMetrics::getDocumentation(static_cast<CurrentMetrics::Metric>(i))};

            convertHelpToSingleLine(metric_doc);

            if (!replaceInvalidChars(metric_name))
                continue;
            std::string key{current_metrics_prefix + metric_name};

            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, "gauge");
            writeOutLine(wb, key, value);
        }
    }

    if (send_asynchronous_metrics)
    {
        auto async_metrics_values = async_metrics.getValues();
        for (const auto & name_value : async_metrics_values)
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

    if (send_errors)
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

}

}
