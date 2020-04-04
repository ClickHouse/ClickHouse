#include "PrometheusMetricsWriter.h"

#include <algorithm>

#include <IO/WriteHelpers.h>
#include <Common/StatusInfo.h>
#include <boost/algorithm/string/replace.hpp>

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

void replaceInvalidChars(std::string & metric_name)
{
    std::replace(metric_name.begin(), metric_name.end(), '.', '_');
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
    , send_status_info(config.getBool(config_name + ".status_info", true))
{
}

void PrometheusMetricsWriter::write(WriteBuffer & wb) const
{
    if (send_events)
    {
        for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
        {
            const auto counter = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);

            std::string metric_name{ProfileEvents::getName(static_cast<ProfileEvents::Event>(i))};
            std::string metric_doc{ProfileEvents::getDocumentation(static_cast<ProfileEvents::Event>(i))};

            replaceInvalidChars(metric_name);
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

            replaceInvalidChars(metric_name);
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

            replaceInvalidChars(key);
            auto value = name_value.second;

            // TODO: add HELP section? asynchronous_metrics contains only key and value
            writeOutLine(wb, "# TYPE", key, "gauge");
            writeOutLine(wb, key, value);
        }
    }

    if (send_status_info)
    {
        for (size_t i = 0, end = CurrentStatusInfo::end(); i < end; ++i)
        {
            std::lock_guard<std::mutex> lock(CurrentStatusInfo::locks[static_cast<CurrentStatusInfo::Status>(i)]);
            std::string metric_name{CurrentStatusInfo::getName(static_cast<CurrentStatusInfo::Status>(i))};
            std::string metric_doc{CurrentStatusInfo::getDocumentation(static_cast<CurrentStatusInfo::Status>(i))};

            replaceInvalidChars(metric_name);
            std::string key{current_status_prefix + metric_name};

            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, "gauge");

            for (const auto & value: CurrentStatusInfo::values[i])
            {
                for (const auto & enum_value: CurrentStatusInfo::getAllPossibleValues(static_cast<CurrentStatusInfo::Status>(i)))
                {
                    DB::writeText(key, wb);
                    DB::writeChar('{', wb);
                    DB::writeText(key, wb);
                    DB::writeChar('=', wb);
                    writeDoubleQuotedString(enum_value.first, wb);
                    DB::writeText(",name=", wb);
                    writeDoubleQuotedString(value.first, wb);
                    DB::writeText("} ", wb);
                    DB::writeText(value.second == enum_value.second, wb);
                    DB::writeChar('\n', wb);
                }
            }
        }
    }
}

}
