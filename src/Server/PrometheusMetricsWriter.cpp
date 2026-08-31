#include <Server/PrometheusMetricsWriter.h>

#include <Common/HistogramMetrics.h>
#include <Common/DimensionalMetrics.h>
#include <Common/AsynchronousMetrics.h>
#include <unordered_set>
#include <Common/CurrentMetrics.h>
#include <Common/ErrorCodes.h>
#include <Common/re2.h>
#include <Common/config_version.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

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

namespace HistogramMetrics
{
    extern std::vector<MetricFamily *> keeper_histograms;
}

namespace DimensionalMetrics
{
    extern std::vector<MetricFamily *> keeper_dimensional_metrics;
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

template <typename T>
void writeMetricLine(DB::WriteBuffer & wb, const std::string & key, const std::string & labels_suffix, T && value)
{
    DB::writeText(key, wb);
    DB::writeText(labels_suffix, wb);
    DB::writeChar(' ', wb);
    writeOutLine(wb, std::forward<T>(value));
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
constexpr auto histogram_prefix = "ClickHouseHistogramMetrics_";
constexpr auto dimensional_metrics_prefix = "ClickHouseDimensionalMetrics_";

void writeEvent(DB::WriteBuffer & wb, ProfileEvents::Event event, const std::string & labels_suffix)
{
    const auto counter = ProfileEvents::global_counters[event];

    std::string metric_name{ProfileEvents::getName(static_cast<ProfileEvents::Event>(event))};
    std::string metric_doc{ProfileEvents::getDocumentation(static_cast<ProfileEvents::Event>(event))};

    convertHelpToSingleLine(metric_doc);

    if (!replaceInvalidChars(metric_name))
        return;

    std::string key{profile_events_prefix + metric_name};

    writeOutLine(wb, "# HELP", key, metric_doc);
    writeOutLine(wb, "# TYPE", key, "counter");
    writeMetricLine(wb, key, labels_suffix, counter);
}

void writeMetric(DB::WriteBuffer & wb, size_t metric, const std::string & labels_suffix)
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
    writeMetricLine(wb, key, labels_suffix, value);
}

void writeLabelValueEscaped(DB::WriteBuffer & wb, const std::string & value)
{
    for (char c : value)
    {
        if (c == '\\' || c == '"')
        {
            DB::writeChar('\\', wb);
            DB::writeChar(c, wb);
        }
        else if (c == '\n')
        {
            DB::writeChar('\\', wb);
            DB::writeChar('n', wb);
        }
        else
            DB::writeChar(c, wb);
    }
}

void writeAsyncMetrics(DB::WriteBuffer & wb, const DB::AsynchronousMetricValues & values,
    const std::string & constant_labels, const std::string & constant_labels_suffix)
{
    for (const auto & name_value : values)
    {
        std::string key{asynchronous_metrics_prefix + name_value.first};

        if (!replaceInvalidChars(key))
            continue;

        const auto & value = name_value.second;

        std::string metric_doc{value.documentation};
        convertHelpToSingleLine(metric_doc);

        writeOutLine(wb, "# HELP", key, metric_doc);
        writeOutLine(wb, "# TYPE", key, "gauge");

        if (value.isMap())
        {
            /// A key-value metric is exported as one line per key, with the key as a label,
            /// e.g. `ClickHouseAsyncMetrics_BlockReadBytes{device="sda"} 123`.
            std::string label_name{value.key_label};
            if (!replaceInvalidChars(label_name))
                continue;

            for (const auto & [map_key, map_value] : value.key_values)
            {
                DB::WriteBufferFromOwnString labels_wb;
                DB::writeChar('{', labels_wb);
                DB::writeText(constant_labels, labels_wb);
                if (!constant_labels.empty())
                    DB::writeChar(',', labels_wb);
                DB::writeText(label_name, labels_wb);
                DB::writeText("=\"", labels_wb);
                writeLabelValueEscaped(labels_wb, map_key);
                DB::writeText("\"}", labels_wb);

                writeMetricLine(wb, key, labels_wb.str(), map_value);
            }
        }
        else
            writeMetricLine(wb, key, constant_labels_suffix, value.value);
    }
}

}


namespace DB
{

PrometheusMetricsWriter::PrometheusMetricsWriter(const std::map<std::string, std::string> & constant_labels_)
{
    if (constant_labels_.empty())
        return;

    WriteBufferFromOwnString wb;
    bool first = true;
    for (const auto & [label_name, label_value] : constant_labels_)
    {
        if (!first)
            wb << ',';
        first = false;
        wb << label_name << '=';
        writeDoubleQuotedString(label_value, wb);
    }
    constant_labels = wb.str();
    constant_labels_suffix = "{" + constant_labels + "}";
}

void PrometheusMetricsWriter::writeEvents(WriteBuffer & wb) const
{
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
        writeEvent(wb, i, constant_labels_suffix);
}

void PrometheusMetricsWriter::writeMetrics(WriteBuffer & wb) const
{
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
        writeMetric(wb, i, constant_labels_suffix);
}

void PrometheusMetricsWriter::writeAsynchronousMetrics(WriteBuffer & wb, const AsynchronousMetrics & async_metrics) const
{
    writeAsyncMetrics(wb, async_metrics.getValues(), constant_labels, constant_labels_suffix);
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
        writeMetricLine(wb, key, constant_labels_suffix, error.local.count);

        total_count += error.local.count;
    }

    /// Write the total number of errors as a separate metric
    std::string key{error_metrics_prefix + toString("ALL")};
    writeOutLine(wb, "# HELP", key, "The total number of errors since last server restart");
    writeOutLine(wb, "# TYPE", key, "counter");
    writeMetricLine(wb, key, constant_labels_suffix, total_count);
}

void PrometheusMetricsWriter::writeHistogramMetric(WriteBuffer & wb, const HistogramMetrics::MetricFamily & family, const std::string & extra_labels)
{
    std::string base_name = histogram_prefix + family.getName();
    if (!replaceInvalidChars(base_name))
        return;

    std::string help_text = family.getDocumentation();
    convertHelpToSingleLine(help_text);

    writeOutLine(wb, "# HELP", base_name, help_text);
    writeOutLine(wb, "# TYPE", base_name, "histogram");

    family.forEachMetric([&wb, &family, &base_name, &extra_labels](const HistogramMetrics::LabelValues & label_values, const HistogramMetrics::Metric & metric)
    {
        const auto & buckets = family.getBuckets();
        const auto & labels = family.getLabels();
        HistogramMetrics::Metric::Counter cumulative_count = 0;

        for (size_t i = 0; i < buckets.size() + 1; ++i)
        {
            cumulative_count += metric.getCounter(i);

            wb << base_name << "_bucket{";

            if (!extra_labels.empty())
                wb << extra_labels << ',';

            for (size_t j = 0; j < labels.size(); ++j)
            {
                wb << labels[j] << '=';
                writeDoubleQuotedString(label_values[j], wb);
                wb << ',';
            }

            wb << "le=\"";
            if (i != buckets.size())
            {
                wb << buckets[i];
            }
            else
            {
                wb << "+Inf";
            }

            wb << "\"}" << ' ' << cumulative_count << '\n';
        }

        wb << base_name << "_count";
        if (!labels.empty() || !extra_labels.empty())
        {
            wb << '{' << extra_labels;
            for (size_t j = 0; j < labels.size(); ++j)
            {
                if (j != 0 || !extra_labels.empty())
                {
                    wb << ',';
                }
                wb << labels[j] << '=';
                writeDoubleQuotedString(label_values[j], wb);
            }
            wb << '}';
        }
        wb << ' ' << cumulative_count << '\n';

        wb << base_name << "_sum";
        if (!labels.empty() || !extra_labels.empty())
        {
            wb << '{' << extra_labels;
            for (size_t j = 0; j < labels.size(); ++j)
            {
                if (j > 0 || !extra_labels.empty())
                {
                    wb << ',';
                }
                wb << labels[j] << '=';
                writeDoubleQuotedString(label_values[j], wb);
            }
            wb << '}';
        }
        wb << ' ' << metric.getSum() << '\n';
    });
}

void PrometheusMetricsWriter::writeHistogramMetrics(WriteBuffer & wb) const
{
    HistogramMetrics::Factory::instance().forEachFamily([this, &wb](const HistogramMetrics::MetricFamily & family)
    {
        writeHistogramMetric(wb, family, constant_labels);
    });
}

void PrometheusMetricsWriter::writeDimensionalMetric(WriteBuffer & wb, const DimensionalMetrics::MetricFamily & family, const std::string & extra_labels)
{
    std::string base_name = dimensional_metrics_prefix + family.getName();
    if (!replaceInvalidChars(base_name))
        return;

    std::string help_text = family.getDocumentation();
    convertHelpToSingleLine(help_text);

    writeOutLine(wb, "# HELP", base_name, help_text);
    writeOutLine(wb, "# TYPE", base_name, family.getTypeString());

    family.forEachMetric([&wb, &family, &base_name, &extra_labels](const DimensionalMetrics::LabelValues & label_values, const DimensionalMetrics::Metric & metric)
    {
        wb << base_name;
        const auto & labels = family.getLabels();
        if (!labels.empty() || !extra_labels.empty())
        {
            wb << '{' << extra_labels;
            for (size_t i = 0; i < labels.size(); ++i)
            {
                if (i != 0 || !extra_labels.empty())
                {
                    wb << ',';
                }
                wb << labels[i] << '=';
                writeDoubleQuotedString(label_values[i], wb);
            }
            wb << '}';
        }
        wb << ' ' << metric.get() << '\n';
    });
}

void PrometheusMetricsWriter::writeDimensionalMetrics(WriteBuffer & wb) const
{
    DimensionalMetrics::Factory::instance().forEachFamily([this, &wb](const DimensionalMetrics::MetricFamily & family)
    {
        writeDimensionalMetric(wb, family, constant_labels);
    });
}

void PrometheusMetricsWriter::writeInfo(WriteBuffer & wb) const
{
    std::string key{"ClickHouse_Info"};

    writeOutLine(wb, "# HELP", key, "ClickHouse server information");
    writeOutLine(wb, "# TYPE", key, "gauge");

    wb << key << '{';
    if (!constant_labels.empty())
        wb << constant_labels << ',';
    wb << "name=\"" << VERSION_NAME << "\"";
    wb << ",version=\"" << VERSION_STRING << "\"";
    wb << ",version_describe=\"" << VERSION_DESCRIBE << "\"";
    wb << ",version_major=\"" << VERSION_MAJOR << "\"";
    wb << ",version_minor=\"" << VERSION_MINOR << "\"";
    wb << ",version_patch=\"" << VERSION_PATCH << "\"";
    wb << '}' << " 1" << '\n';
}


std::unordered_set<std::string> PrometheusMetricsWriter::getReservedLabelNames(
    bool expose_info,
    bool expose_asynchronous_metrics,
    AsynchronousMetricsKeyValuesMode async_metrics_mode,
    bool expose_histograms,
    bool expose_dimensional_metrics) const
{
    std::unordered_set<std::string> reserved_names;

    if (expose_info)
        reserved_names.insert({"name", "version", "version_describe", "version_major", "version_minor", "version_patch"});

    /// Every one of these labels belongs to a metric family that also has a pre-26.8 scalar name, so in
    /// `legacy_names` mode none of them is written and a configuration that was valid before 26.8 - a
    /// constant `device` label, for example - is valid again.
    if (expose_asynchronous_metrics && async_metrics_mode != AsynchronousMetricsKeyValuesMode::LegacyNames)
        reserved_names.insert({"channel", "cpu", "device", "disk", "interface", "mc", "sensor"});

    if (expose_histograms)
    {
        reserved_names.insert("le");
        HistogramMetrics::Factory::instance().forEachFamily(
            [&reserved_names](const HistogramMetrics::MetricFamily & family)
            {
                for (const auto & label : family.getLabels())
                    reserved_names.insert(label);
            });
    }

    if (expose_dimensional_metrics)
        DimensionalMetrics::Factory::instance().forEachFamily(
            [&reserved_names](const DimensionalMetrics::MetricFamily & family)
            {
                for (const auto & label : family.getLabels())
                    reserved_names.insert(label);
            });

    return reserved_names;
}


void KeeperPrometheusMetricsWriter::writeEvents([[maybe_unused]] WriteBuffer & wb) const
{
#if USE_NURAFT
    for (auto event : ProfileEvents::keeper_profile_events)
        writeEvent(wb, event, constant_labels_suffix);
#endif
}

void KeeperPrometheusMetricsWriter::writeMetrics([[maybe_unused]] WriteBuffer & wb) const
{
#if USE_NURAFT
    for (auto metric : CurrentMetrics::keeper_metrics)
        writeMetric(wb, metric, constant_labels_suffix);
#endif
}

void KeeperPrometheusMetricsWriter::writeAsynchronousMetrics([[maybe_unused]] WriteBuffer & wb,
                                                             [[maybe_unused]] const AsynchronousMetrics & async_metrics) const
{
#if USE_NURAFT
    writeAsyncMetrics(wb, async_metrics.getValues(), constant_labels, constant_labels_suffix);
#endif
}

void KeeperPrometheusMetricsWriter::writeHistogramMetrics([[maybe_unused]] WriteBuffer & wb) const
{
#if USE_NURAFT
    for (const auto * histogram : HistogramMetrics::keeper_histograms)
    {
        writeHistogramMetric(wb, *histogram, constant_labels);
    }
#endif
}

void KeeperPrometheusMetricsWriter::writeDimensionalMetrics([[maybe_unused]] WriteBuffer & wb) const
{
#if USE_NURAFT
    for (const auto * metric : DimensionalMetrics::keeper_dimensional_metrics)
    {
        writeDimensionalMetric(wb, *metric, constant_labels);
    }
#endif
}

void KeeperPrometheusMetricsWriter::writeErrors(WriteBuffer &) const
{
}

std::unordered_set<std::string> KeeperPrometheusMetricsWriter::getReservedLabelNames(
    [[maybe_unused]] bool expose_info,
    [[maybe_unused]] bool expose_asynchronous_metrics,
    [[maybe_unused]] AsynchronousMetricsKeyValuesMode async_metrics_mode,
    [[maybe_unused]] bool expose_histograms,
    [[maybe_unused]] bool expose_dimensional_metrics) const
{
    std::unordered_set<std::string> reserved_names;

    /// writeInfo() is not overridden for Keeper, so ClickHouse_Info is still exposed when enabled.
    if (expose_info)
        reserved_names.insert({"name", "version", "version_describe", "version_major", "version_minor", "version_patch"});

#if USE_NURAFT
    /// As above: these labels disappear together with the key-value form.
    if (expose_asynchronous_metrics && async_metrics_mode != AsynchronousMetricsKeyValuesMode::LegacyNames)
        reserved_names.insert({"channel", "cpu", "device", "disk", "interface", "mc", "sensor"});
#endif

#if USE_NURAFT
    /// Keeper only exposes the curated keeper_* families, not every family registered in the process.
    if (expose_histograms)
    {
        reserved_names.insert("le");
        for (const auto * histogram : HistogramMetrics::keeper_histograms)
            for (const auto & label : histogram->getLabels())
                reserved_names.insert(label);
    }

    if (expose_dimensional_metrics)
        for (const auto * metric : DimensionalMetrics::keeper_dimensional_metrics)
            for (const auto & label : metric->getLabels())
                reserved_names.insert(label);
#endif

    return reserved_names;
}

}
