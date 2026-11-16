#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <Common/AsynchronousMetricsKeyValuesMode.h>
#include <Common/HistogramMetrics.h>
#include <Common/DimensionalMetrics.h>


namespace DB
{
class AsynchronousMetrics;
class WriteBuffer;

/// Write metrics in Prometheus format
class PrometheusMetricsWriter
{
public:
    /// `constant_labels_` are added to every exposed metric.
    explicit PrometheusMetricsWriter(const std::map<std::string, std::string> & constant_labels_ = {});
    virtual ~PrometheusMetricsWriter() = default;

    virtual void writeMetrics(WriteBuffer & wb) const;
    virtual void writeAsynchronousMetrics(WriteBuffer & wb, const AsynchronousMetrics & async_metrics) const;
    virtual void writeEvents(WriteBuffer & wb) const;
    virtual void writeErrors(WriteBuffer & wb) const;
    virtual void writeHistogramMetrics(WriteBuffer & wb) const;
    virtual void writeDimensionalMetrics(WriteBuffer & wb) const;
    virtual void writeInfo(WriteBuffer & wb) const;

    /// Label names this writer emits itself for the sections enabled by the given flags (the "le" label
    /// of histogram buckets, the "ClickHouse_Info" labels, the asynchronous metric key labels, and the
    /// per-sample labels of the exposed histogram/dimensional families). A constant label must not reuse
    /// any of them, or an exported sample would carry two labels with the same name. `async_metrics_mode`
    /// is needed because it decides whether the asynchronous metric keys are exposed as labels at all.
    /// The default reflects the full server surface;
    /// derived writers override to reflect their own (e.g. Keeper exposes only keeper_* families).
    virtual std::unordered_set<std::string> getReservedLabelNames(
        bool expose_info,
        bool expose_asynchronous_metrics,
        AsynchronousMetricsKeyValuesMode async_metrics_mode,
        bool expose_histograms,
        bool expose_dimensional_metrics) const;

    /// `extra_labels` must be either empty or rendered as `name="value",...` (without braces);
    /// they are written before the family's own labels.
    static void writeHistogramMetric(WriteBuffer & wb, const HistogramMetrics::MetricFamily & family, const std::string & extra_labels = {});
    static void writeDimensionalMetric(WriteBuffer & wb, const DimensionalMetrics::MetricFamily & family, const std::string & extra_labels = {});

protected:
    /// Constant labels rendered as `name="value",...` (without braces), empty if no constant labels are configured.
    std::string constant_labels;
    /// The same labels rendered as a `{name="value",...}` suffix for metrics without their own labels.
    std::string constant_labels_suffix;
};


class KeeperPrometheusMetricsWriter : public PrometheusMetricsWriter
{
public:
    using PrometheusMetricsWriter::PrometheusMetricsWriter;

    void writeMetrics(WriteBuffer & wb) const override;
    void writeAsynchronousMetrics(WriteBuffer & wb, const AsynchronousMetrics & async_metrics) const override;
    void writeEvents(WriteBuffer & wb) const override;
    void writeErrors(WriteBuffer & wb) const override;
    void writeHistogramMetrics(WriteBuffer & wb) const override;
    void writeDimensionalMetrics(WriteBuffer & wb) const override;
    std::unordered_set<std::string> getReservedLabelNames(
        bool expose_info,
        bool expose_asynchronous_metrics,
        AsynchronousMetricsKeyValuesMode async_metrics_mode,
        bool expose_histograms,
        bool expose_dimensional_metrics) const override;
};

}
