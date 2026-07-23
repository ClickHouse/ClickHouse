#pragma once

#include <map>
#include <memory>
#include <string>
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
};

}
