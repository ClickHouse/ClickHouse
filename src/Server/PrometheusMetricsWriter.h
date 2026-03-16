#pragma once

#include <memory>
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
    virtual ~PrometheusMetricsWriter() = default;

    virtual void writeMetrics(WriteBuffer & wb) const;
    virtual void writeAsynchronousMetrics(WriteBuffer & wb, const AsynchronousMetrics & async_metrics) const;
    virtual void writeEvents(WriteBuffer & wb) const;
    virtual void writeErrors(WriteBuffer & wb) const;
    virtual void writeHistogramMetrics(WriteBuffer & wb) const;
    virtual void writeDimensionalMetrics(WriteBuffer & wb) const;

    static void writeHistogramMetric(WriteBuffer & wb, const HistogramMetrics::MetricFamily & family);
    static void writeDimensionalMetric(WriteBuffer & wb, const DimensionalMetrics::MetricFamily & family);
};


class KeeperPrometheusMetricsWriter : public PrometheusMetricsWriter
{
public:
    void writeMetrics(WriteBuffer & wb) const override;
    void writeAsynchronousMetrics(WriteBuffer & wb, const AsynchronousMetrics & async_metrics) const override;
    void writeEvents(WriteBuffer & wb) const override;
    void writeErrors(WriteBuffer & wb) const override;
    void writeHistogramMetrics(WriteBuffer & wb) const override;
    void writeDimensionalMetrics(WriteBuffer & wb) const override;
};

}
