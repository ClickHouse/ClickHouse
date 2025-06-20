#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace DB::Histogram
{
    using Value = Float64;
    using Buckets = std::vector<Value>;
    using Labels = std::vector<String>;
    using LabelValues = std::vector<String>;

    struct Metric
    {
        using Counter = UInt64;
        using Sum = Value;

        explicit Metric(const Buckets & buckets_);
        void observe(Value value);
        Counter getCounter(size_t idx) const;
        Sum getSum() const;

    private:
        using AtomicCounters = std::vector<std::atomic<Counter>>;
        using AtomicSum = std::atomic<Value>;

        const Buckets & buckets;
        AtomicCounters counters;
        AtomicSum sum;
    };

    struct MetricFamily
    {
    private:
        struct LabelValuesHash
        {
            size_t operator()(const LabelValues & label_values) const;
        };

        using MetricsMap = std::unordered_map<LabelValues, std::shared_ptr<Metric>, LabelValuesHash>;

    public:
        explicit MetricFamily(Buckets buckets_, Labels labels_);
        Metric & withLabels(LabelValues label_values);
        MetricsMap getMetrics() const;
        const Buckets & getBuckets() const;
        const Labels & getLabels() const;

    private:
        mutable std::shared_mutex mutex;
        MetricsMap metrics;
        const Buckets buckets;
        const Labels labels;
    };

    struct MetricRecord
    {
        MetricRecord(String name_, String documentation_, Buckets buckets, Labels labels);
        String name;
        String documentation;
        MetricFamily family;
    };

    using MetricRecordPtr = std::shared_ptr<MetricRecord>;
    using MetricRecords = std::vector<MetricRecordPtr>;

    class Factory
    {
    public:
        static Factory & instance();
        MetricFamily & registerMetric(String name, String documentation, Buckets buckets, Labels labels);
        MetricRecords getRecords() const;

    private:
        mutable std::mutex mutex;
        MetricRecords registry TSA_GUARDED_BY(mutex);
    };
}
