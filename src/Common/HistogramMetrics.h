#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <Common/SharedMutex.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace HistogramMetrics
{
    using Value = Int64;
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

        using MetricsMap = std::unordered_map<LabelValues, std::unique_ptr<Metric>, LabelValuesHash>;

    public:
        MetricFamily(String name_, String documentation_, Buckets buckets_, Labels labels_);

        Metric & withLabels(LabelValues label_values);

        template <typename Func>
        void forEachMetric(Func && func) const
        {
            std::shared_lock lock(mutex);
            for (const auto & [label_values, metric] : metrics)
            {
                func(label_values, *metric);
            }
        }

        const Buckets & getBuckets() const;
        const Labels & getLabels() const;
        const String & getName() const;
        const String & getDocumentation() const;

    private:
        mutable DB::SharedMutex mutex;
        MetricsMap metrics;
        const String name;
        const String documentation;
        const Buckets buckets;
        const Labels labels;
    };

    using MetricFamilyPtr = std::unique_ptr<MetricFamily>;
    using MetricFamilies = std::vector<MetricFamilyPtr>;

    void observe(MetricFamily & metric, LabelValues labels, Value value);
    void observe(Metric & metric, Value value);

    class Factory
    {
    public:
        static Factory & instance();
        MetricFamily & registerMetric(String name, String documentation, Buckets buckets, Labels labels);
        Metric & registerMetric(String name, String documentation, Buckets buckets);

        template <typename Func>
        void forEachFamily(Func && func) const
        {
            std::shared_lock lock(mutex);
            for (const MetricFamilyPtr & family : registry)
            {
                func(*family);
            }
        }

    private:
        mutable DB::SharedMutex mutex;
        MetricFamilies registry;
    };
}
