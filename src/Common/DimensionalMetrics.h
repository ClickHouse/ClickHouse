#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace DB::DimensionalMetrics
{
    using Value = Float64;
    using Labels = std::vector<String>;
    using LabelValues = std::vector<String>;

    struct Metric
    {
        Metric();
        void set(Value value_);
        void increment(Value amount = 1.0);
        void decrement(Value amount = 1.0);
        Value get() const;

    private:
        std::atomic<Value> value;
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
        explicit MetricFamily(Labels labels_, std::vector<LabelValues> initial_label_values = {});
        Metric & withLabels(LabelValues label_values);
        void unregister(LabelValues label_values) noexcept;
        MetricsMap getMetrics() const;
        const Labels & getLabels() const;

    private:
        mutable std::shared_mutex mutex;
        MetricsMap metrics;
        const Labels labels;
    };

    struct MetricRecord
    {
        MetricRecord(String name_, String documentation_, Labels labels, std::vector<LabelValues> initial_label_values);
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
        MetricFamily & registerMetric(
            String name,
            String documentation,
            Labels labels,
            std::vector<LabelValues> initial_label_values = {});
        MetricRecords getRecords() const;

    private:
        mutable std::mutex mutex;
        MetricRecords registry TSA_GUARDED_BY(mutex);
    };
}
