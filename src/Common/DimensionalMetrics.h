#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <Common/SharedMutex.h>

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace DB
{
    class WriteBuffer;
}

namespace DimensionalMetrics
{
    using Value = Float64;
    using Labels = std::vector<String>;
    using LabelValues = std::vector<String>;

    enum class MetricType
    {
        Gauge,
        Counter,
    };

    struct Metric
    {
        Metric() : value(0.0) {}

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

        using MetricsMap = std::unordered_map<LabelValues, std::unique_ptr<Metric>, LabelValuesHash>;

    public:
        MetricFamily(
            String name_,
            String documentation_,
            Labels labels_,
            std::vector<LabelValues> initial_label_values = {},
            MetricType type_ = MetricType::Gauge);
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

        const Labels & getLabels() const;
        const String & getName() const;
        const String & getDocumentation() const;
        const String & getTypeString() const;

    private:
        mutable DB::SharedMutex mutex;
        MetricsMap metrics;
        const String name;
        const String documentation;
        const Labels labels;
        const String type_string;
    };

    using MetricFamilyPtr = std::unique_ptr<MetricFamily>;
    using MetricFamilies = std::vector<MetricFamilyPtr>;

    void add(MetricFamily & metric, LabelValues labels, Value amount = 1.0);
    void sub(MetricFamily & metric, LabelValues labels, Value amount = 1.0);
    void set(MetricFamily & metric, LabelValues labels, Value value);

    class Factory
    {
    public:
        static Factory & instance();
        MetricFamily & registerMetric(
            String name,
            String documentation,
            Labels labels,
            std::vector<LabelValues> initial_label_values = {},
            MetricType type = MetricType::Gauge);

        template <typename Func>
        void forEachFamily(Func && func) const
        {
            std::shared_lock lock(mutex);
            for (const auto & family : registry)
            {
                func(*family);
            }
        }

    private:
        mutable DB::SharedMutex mutex;
        MetricFamilies registry;
    };
}
