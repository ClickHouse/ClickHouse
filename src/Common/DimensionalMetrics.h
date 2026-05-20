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

namespace DB
{
    class WriteBuffer;
}

namespace DimensionalMetrics
{
    using Value = Float64;
    using Labels = std::vector<String>;
    using LabelValues = std::vector<String>;

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

        using MetricsMap = std::unordered_map<LabelValues, std::shared_ptr<Metric>, LabelValuesHash>;

    public:
        MetricFamily(String name_, String documentation_, Labels labels_, std::vector<LabelValues> initial_label_values = {});
        /// Returns the metric for the given labels, creating it if absent.
        /// Currently returns Metric & for backward compatibility; TODO: change
        /// return type to shared_ptr<Metric> (matching getOrCreate) once
        /// existing callers that store a Metric & are updated.
        Metric & withLabels(LabelValues label_values);

        /// Like withLabels but returns a shared_ptr so the Metric stays alive
        /// even if removeWhere concurrently erases the map entry (orphaned
        /// write, no UB). Use this when the label set can be pruned at runtime.
        std::shared_ptr<Metric> getOrCreate(LabelValues label_values);

        template <typename Func>
        void forEachMetric(Func && func) const
        {
            std::shared_lock lock(mutex);
            for (const auto & [label_values, metric] : metrics)
            {
                func(label_values, *metric);
            }
        }

        template <typename Predicate>
        size_t removeWhere(Predicate && pred)
        {
            std::lock_guard lock(mutex);
            size_t removed = 0;
            auto it = metrics.begin();
            while (it != metrics.end())
            {
                if (pred(it->first))
                {
                    it = metrics.erase(it);
                    ++removed;
                }
                else
                    ++it;
            }
            return removed;
        }

        const Labels & getLabels() const;
        const String & getName() const;
        const String & getDocumentation() const;

    private:
        mutable DB::SharedMutex mutex;
        MetricsMap metrics;
        const String name;
        const String documentation;
        const Labels labels;
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
            std::vector<LabelValues> initial_label_values = {});

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
