#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#include "Common/SipHash.h"
#include "base/types.h"

namespace Histogram
{
    using Value = Int64;
    using Counter = UInt64;
    using AtomicCounter = std::atomic<Counter>;
    using AtomicCounters = std::vector<AtomicCounter>;
    using Sum = Value;
    using AtomicSum = std::atomic<Value>;
    using Buckets = std::vector<Value>;
    using Label = String;
    using LabelValue = String;
    using Labels = std::vector<Label>;
    using LabelValues = std::vector<LabelValue>;

    struct Metric
    {
        explicit Metric(const Buckets & buckets_) : buckets(buckets_), counters(buckets.size() + 1), sum() {}

        void observe(Value value)
        {
            const size_t bucket_idx = std::distance(
                buckets.begin(),
                std::lower_bound(buckets.begin(), buckets.end(), value)
            );
            counters[bucket_idx].fetch_add(1, std::memory_order_relaxed);
            sum.fetch_add(value, std::memory_order_relaxed);
        }

        Counter getCounter(size_t idx) const
        {
            return counters[idx].load(std::memory_order_relaxed);
        }

        Sum getSum() const
        {
            return sum.load(std::memory_order_relaxed);
        }

    private:
        const Buckets & buckets;
        AtomicCounters counters;
        AtomicSum sum;
    };

    struct MetricFamily
    {
    private:
        struct LabelValuesHash;
        using MetricsMap = std::unordered_map<LabelValues, std::shared_ptr<Metric>, LabelValuesHash>;

    public:

        explicit MetricFamily(Buckets buckets_, Labels labels_) : buckets(std::move(buckets_)), labels(std::move(labels_)) {}

        Metric & withLabels(LabelValues label_values)
        {
            assert(label_values.size() == labels.size());
            std::lock_guard lock(mutex);
            auto [it, _] = metrics.try_emplace(std::move(label_values), std::make_shared<Metric>(buckets));
            return *it->second;
        }

        MetricsMap getMetrics() const
        {
            std::lock_guard lock(mutex);
            return metrics;
        }

        const Buckets & getBuckets() const
        {
            return buckets;
        }

        const Labels & getLabels() const
        {
            return labels;
        }

    private:
        struct LabelValuesHash
        {
            size_t operator()(const LabelValues & label_values) const
            {
                SipHash hash;
                hash.update(label_values.size());
                for (const String & label_value : label_values)
                {
                    hash.update(label_value);
                }
                return hash.get64();
            }
        };

        mutable std::mutex mutex;
        MetricsMap metrics TSA_GUARDED_BY(mutex);
        const Buckets buckets;
        const Labels labels;
    };

    struct MetricRecord
    {
        MetricRecord(String name_, String documentation_, Buckets buckets, Labels labels)
        : name(name_), documentation(documentation_), family(std::move(buckets), std::move(labels)) {}

        String name;
        String documentation;
        MetricFamily family;
    };
    using MetricRecordPtr = std::shared_ptr<MetricRecord>;
    using MetricRecords = std::vector<MetricRecordPtr>;

    class Factory
    {
    public:
        static Factory & instance()
        {
            static Factory factory;
            return factory;
        }

        MetricFamily & registerMetric(String name, String documentation, Buckets buckets, Labels labels)
        {
            std::lock_guard lock(mutex);
            registry.push_back(
                std::make_shared<MetricRecord>(name, documentation, std::move(buckets), std::move(labels))
            );
            return registry.back()->family;
        }

        MetricRecords getRecords() const
        {
            std::lock_guard lock(mutex);
            return registry;
        }

    private:
        mutable std::mutex mutex;
        MetricRecords registry TSA_GUARDED_BY(mutex);
    };
};
