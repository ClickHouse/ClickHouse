#include <Common/Histogram.h>
#include <Common/SipHash.h>

#include <algorithm>
#include <mutex>

namespace DB::Histogram
{
    Metric::Metric(const Buckets& buckets_)
        : buckets(buckets_)
        , counters(buckets.size() + 1)
        , sum()
    {
    }

    void Metric::observe(Value value)
    {
        const size_t bucket_idx = std::distance(
            buckets.begin(),
            std::lower_bound(buckets.begin(), buckets.end(), value)
        );
        counters[bucket_idx].fetch_add(1, std::memory_order_relaxed);
        sum.fetch_add(value, std::memory_order_relaxed);
    }

    Metric::Counter Metric::getCounter(size_t idx) const
    {
        return counters[idx].load(std::memory_order_relaxed);
    }

    Metric::Sum Metric::getSum() const
    {
        return sum.load(std::memory_order_relaxed);
    }

    size_t MetricFamily::LabelValuesHash::operator()(const LabelValues& label_values) const
    {
        SipHash hash;
        hash.update(label_values.size());
        for (const String& label_value : label_values)
        {
            hash.update(label_value);
        }
        return hash.get64();
    }

    MetricFamily::MetricFamily(Buckets buckets_, Labels labels_)
        : buckets(std::move(buckets_))
        , labels(std::move(labels_))
    {
    }

    Metric & MetricFamily::withLabels(LabelValues label_values)
    {
        assert(label_values.size() == labels.size());
        {
            std::shared_lock lock(mutex);
            auto it = metrics.find(label_values);
            if (it != metrics.end())
            {
                return *it->second;
            }
        }
        std::lock_guard lock(mutex);
        auto [it, _] = metrics.try_emplace(std::move(label_values), std::make_shared<Metric>(buckets));
        return *it->second;
    }

    MetricFamily::MetricsMap MetricFamily::getMetrics() const
    {
        std::lock_guard lock(mutex);
        return metrics;
    }

    const Buckets & MetricFamily::getBuckets() const { return buckets; }
    const Labels & MetricFamily::getLabels() const { return labels; }

    MetricRecord::MetricRecord(String name_, String documentation_, Buckets buckets, Labels labels)
        : name(std::move(name_))
        , documentation(std::move(documentation_))
        , family(std::move(buckets), std::move(labels))
    {
    }

    Factory & Factory::instance()
    {
        static Factory factory;
        return factory;
    }

    MetricFamily & Factory::registerMetric(String name, String documentation, Buckets buckets, Labels labels)
    {
        std::lock_guard lock(mutex);
        registry.push_back(
            std::make_shared<MetricRecord>(std::move(name), std::move(documentation), std::move(buckets), std::move(labels))
        );
        return registry.back()->family;
    }

    MetricRecords Factory::getRecords() const
    {
        std::lock_guard lock(mutex);
        return registry;
    }
}
