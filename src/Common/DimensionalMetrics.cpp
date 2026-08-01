#include <Common/DimensionalMetrics.h>
#include <Common/SipHash.h>

#include <cassert>
#include <mutex>

namespace DB::DimensionalMetrics
{
    Metric::Metric() : value(0.0) {}

    void Metric::set(Value value_)
    {
        value.store(value_, std::memory_order_relaxed);
    }

    void Metric::increment(Value amount)
    {
        value.fetch_add(amount, std::memory_order_relaxed);
    }

    void Metric::decrement(Value amount)
    {
        value.fetch_sub(amount, std::memory_order_relaxed);
    }

    Value Metric::get() const
    {
        return value.load(std::memory_order_relaxed);
    }

    size_t MetricFamily::LabelValuesHash::operator()(const LabelValues & label_values) const
    {
        SipHash hash;
        hash.update(label_values.size());
        for (const String & label_value : label_values)
        {
            hash.update(label_value.data(), label_value.size());
        }
        return hash.get64();
    }

    MetricFamily::MetricFamily(Labels labels_, std::vector<LabelValues> initial_label_values)
        : labels(std::move(labels_))
    {
        for (auto & label_values : initial_label_values)
        {
            withLabels(std::move(label_values));
        }
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
        auto [it, _] = metrics.try_emplace(std::move(label_values), std::make_shared<Metric>());
        return *it->second;
    }

    void MetricFamily::unregister(LabelValues label_values) noexcept
    {
        std::lock_guard lock(mutex);
        metrics.erase(label_values);
    }

    MetricFamily::MetricsMap MetricFamily::getMetrics() const
    {
        std::shared_lock lock(mutex);
        return metrics;
    }

    const Labels & MetricFamily::getLabels() const { return labels; }

    MetricRecord::MetricRecord(String name_, String documentation_, Labels labels, std::vector<LabelValues> initial_label_values)
        : name(std::move(name_))
        , documentation(std::move(documentation_))
        , family(std::move(labels), std::move(initial_label_values))
    {
    }

    Factory & Factory::instance()
    {
        static Factory factory;
        return factory;
    }

    MetricFamily & Factory::registerMetric(
        String name,
        String documentation,
        Labels labels,
        std::vector<LabelValues> initial_label_values)
    {
        std::lock_guard lock(mutex);
        registry.push_back(
            std::make_shared<MetricRecord>(
                std::move(name),
                std::move(documentation),
                std::move(labels),
                std::move(initial_label_values)
            )
        );
        return registry.back()->family;
    }

    MetricRecords Factory::getRecords() const
    {
        std::lock_guard lock(mutex);
        return registry;
    }
}
