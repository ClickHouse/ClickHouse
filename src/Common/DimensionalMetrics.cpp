#include <Common/DimensionalMetrics.h>
#include <Common/SipHash.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <cassert>
#include <mutex>

namespace DB::DimensionalMetrics
{

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

    void Metric::writePrometheusLine(
        WriteBuffer & wb,
        const String & metric_name,
        const Labels & labels,
        const LabelValues & label_values) const
    {
        wb << metric_name;
        if (!labels.empty())
        {
            wb << '{';
            for (size_t i = 0; i < labels.size(); ++i)
            {
                if (i != 0)
                {
                    wb << ',';
                }
                wb << labels[i] << "=\"" << label_values[i] << '"';
            }
            wb << '}';
        }
        wb << ' ' << get() << '\n';
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

    MetricFamily::MetricFamily(String name_, String documentation_, Labels labels_, std::vector<LabelValues> initial_label_values)
        : name(std::move(name_))
        , documentation(std::move(documentation_))
        , labels(std::move(labels_))
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
        auto [it, _] = metrics.try_emplace(std::move(label_values), std::make_unique<Metric>());
        return *it->second;
    }

    void MetricFamily::unregister(LabelValues label_values) noexcept
    {
        std::lock_guard lock(mutex);
        metrics.erase(label_values);
    }

    const Labels & MetricFamily::getLabels() const { return labels; }


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
            std::make_unique<MetricFamily>(
                std::move(name),
                std::move(documentation),
                std::move(labels),
                std::move(initial_label_values)
            )
        );
        return *registry.back();
    }

    void Factory::clear()
    {
        std::lock_guard lock(mutex);
        registry.clear();
    }
}
