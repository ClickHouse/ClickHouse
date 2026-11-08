#include <Common/DimensionalMetrics.h>
#include <Common/SipHash.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <cassert>
#include <mutex>
#include <shared_mutex>

namespace DimensionalMetrics
{
    MetricFamily & MergeFailures = Factory::instance().registerMetric(
        "merge_failures",
        "Number of all failed merges since startup.",
        {"error_name"}
    );

    MetricFamily & StartupScriptsFailureReason = Factory::instance().registerMetric(
        "startup_scripts_failure_reason",
        "Indicates startup scripts failures by error type. Set to 1 when a startup script fails, labelled with the error name.",
        {"error_name"}
    );

    MetricFamily & MergeTreeParts = Factory::instance().registerMetric(
        "merge_tree_parts",
        "Number of merge tree data parts, labelled by part state (e.g. Temporary, PreActive, Active, Outdated, Deleting, DeleteOnDestroy), part type (e.g. Wide, Compact, Unknown) and whether it is a projection part.",
        {"part_state", "part_type", "part_is_projection"}
    );

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

    const Labels & MetricFamily::getLabels() const { return labels; }
    const String & MetricFamily::getName() const { return name; }
    const String & MetricFamily::getDocumentation() const { return documentation; }

    void add(MetricFamily & metric, LabelValues labels, Value amount)
    {
        metric.withLabels(std::move(labels)).increment(amount);
    }

    void sub(MetricFamily & metric, LabelValues labels, Value amount)
    {
        metric.withLabels(std::move(labels)).decrement(amount);
    }

    void set(MetricFamily & metric, LabelValues labels, Value value)
    {
        metric.withLabels(std::move(labels)).set(value);
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
            std::make_unique<MetricFamily>(
                std::move(name),
                std::move(documentation),
                std::move(labels),
                std::move(initial_label_values)
            )
        );
        return *registry.back();
    }
}
