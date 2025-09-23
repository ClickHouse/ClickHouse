#include <Common/Histogram.h>
#include <Common/SipHash.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <algorithm>
#include <mutex>

namespace DB::Histogram
{
    Metric::Metric(const Buckets & buckets_)
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

    void Metric::writePrometheusLines(
        WriteBuffer & wb,
        const String & metric_name,
        const Labels & labels,
        const LabelValues & label_values) const
    {
        Counter cumulative_count = 0;

        for (size_t i = 0; i < buckets.size() + 1; ++i)
        {
            cumulative_count += getCounter(i);

            wb << metric_name << "_bucket{";

            for (size_t j = 0; j < labels.size(); ++j)
            {
                wb << labels[j] << "=\"" << label_values[j] << "\",";
            }

            wb << "le=\"";
            if (i != buckets.size())
            {
                wb << buckets[i];
            }
            else
            {
                wb << "+Inf";
            }

            wb << "\"}" << ' ' << cumulative_count << '\n';
        }

        wb << metric_name << "_count";
        if (!labels.empty())
        {
            wb << '{';
            for (size_t j = 0; j < labels.size(); ++j)
            {
                if (j != 0)
                {
                    wb << ',';
                }
                wb << labels[j] << "=\"" << label_values[j] << '"';
            }
            wb << '}';
        }
        wb << ' ' << cumulative_count << '\n';

        wb << metric_name << "_sum";
        if (!labels.empty())
        {
            wb << '{';
            for (size_t j = 0; j < labels.size(); ++j)
            {
                if (j > 0)
                {
                    wb << ',';
                }
                wb << labels[j] << "=\"" << label_values[j] << '"';
            }
            wb << '}';
        }
        wb << ' ' << getSum() << '\n';
    }

    size_t MetricFamily::LabelValuesHash::operator()(const LabelValues& label_values) const
    {
        SipHash hash;
        hash.update(label_values.size());
        for (const String& label_value : label_values)
        {
            hash.update(label_value.data(), label_value.size());
        }
        return hash.get64();
    }

    MetricFamily::MetricFamily(String name_, String documentation_, Buckets buckets_, Labels labels_)
        : name(std::move(name_))
        , documentation(std::move(documentation_))
        , buckets(std::move(buckets_))
        , labels(std::move(labels_))
    {
    }

    Metric & MetricFamily::withLabels(LabelValues label_values)
    {
        assert(label_values.size() == labels.size());
        {
            std::shared_lock lock(mutex);
            if (auto it = metrics.find(label_values); it != metrics.end())
            {
                return *it->second;
            }
        }

        std::lock_guard lock(mutex);
        auto [it, _] = metrics.try_emplace(
            std::move(label_values),
            std::make_unique<Metric>(buckets));
        return *it->second;
    }

    const Buckets & MetricFamily::getBuckets() const { return buckets; }
    const Labels & MetricFamily::getLabels() const { return labels; }


    Factory & Factory::instance()
    {
        static Factory factory;
        return factory;
    }

    MetricFamily & Factory::registerMetric(String name, String documentation, Buckets buckets, Labels labels)
    {
        std::lock_guard lock(mutex);
        registry.push_back(
            std::make_unique<MetricFamily>(std::move(name), std::move(documentation), std::move(buckets), std::move(labels))
        );
        return *registry.back();
    }

    void Factory::clear()
    {
        std::lock_guard lock(mutex);
        registry.clear();
    }
}
