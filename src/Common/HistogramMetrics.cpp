#include <Common/HistogramMetrics.h>
#include <Common/SipHash.h>

#include <algorithm>
#include <mutex>
#include <shared_mutex>

namespace HistogramMetrics
{
    MetricFamily & AzureBlobConnect = Factory::instance().registerMetric(
        "azure_connect_microseconds",
        "Time to establish connection with Azure Blob Storage, in microseconds.",
        {100, 1000, 10000, 100000, 200000, 300000, 500000, 1000000, 1500000},
        {}
    );

    MetricFamily & DiskAzureConnect = Factory::instance().registerMetric(
        "disk_azure_connect_microseconds",
        "Time to establish connection with DiskAzure, in microseconds.",
        {100, 1000, 10000, 100000, 200000, 300000, 500000, 1000000, 1500000},
        {}
    );

    MetricFamily & AzureFirstByte = Factory::instance().registerMetric(
        "azure_first_byte_microseconds",
        "Time to receive the first byte from an Azure Blob Storage request, in microseconds.",
        {100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000},
        {"http_method", "attempt"}
    );

    MetricFamily & DiskAzureFirstByte = Factory::instance().registerMetric(
        "disk_azure_first_byte_microseconds",
        "Time to receive the first byte from a DiskAzure request, in microseconds.",
        {100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000},
        {"http_method", "attempt"}
    );

    MetricFamily & S3Connect = Factory::instance().registerMetric(
        "s3_connect_microseconds",
        "Time to establish connection with S3, in microseconds.",
        {100, 1000, 10000, 100000, 200000, 300000, 500000, 1000000, 1500000},
        {}
    );

    MetricFamily & DiskS3Connect = Factory::instance().registerMetric(
        "disk_s3_connect_microseconds",
        "Time to establish connection with DiskS3, in microseconds.",
        {100, 1000, 10000, 100000, 200000, 300000, 500000, 1000000, 1500000},
        {}
    );

    MetricFamily & S3FirstByte = Factory::instance().registerMetric(
        "s3_first_byte_microseconds",
        "Time to receive the first byte from an S3 request, in microseconds.",
        {100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000},
        {"http_method", "attempt"}
    );

    MetricFamily & DiskS3FirstByte = Factory::instance().registerMetric(
        "disk_s3_first_byte_microseconds",
        "Time to receive the first byte from a DiskS3 request, in microseconds.",
        {100, 1000, 10000, 100000, 300000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000},
        {"http_method", "attempt"}
    );

    MetricFamily & KeeperResponseTime = Factory::instance().registerMetric(
        "keeper_response_time_ms",
        "The response time of Keeper, in milliseconds",
        {1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2500, 5000, 10000},
        {"operation"}
    );

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
    const String & MetricFamily::getName() const { return name; }
    const String & MetricFamily::getDocumentation() const { return documentation; }

    void observe(MetricFamily & metric, LabelValues labels, Value value)
    {
        metric.withLabels(std::move(labels)).observe(value);
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
            std::make_unique<MetricFamily>(std::move(name), std::move(documentation), std::move(buckets), std::move(labels))
        );
        return *registry.back();
    }
}
