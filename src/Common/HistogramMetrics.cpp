#include <Common/HistogramMetrics.h>
#include <Common/SipHash.h>

#include <algorithm>
#include <mutex>
#include <boost/container_hash/hash.hpp>

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
        {10, 100, 150, 225, 337, 500, 750},
        {"operation_type"}
    );
    Metric & KeeperResponseTimeReadonly = KeeperResponseTime.withLabels({"readonly"});
    Metric & KeeperResponseTimeWrite = KeeperResponseTime.withLabels({"write"});
    Metric & KeeperResponseTimeMulti = KeeperResponseTime.withLabels({"multi"});

    Metric & KeeperClientQueueDuration = Factory::instance().registerMetric(
        "keeper_client_queue_duration_milliseconds",
        "Time requests spend waiting to be enqueued and waiting in the queue before processed",
        {10, 100, 250, 500}
    );

    MetricFamily & KeeperReceiveRequestTimeMetricFamily = Factory::instance().registerMetric(
        "keeper_receive_request_time_milliseconds",
        "Time to receive and parse request from client in TCP handler",
        {10, 100, 250, 500},
        {}
    );
    Metric & KeeperReceiveRequestTime = KeeperReceiveRequestTimeMetricFamily.withLabels({});

    MetricFamily & KeeperDispatcherRequestsQueueTimeMetricFamily = Factory::instance().registerMetric(
        "keeper_dispatcher_requests_queue_time_milliseconds",
        "Time request spends in dispatcher requests queue",
        {10, 100, 250, 500},
        {}
    );
    Metric & KeeperDispatcherRequestsQueueTime = KeeperDispatcherRequestsQueueTimeMetricFamily.withLabels({});

    MetricFamily & KeeperWritePreCommitTimeMetricFamily = Factory::instance().registerMetric(
        "keeper_write_pre_commit_time_milliseconds",
        "Time to preprocess write request before Raft commit",
        {10, 100, 150, 225, 337, 500, 750},
        {}
    );
    Metric & KeeperWritePreCommitTime = KeeperWritePreCommitTimeMetricFamily.withLabels({});

    MetricFamily & KeeperWriteCommitTimeMetricFamily = Factory::instance().registerMetric(
        "keeper_write_commit_time_milliseconds",
        "Time to process write request after Raft commit",
        {10, 100, 150, 225, 337, 500, 750},
        {}
    );
    Metric & KeeperWriteCommitTime = KeeperWriteCommitTimeMetricFamily.withLabels({});

    MetricFamily & KeeperDispatcherResponsesQueueTimeMetricFamily = Factory::instance().registerMetric(
        "keeper_dispatcher_responses_queue_time_milliseconds",
        "Time response spends in dispatcher responses queue",
        {10, 100, 250, 500},
        {}
    );
    Metric & KeeperDispatcherResponsesQueueTime = KeeperDispatcherResponsesQueueTimeMetricFamily.withLabels({});

    MetricFamily & KeeperSendResponseTimeMetricFamily = Factory::instance().registerMetric(
        "keeper_send_response_time_milliseconds",
        "Time to send response to client in TCP handler (includes queueing and writing to socket)",
        {10, 100, 250, 500},
        {}
    );
    Metric & KeeperSendResponseTime = KeeperSendResponseTimeMetricFamily.withLabels({});

    MetricFamily & KeeperReadWaitForWriteTimeMetricFamily = Factory::instance().registerMetric(
        "keeper_read_wait_for_write_time_milliseconds",
        "Time read request waits for the write request it depends on to complete",
        {10, 100, 250, 500},
        {}
    );
    Metric & KeeperReadWaitForWriteTime = KeeperReadWaitForWriteTimeMetricFamily.withLabels({});

    MetricFamily & KeeperReadProcessTimeMetricFamily = Factory::instance().registerMetric(
        "keeper_read_process_time_milliseconds",
        "Time to process read request",
        {10, 100, 250, 500},
        {}
    );
    Metric & KeeperReadProcessTime = KeeperReadProcessTimeMetricFamily.withLabels({});

    MetricFamily & KeeperBatchSizeElementsMetricFamily = Factory::instance().registerMetric(
        "keeper_batch_size_elements",
        "Size of batch sent to Raft in elements.",
        {4, 8, 16, 32, 64, 128, 256, 299},
        {}
    );
    Metric & KeeperCurrentBatchSizeElements = KeeperBatchSizeElementsMetricFamily.withLabels({});

    MetricFamily & KeeperBatchSizeBytesMetricFamily = Factory::instance().registerMetric(
        "keeper_batch_size_bytes",
        "Size of batch sent to Raft in bytes.",
        {1 << 12, 1 << 13, 1 << 14, 1 << 15, 1 << 16, 1 << 17, 1 << 18, 307199},
        {}
    );
    Metric & KeeperCurrentBatchSizeBytes = KeeperBatchSizeBytesMetricFamily.withLabels({});

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

    size_t MetricFamily::LabelValuesHash::operator()(const LabelValues & label_values) const
    {
        size_t result = 0;
        for (const String & label_value : label_values)
        {
            boost::hash_combine(result, label_value);
        }
        return result;
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
        chassert(label_values.size() == labels.size());
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

    void observe(Metric & metric, Value value)
    {
        metric.observe(value);
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

    Metric & Factory::registerMetric(String name, String documentation, Buckets buckets)
    {
        auto & family = registerMetric(std::move(name), std::move(documentation), std::move(buckets), {});
        return family.withLabels({});
    }
}
