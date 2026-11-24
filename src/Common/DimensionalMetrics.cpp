#include <Common/DimensionalMetrics.h>
#include <Common/SipHash.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <mutex>
#include <shared_mutex>

namespace DimensionalMetrics
{
    namespace
    {
        String metricTypeToString(MetricType type)
        {
            switch (type)
            {
                case MetricType::Gauge:
                    return "gauge";
                case MetricType::Counter:
                    return "counter";
            }
            UNREACHABLE();
        }
    }

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

    MetricFamily & ObjectStorageQueueFailures = Factory::instance().registerMetric(
        "object_storage_queue_failures_total",
        "Number of ObjectStorageQueue (S3Queue/AzureQueue) failures, labelled by database, table, processing stage (read, set_processing, insert, commit, after_processing) and error code. For the after_processing stage the value is the number of objects for which the after_processing action (delete, tag or move) did not complete.",
        {"database", "table", "stage", "error_code"},
        {},
        MetricType::Counter
    );

    MetricFamily & ObjectStorageQueuePermanentlyFailedFiles = Factory::instance().registerMetric(
        "object_storage_queue_permanently_failed_files_total",
        "Number of ObjectStorageQueue (S3Queue/AzureQueue) files given up on for good after exhausting retries (or with retries disabled), labelled by database and table. Each of these represents a file whose data will never be processed.",
        {"database", "table"},
        {},
        MetricType::Counter
    );

    MetricFamily & ObjectStorageQueueNewestSeenTimestamp = Factory::instance().registerMetric(
        "object_storage_queue_newest_seen_object_timestamp_seconds",
        "Unix timestamp of the last-modified time of the newest object seen so far by an ObjectStorageQueue (S3Queue/AzureQueue) table, labelled by database and table. Compare against object_storage_queue_newest_committed_object_timestamp_seconds to estimate pipeline lag. The series is removed when the table is dropped or detached. After a rename the series of the old name stays until the server restarts.",
        {"database", "table"}
    );

    MetricFamily & ObjectStorageQueueNewestCommittedTimestamp = Factory::instance().registerMetric(
        "object_storage_queue_newest_committed_object_timestamp_seconds",
        "Unix timestamp of the last-modified time of the newest object fully processed so far by an ObjectStorageQueue (S3Queue/AzureQueue) table, labelled by database and table. Compare against object_storage_queue_newest_seen_object_timestamp_seconds to estimate pipeline lag. The series is removed when the table is dropped or detached. After a rename the series of the old name stays until the server restarts.",
        {"database", "table"}
    );

    MetricFamily & ObjectStorageQueueProcessedFiles = Factory::instance().registerMetric(
        "object_storage_queue_processed_files_total",
        "Number of files committed as processed by this ObjectStorageQueue (S3Queue/AzureQueue) table on this server, labelled by database and table.",
        {"database", "table"},
        {},
        MetricType::Counter
    );

    MetricFamily & ObjectStorageQueueLastPollTimestamp = Factory::instance().registerMetric(
        "object_storage_queue_last_poll_timestamp_seconds",
        "Unix timestamp of the last time a background streaming thread of an ObjectStorageQueue (S3Queue/AzureQueue) table on this server started a cycle that looks for files to process, labelled by database and table. It is set after the table registered itself as active in Keeper and before it looks for files, so it advances also when the cycle finds no files or fails, and it does not advance when the registration fails. It does not advance while the table has no ready attached materialized views, while streaming is stopped or paused, or while streaming is disabled on the server, because the table does not consume then. All streaming threads of a table write the same series, so it shows the newest cycle of any of them. The series is removed when the table is dropped or detached. After a rename the series of the old name stays until the server restarts.",
        {"database", "table"}
    );

    MetricFamily & SystemLogFlushes = Factory::instance().registerMetric(
        "system_log_flushes_total",
        "Number of attempts to flush the in-memory buffer of a system log to its table, labelled by the database and the table of the system log and by result (success, failure).",
        {"database", "table", "result"},
        {},
        MetricType::Counter
    );

    MetricFamily & SystemLogFlushedRows = Factory::instance().registerMetric(
        "system_log_flushed_rows_total",
        "Number of rows written by successful flushes of a system log, labelled by the database and the table of the system log.",
        {"database", "table"},
        {},
        MetricType::Counter
    );

    MetricFamily & SystemLogDroppedRows = Factory::instance().registerMetric(
        "system_log_dropped_rows_total",
        "Number of rows of a system log that were dropped because its in-memory buffer reached max_size_rows, labelled by the database and the table of the system log. It is updated when the buffer is taken for the next flush.",
        {"database", "table"},
        {},
        MetricType::Counter
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

    MetricFamily::MetricFamily(
        String name_,
        String documentation_,
        Labels labels_,
        std::vector<LabelValues> initial_label_values,
        MetricType type_)
        : name(std::move(name_))
        , documentation(std::move(documentation_))
        , labels(std::move(labels_))
        , type_string(metricTypeToString(type_))
    {
        for (auto & label_values : initial_label_values)
        {
            withLabels(std::move(label_values));
        }
    }

    Metric & MetricFamily::withLabels(LabelValues label_values)
    {
        chassert(label_values.size() == labels.size());
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

    template <typename Func>
    void MetricFamily::updateUnderLock(LabelValues label_values, Func && func)
    {
        chassert(label_values.size() == labels.size());
        {
            std::shared_lock lock(mutex);
            auto it = metrics.find(label_values);
            if (it != metrics.end())
            {
                func(*it->second);
                return;
            }
        }

        std::lock_guard lock(mutex);
        auto [it, _] = metrics.try_emplace(std::move(label_values), std::make_unique<Metric>());
        func(*it->second);
    }

    void MetricFamily::set(LabelValues label_values, Value value)
    {
        updateUnderLock(std::move(label_values), [value](Metric & metric) { metric.set(value); });
    }

    void MetricFamily::add(LabelValues label_values, Value amount)
    {
        updateUnderLock(std::move(label_values), [amount](Metric & metric) { metric.increment(amount); });
    }

    void MetricFamily::sub(LabelValues label_values, Value amount)
    {
        updateUnderLock(std::move(label_values), [amount](Metric & metric) { metric.decrement(amount); });
    }

    bool MetricFamily::remove(const LabelValues & label_values)
    {
        chassert(label_values.size() == labels.size());
        std::lock_guard lock(mutex);
        return metrics.erase(label_values) > 0;
    }

    const Labels & MetricFamily::getLabels() const { return labels; }
    const String & MetricFamily::getName() const { return name; }
    const String & MetricFamily::getDocumentation() const { return documentation; }
    const String & MetricFamily::getTypeString() const { return type_string; }

    void add(MetricFamily & metric, LabelValues labels, Value amount)
    {
        metric.add(std::move(labels), amount);
    }

    void sub(MetricFamily & metric, LabelValues labels, Value amount)
    {
        metric.sub(std::move(labels), amount);
    }

    void set(MetricFamily & metric, LabelValues labels, Value value)
    {
        metric.set(std::move(labels), value);
    }

    bool remove(MetricFamily & metric, const LabelValues & labels)
    {
        return metric.remove(labels);
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
        std::vector<LabelValues> initial_label_values,
        MetricType type)
    {
        std::lock_guard lock(mutex);
        registry.push_back(
            std::make_unique<MetricFamily>(
                std::move(name),
                std::move(documentation),
                std::move(labels),
                std::move(initial_label_values),
                type
            )
        );
        return *registry.back();
    }
}
