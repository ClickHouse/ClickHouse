#include <Columns/IColumn.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/System/StorageSystemHistogramMetrics.h>
#include <Common/HistogramMetrics.h>


namespace DB
{

ColumnsDescription StorageSystemHistogramMetrics::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"metric", std::make_shared<DataTypeString>(), "Metric name."},
        {"value", std::make_shared<DataTypeFloat64>(), "Metric value."},
        {"description", std::make_shared<DataTypeString>(), "Metric description."},
        {"labels", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "Metric labels."},
    };

    description.setAliases({
        {"name", std::make_shared<DataTypeString>(), "metric"}
    });

    return description;
}

void StorageSystemHistogramMetrics::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = HistogramMetrics::Factory::instance();
    factory.forEachFamily([&res_columns](const HistogramMetrics::MetricFamily & family)
    {
        const auto & buckets = family.getBuckets();
        const auto & labels = family.getLabels();

        family.forEachMetric([&res_columns, &family, &buckets, &labels](const HistogramMetrics::LabelValues & label_values, const HistogramMetrics::Metric & metric)
        {
            Map labels_map;
            for (size_t i = 0; i < label_values.size(); ++i)
            {
                labels_map.push_back(Tuple{labels[i], label_values[i]});
            }

            UInt64 partial_sum = 0;
            for (size_t counter_idx = 0; counter_idx < buckets.size() + 1; ++counter_idx)
            {
                partial_sum += metric.getCounter(counter_idx);

                String le;
                if (counter_idx < buckets.size())
                {
                    WriteBufferFromOwnString wb;
                    wb << buckets[counter_idx];
                    le = std::move(wb.str());
                }
                else
                {
                    le = "+Inf";
                }
                labels_map.push_back(Tuple{"le", le});

                res_columns[0]->insert(family.getName());
                res_columns[1]->insert(static_cast<HistogramMetrics::Value>(partial_sum));
                res_columns[2]->insert(family.getDocumentation());
                res_columns[3]->insert(labels_map);

                labels_map.pop_back();
            }

            // _sum metric
            res_columns[0]->insert(family.getName() + "_sum");
            res_columns[1]->insert(metric.getSum());
            res_columns[2]->insert(family.getDocumentation());
            res_columns[3]->insert(labels_map);
        });
    });
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemHistogramMetrics) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "histogram_metrics",
    .description = R"DOCS_MD(
This table contains histogram metrics that can be calculated instantly and exported in the Prometheus format. It is always up to date. Replaces the deprecated `system.latency_log`.
)DOCS_MD",
    .examples = R"DOCS_MD(
You can use a query like this to export all the histogram metrics in the Prometheus format.
```sql
SELECT
  metric AS name,
  toFloat64(value) AS value,
  description AS help,
  labels,
  'histogram' AS type
FROM system.histogram_metrics
FORMAT Prometheus
```
)DOCS_MD",
    .additional_sections = R"DOCS_MD(
## Metric descriptions {#metric_descriptions}

| Metric | Description |
|---|---|
| `keeper_response_time_ms_bucket` | The response time of Keeper, in milliseconds. |
| `keeper_client_queue_duration_milliseconds_bucket` | Time requests spend waiting to be enqueued and waiting in the queue before being processed by the Keeper client, in milliseconds. |
| `keeper_receive_request_time_milliseconds_bucket` | Time to receive and parse a request from the client in the Keeper TCP handler, in milliseconds. |
| `keeper_dispatcher_requests_queue_time_milliseconds_bucket` | Time a request spends in the Keeper dispatcher requests queue, in milliseconds. |
| `keeper_write_pre_commit_time_milliseconds_bucket` | Time to preprocess a write request before Raft commit, in milliseconds. |
| `keeper_write_commit_time_milliseconds_bucket` | Time to process a write request after Raft commit, in milliseconds. |
| `keeper_dispatcher_responses_queue_time_milliseconds_bucket` | Time a response spends in the Keeper dispatcher responses queue, in milliseconds. |
| `keeper_send_response_time_milliseconds_bucket` | Time to send a response to the client in the Keeper TCP handler (includes queueing and writing to socket), in milliseconds. |
| `keeper_read_wait_for_write_time_milliseconds_bucket` | Time a read request waits for the write request it depends on to complete, in milliseconds. |
| `keeper_read_process_time_milliseconds_bucket` | Time to process a read request in Keeper, in milliseconds. |
| `keeper_batch_size_elements_bucket` | Batch size sent to Raft, in elements. |
| `keeper_batch_size_bytes_bucket` | Batch size sent to Raft, in bytes. |
| `filesystem_cache_evicted_segment_hits_bucket` | Distribution of cache-hit counts on file segments at the moment of their eviction, labelled by cache name. |
| `filesystem_cache_evicted_segment_size_bytes_bucket` | Distribution of byte sizes of evicted file segments, labelled by cache name. |
| `filesystem_cache_evicted_segment_hits_by_user_bucket` | Distribution of cache-hit counts on evicted file segments, labelled by cache name and user id. |
| `filesystem_cache_evicted_segment_size_bytes_by_user_bucket` | Distribution of byte sizes of evicted file segments, labelled by cache name and user id. |
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metric_log](/reference/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD")

}
