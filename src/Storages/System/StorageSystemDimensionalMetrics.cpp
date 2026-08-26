#include <Columns/IColumn.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <Storages/System/StorageSystemDimensionalMetrics.h>
#include <Common/DimensionalMetrics.h>


namespace DB
{

ColumnsDescription StorageSystemDimensionalMetrics::getColumnsDescription()
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

void StorageSystemDimensionalMetrics::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = DimensionalMetrics::Factory::instance();
    factory.forEachFamily([&res_columns](const DimensionalMetrics::MetricFamily & family)
    {
        const auto & labels = family.getLabels();
        family.forEachMetric([&res_columns, &family, &labels](const DimensionalMetrics::LabelValues & label_values, const DimensionalMetrics::Metric & metric)
        {
            Map labels_map;
            for (size_t i = 0; i < label_values.size(); ++i)
            {
                labels_map.push_back(Tuple{labels[i], label_values[i]});
            }
            res_columns[0]->insert(family.getName());
            res_columns[1]->insert(metric.get());
            res_columns[2]->insert(family.getDocumentation());
            res_columns[3]->insert(labels_map);
        });
    });
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDimensionalMetrics) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "dimensional_metrics",
    .description = R"DOCS_MD(
This table contains dimensional metrics that can be calculated instantly and exported in the Prometheus format. It is always up to date.

## Metric descriptions {#metric_descriptions}

### merge_failures {#merge_failures}
Number of all failed merges since startup.

### startup_scripts_failure_reason {#startup_scripts_failure_reason}
Indicates startup scripts failures by error type. Set to 1 when a startup script fails, labelled with the error name.

### merge_tree_parts {#merge_tree_parts}
Number of merge tree data parts, labelled by part state, part type, and whether it is a projection part.

### `filesystem_cache_evictions_total` {#filesystem-cache-evictions-total}
Number of file segments evicted from a filesystem cache, labelled by cache name. Disabled by default; enable with `expose_prometheus_eviction_metrics`.

### `filesystem_cache_evicted_bytes_total` {#filesystem-cache-evicted-bytes-total}
Total bytes of file segments evicted from a filesystem cache, labelled by cache name. Disabled by default; enable with `expose_prometheus_eviction_metrics`.

### `filesystem_cache_evictions_by_user_total` {#filesystem-cache-evictions-by-user-total}
Number of file segments evicted from a filesystem cache, labelled by cache name and user id. Disabled by default; enable with `expose_prometheus_eviction_metrics` and `expose_prometheus_eviction_metrics_per_user`.

### `filesystem_cache_evicted_bytes_by_user_total` {#filesystem-cache-evicted-bytes-by-user-total}
Total bytes of file segments evicted from a filesystem cache, labelled by cache name and user id. Disabled by default; enable with `expose_prometheus_eviction_metrics` and `expose_prometheus_eviction_metrics_per_user`.

### `object_storage_queue_failures_total` {#object-storage-queue-failures-total}
Number of `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) failures, labelled by database, table, processing stage (`read`, `set_processing`, `insert`, `commit`) and error code.

### `object_storage_queue_permanently_failed_files_total` {#object-storage-queue-permanently-failed-files-total}
Number of `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) files given up on for good after exhausting retries (or with retries disabled), labelled by database and table. Each of these represents a file whose data will never be processed.

### `object_storage_queue_newest_seen_object_timestamp_seconds` {#object-storage-queue-newest-seen-object-timestamp-seconds}
Unix timestamp of the last-modified time of the newest object seen so far by an `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) table, labelled by database and table.

### `object_storage_queue_newest_committed_object_timestamp_seconds` {#object-storage-queue-newest-committed-object-timestamp-seconds}
Unix timestamp of the last-modified time of the newest object fully processed so far by an `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) table, labelled by database and table.
)DOCS_MD",
    .examples = R"DOCS_MD(
You can use a query like this to export all the dimensional metrics in the Prometheus format.
```sql
SELECT
  metric AS name,
  toFloat64(value) AS value,
  description AS help,
  labels,
  'gauge' AS type
FROM system.dimensional_metrics
FORMAT Prometheus
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metric_log](/reference/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD")

}
