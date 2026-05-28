#include <Interpreters/ReaderExecutorLog.h>

#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

ColumnsDescription ReaderExecutorLogElement::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"hostname", low_cardinality_string, "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},

        {"query_id", std::make_shared<DataTypeString>(), "Id of the query that created this `ReaderExecutor`."},
        {"source_file_path", std::make_shared<DataTypeString>(), "Cache-key path the executor was reading. Typically the first object's `remote_path`."},
        {"total_size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Total logical size in bytes the executor was set up to read across all objects. `NULL` when the underlying object had no known size (e.g. S3 HEAD without `Content-Length`)."},

        {"cache_hit_bytes", std::make_shared<DataTypeUInt64>(), "Bytes served from cache layers."},
        {"cache_miss_bytes", std::make_shared<DataTypeUInt64>(), "Bytes fetched from source."},
        {"cache_populated_bytes", std::make_shared<DataTypeUInt64>(), "Bytes written back into caches via `put`."},
        {"allocated_bytes", std::make_shared<DataTypeUInt64>(), "Bytes allocated for `OwnedRopeBuffer` during cache `get` and source read."},

        {"cache_get_requests", std::make_shared<DataTypeUInt64>(), "Number of `ICacheHandle::get` invocations."},
        {"cache_populate_requests", std::make_shared<DataTypeUInt64>(), "Number of `ICacheHandle::put` invocations."},
        {"source_requests", std::make_shared<DataTypeUInt64>(), "Number of source-side requests (live-buffer reuses are not counted)."},

        {"cache_get_microseconds", std::make_shared<DataTypeUInt64>(), "Time spent inside `ICacheHandle::get`."},
        {"cache_populate_microseconds", std::make_shared<DataTypeUInt64>(), "Time spent inside `ICacheHandle::put`."},
        {"source_read_microseconds", std::make_shared<DataTypeUInt64>(), "Time spent in source reads (foreground and prefetch worker combined)."},
        {"decrypt_microseconds", std::make_shared<DataTypeUInt64>(), "Time spent in decryption layers."},
        {"prefetch_wait_microseconds", std::make_shared<DataTypeUInt64>(), "Time the consumer blocked on a not-yet-ready prefetch. Contributes directly to query latency."},
        {"sync_read_microseconds", std::make_shared<DataTypeUInt64>(), "Time the consumer spent in an in-line synchronous read because no usable prefetch was available. Contributes directly to query latency."},

        {"prefetch_hits", std::make_shared<DataTypeUInt64>(), "Number of windows served by an in-flight prefetch."},
        {"prefetch_cancelled", std::make_shared<DataTypeUInt64>(), "Number of prefetches cancelled before their worker ran."},
        {"prefetch_pool_full", std::make_shared<DataTypeUInt64>(), "Number of times `PrefetchThreadPool::submit` returned `nullptr` (queue full)."},
    };
}

void ReaderExecutorLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(query_id);
    columns[i++]->insert(source_file_path);
    if (total_size.has_value())
        columns[i++]->insert(*total_size);
    else
        columns[i++]->insertDefault();

    columns[i++]->insert(cache_hit_bytes);
    columns[i++]->insert(cache_miss_bytes);
    columns[i++]->insert(cache_populated_bytes);
    columns[i++]->insert(allocated_bytes);

    columns[i++]->insert(cache_get_requests);
    columns[i++]->insert(cache_populate_requests);
    columns[i++]->insert(source_requests);

    columns[i++]->insert(cache_get_us);
    columns[i++]->insert(cache_populate_us);
    columns[i++]->insert(source_read_us);
    columns[i++]->insert(decrypt_us);
    columns[i++]->insert(prefetch_wait_us);
    columns[i++]->insert(sync_read_us);

    columns[i++]->insert(prefetch_hits);
    columns[i++]->insert(prefetch_cancelled);
    columns[i++]->insert(prefetch_pool_full);
}

}
