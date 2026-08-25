#include <Columns/IColumn.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/StorageSystemReplicatedFetches.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>

namespace DB
{

ColumnsDescription StorageSystemReplicatedFetches::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Name of the database."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table."},
        {"elapsed", std::make_shared<DataTypeFloat64>(), "The time elapsed (in seconds) since showing currently running background fetches started."},
        {"progress", std::make_shared<DataTypeFloat64>(), "The percentage of completed work from 0 to 1."},
        {"result_part_name", std::make_shared<DataTypeString>(),
            "The name of the part that will be formed as the result of showing currently running background fetches."},
        {"result_part_path", std::make_shared<DataTypeString>(),
            "Absolute path to the part that will be formed as the result of showing currently running background fetches."},
        {"partition_id", std::make_shared<DataTypeString>(), "ID of the partition."},
        {"total_size_bytes_compressed", std::make_shared<DataTypeUInt64>(), "The total size (in bytes) of the compressed data in the result part."},
        {"bytes_read_compressed", std::make_shared<DataTypeUInt64>(), "The number of compressed bytes read from the result part."},
        {"source_replica_path", std::make_shared<DataTypeString>(), "Absolute path to the source replica."},
        {"source_replica_hostname", std::make_shared<DataTypeString>(), "Hostname of the source replica."},
        {"source_replica_port", std::make_shared<DataTypeUInt16>(), "Port number of the source replica."},
        {"interserver_scheme", std::make_shared<DataTypeString>(), "Name of the interserver scheme."},
        {"URI", std::make_shared<DataTypeString>(), "Uniform resource identifier."},
        {"to_detached", std::make_shared<DataTypeUInt8>(),
            "The flag indicates whether the currently running background fetch is being performed using the TO DETACHED expression."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "Thread identifier."},
    };
}

void StorageSystemReplicatedFetches::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & fetch : context->getReplicatedFetchList().get())
    {
        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, fetch.database, fetch.table))
            continue;

        size_t i = 0;
        res_columns[i++]->insert(fetch.database);
        res_columns[i++]->insert(fetch.table);
        res_columns[i++]->insert(fetch.elapsed);
        res_columns[i++]->insert(fetch.progress);
        res_columns[i++]->insert(fetch.result_part_name);
        res_columns[i++]->insert(fetch.result_part_path);
        res_columns[i++]->insert(fetch.partition_id);
        res_columns[i++]->insert(fetch.total_size_bytes_compressed);
        res_columns[i++]->insert(fetch.bytes_read_compressed);
        res_columns[i++]->insert(fetch.source_replica_path);
        res_columns[i++]->insert(fetch.source_replica_hostname);
        res_columns[i++]->insert(fetch.source_replica_port);
        res_columns[i++]->insert(fetch.interserver_scheme);
        res_columns[i++]->insert(fetch.uri);
        res_columns[i++]->insert(fetch.to_detached);
        res_columns[i++]->insert(fetch.thread_id);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemReplicatedFetches) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "replicated_fetches",
    .description = R"DOCS_MD(
Contains information about currently running background fetches.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.replicated_fetches LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
database:                    default
table:                       t
elapsed:                     7.243039876
progress:                    0.41832135995612835
result_part_name:            all_0_0_0
result_part_path:            /var/lib/clickhouse/store/700/70080a04-b2de-4adf-9fa5-9ea210e81766/all_0_0_0/
partition_id:                all
total_size_bytes_compressed: 1052783726
bytes_read_compressed:       440401920
source_replica_path:         /clickhouse/test/t/replicas/1
source_replica_hostname:     node1
source_replica_port:         9009
interserver_scheme:          http
URI:                         http://node1:9009/?endpoint=DataPartsExchange%3A%2Fclickhouse%2Ftest%2Ft%2Freplicas%2F1&part=all_0_0_0&client_protocol_version=4&compress=false
to_detached:                 0
thread_id:                   54
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [Managing ReplicatedMergeTree Tables](/reference/statements/system#managing-replicatedmergetree-tables)
)DOCS_MD")

}
