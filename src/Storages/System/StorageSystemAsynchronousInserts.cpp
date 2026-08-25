#include <Storages/System/StorageSystemAsynchronousInserts.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Core/DecimalFunctions.h>
#include <Parsers/ASTInsertQuery.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>

namespace DB
{

static constexpr auto TIME_SCALE = 6;

ColumnsDescription StorageSystemAsynchronousInserts::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"query", std::make_shared<DataTypeString>(), "Query text."},
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"format", std::make_shared<DataTypeString>(), "Format name."},
        {"first_update", std::make_shared<DataTypeDateTime64>(TIME_SCALE), "First insert time with microseconds resolution."},
        {"total_bytes", std::make_shared<DataTypeUInt64>(), "Total number of bytes waiting in the queue."},
        {"entries.query_id", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Array of query ids of the inserts waiting in the queue."},
        {"entries.bytes", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "Array of bytes of each insert query waiting in the queue."},
    };
}

void StorageSystemAsynchronousInserts::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    using namespace std::chrono;

    auto * insert_queue = context->tryGetAsynchronousInsertQueue();
    if (!insert_queue)
        return;

    const auto current_user_id = context->getUserID();
    const bool show_all = context->getAccess()->isGranted(AccessType::SHOW_USERS);

    for (size_t shard_num = 0; shard_num < insert_queue->getPoolSize(); ++shard_num)
    {
        auto [queue, queue_lock] = insert_queue->getQueueLocked(shard_num);

        for (const auto & [first_update, elem] : queue)
        {
            const auto & [key, data] = elem;

            if (!show_all && key.user_id != current_user_id)
                continue;

            auto time_in_microseconds = [](const time_point<steady_clock> & timestamp)
            {
                auto time_diff = duration_cast<microseconds>(steady_clock::now() - timestamp);
                auto time_us = (system_clock::now() - time_diff).time_since_epoch().count();

                DecimalUtils::DecimalComponents<DateTime64> components{time_us / 1'000'000, time_us % 1'000'000};
                return DecimalField<DateTime64>(DecimalUtils::decimalFromComponents<DateTime64>(components, TIME_SCALE), TIME_SCALE);
            };

            const auto & insert_query = key.query->as<const ASTInsertQuery &>();
            size_t i = 0;

            res_columns[i++]->insert(insert_query.formatForLogging());

            /// If query is "INSERT INTO FUNCTION" then table_id is empty.
            if (insert_query.table_id)
            {
                res_columns[i++]->insert(insert_query.table_id.getDatabaseName());
                res_columns[i++]->insert(insert_query.table_id.getTableName());
            }
            else
            {
                res_columns[i++]->insertDefault();
                res_columns[i++]->insertDefault();
            }

            res_columns[i++]->insert(insert_query.format);
            res_columns[i++]->insert(time_in_microseconds(first_update));
            res_columns[i++]->insert(data->size_in_bytes);

            Array arr_query_id;
            Array arr_bytes;

            for (const auto & entry : data->entries)
            {
                arr_query_id.push_back(entry->query_id);
                arr_bytes.push_back(entry->chunk.byteSize());
            }

            res_columns[i++]->insert(arr_query_id);
            res_columns[i++]->insert(arr_bytes);
        }
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemAsynchronousInserts) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "asynchronous_inserts",
    .description = R"DOCS_MD(
Contains information about pending asynchronous inserts in queue.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql title="Query"
SELECT * FROM system.asynchronous_inserts LIMIT 1 \G;
```

```text title="Response"
Row 1:
──────
query:            INSERT INTO public.data_guess (user_id, datasource_id, timestamp, path, type, num, str) FORMAT CSV
database:         public
table:            data_guess
format:           CSV
first_update:     2023-06-08 10:08:54.199606
total_bytes:      133223
entries.query_id: ['b46cd4c4-0269-4d0b-99f5-d27668c6102e']
entries.bytes:    [133223]
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [system.query_log](/reference/system-tables/query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.asynchronous_insert_log](/reference/system-tables/asynchronous_insert_log) — This table contains information about async inserts performed.
)DOCS_MD")

}
