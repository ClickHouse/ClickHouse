#include <Storages/System/StorageSystemAsynchronousInserts.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Parsers/queryToString.h>
#include <Core/DecimalFunctions.h>
#include <Parsers/ASTInsertQuery.h>

namespace DB
{

static constexpr auto TIME_SCALE = 6;

NamesAndTypesList StorageSystemAsynchronousInserts::getNamesAndTypes()
{
    return
    {
        {"query", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"format", std::make_shared<DataTypeString>()},
        {"first_update", std::make_shared<DataTypeDateTime64>(TIME_SCALE)},
        {"total_bytes", std::make_shared<DataTypeUInt64>()},
        {"entries.query_id", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"entries.bytes", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
    };
}

void StorageSystemAsynchronousInserts::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    using namespace std::chrono;

    auto * insert_queue = context->getAsynchronousInsertQueue();
    if (!insert_queue)
        return;

    for (size_t shard_num = 0; shard_num < insert_queue->getPoolSize(); ++shard_num)
    {
        auto [queue, queue_lock] = insert_queue->getQueueLocked(shard_num);

        for (const auto & [first_update, elem] : queue)
        {
            const auto & [key, data] = elem;

            auto time_in_microseconds = [](const time_point<steady_clock> & timestamp)
            {
                auto time_diff = duration_cast<microseconds>(steady_clock::now() - timestamp);
                auto time_us = (system_clock::now() - time_diff).time_since_epoch().count();

                DecimalUtils::DecimalComponents<DateTime64> components{time_us / 1'000'000, time_us % 1'000'000};
                return DecimalField(DecimalUtils::decimalFromComponents<DateTime64>(components, TIME_SCALE), TIME_SCALE);
            };

            const auto & insert_query = key.query->as<const ASTInsertQuery &>();
            size_t i = 0;

            res_columns[i++]->insert(queryToString(insert_query));

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
                arr_bytes.push_back(entry->bytes.size());
            }

            res_columns[i++]->insert(arr_query_id);
            res_columns[i++]->insert(arr_bytes);
        }
    }
}

}
