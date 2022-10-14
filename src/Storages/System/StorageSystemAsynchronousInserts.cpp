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
        {"last_update", std::make_shared<DataTypeDateTime64>(TIME_SCALE)},
        {"total_bytes", std::make_shared<DataTypeUInt64>()},
        {"entries.query_id", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"entries.bytes", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"entries.finished", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>())},
        {"entries.exception", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
    };
}

void StorageSystemAsynchronousInserts::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    using namespace std::chrono;

    auto * insert_queue = context->getAsynchronousInsertQueue();
    if (!insert_queue)
        return;

    auto [queue, queue_lock] = insert_queue->getQueueLocked();
    for (const auto & [key, elem] : queue)
    {
        std::lock_guard elem_lock(elem->mutex);

        if (!elem->data)
            continue;

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
        res_columns[i++]->insert(time_in_microseconds(elem->data->first_update));
        res_columns[i++]->insert(time_in_microseconds(elem->data->last_update));
        res_columns[i++]->insert(elem->data->size);

        Array arr_query_id;
        Array arr_bytes;
        Array arr_finished;
        Array arr_exception;

        for (const auto & entry : elem->data->entries)
        {
            arr_query_id.push_back(entry->query_id);
            arr_bytes.push_back(entry->bytes.size());
            arr_finished.push_back(entry->isFinished());

            if (auto exception = entry->getException())
            {
                try
                {
                    std::rethrow_exception(exception);
                }
                catch (const Exception & e)
                {
                    arr_exception.push_back(e.displayText());
                }
                catch (...)
                {
                    arr_exception.push_back("Unknown exception");
                }
            }
            else
                arr_exception.push_back("");
        }

        res_columns[i++]->insert(arr_query_id);
        res_columns[i++]->insert(arr_bytes);
        res_columns[i++]->insert(arr_finished);
        res_columns[i++]->insert(arr_exception);
    }
}

}
