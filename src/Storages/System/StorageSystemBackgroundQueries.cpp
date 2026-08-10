#include <Storages/System/StorageSystemBackgroundQueries.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/BackgroundQueriesDistributedRegistry.h>
#include <Interpreters/Context.h>
#include <Storages/System/SystemTableSourceRegistry.h>


namespace DB
{

ColumnsDescription StorageSystemBackgroundQueries::getColumnsDescription()
{
    auto status_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values{{"Running", 1}, {"Finished", 2}, {"Failed", 3}, {"Unknown", 4}, {"RegistryInternalError", 5}});

    return ColumnsDescription
    {
        {"query_id", std::make_shared<DataTypeString>(), "ID of the query."},
        {"host", std::make_shared<DataTypeString>(), "Fully qualified domain name of the server that runs the query."},
        {"user", std::make_shared<DataTypeString>(), "User who submitted the query."},
        {"query", std::make_shared<DataTypeString>(), "Query text, truncated to the `background_queries_registry_max_query_length` server setting."},
        {"status", status_type, "Status of the query. `Unknown` means that the server running the query stopped reporting its status, so the outcome was not recorded and the query may or may not have completed. `RegistryInternalError` means that the query's entry cannot be read from the registry."},
        {"exception_code", std::make_shared<DataTypeInt32>(), "Code of the exception if the query failed."},
        {"exception", std::make_shared<DataTypeString>(), "Message of the exception if the query failed."},
        {"submit_time", std::make_shared<DataTypeDateTime>(), "Time when the query was submitted."},
        {"finish_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time when the query finished. NULL while the query is running."},
    };
}

void StorageSystemBackgroundQueries::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    context->getBackgroundQueriesDistributedRegistry().forEach([&](const auto & entry)
    {
        size_t column_index = 0;
        res_columns[column_index++]->insert(entry.query_id);
        res_columns[column_index++]->insert(entry.host);
        res_columns[column_index++]->insert(entry.user);
        res_columns[column_index++]->insert(entry.query);
        res_columns[column_index++]->insert(static_cast<Int8>(entry.status));
        res_columns[column_index++]->insert(entry.exception_code);
        res_columns[column_index++]->insert(entry.exception);
        res_columns[column_index++]->insert(static_cast<UInt64>(entry.submit_time));
        if (entry.finish_time)
            res_columns[column_index++]->insert(static_cast<UInt64>(entry.finish_time));
        else
            res_columns[column_index++]->insertDefault();
    });
}

void StorageSystemBackgroundQueries::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    Context::getGlobalContextInstance()->getBackgroundQueriesDistributedRegistry().truncate();
}

}

namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemBackgroundQueries) }
