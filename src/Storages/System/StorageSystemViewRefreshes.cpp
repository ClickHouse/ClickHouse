#include <Storages/System/StorageSystemViewRefreshes.h>

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/MaterializedView/RefreshSet.h>


namespace DB
{

NamesAndTypesList StorageSystemViewRefreshes::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"view", std::make_shared<DataTypeString>()},
        {"status", std::make_shared<DataTypeString>()},
        {"last_refresh_result", std::make_shared<DataTypeString>()},
        {"last_refresh_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())},
        {"next_refresh_time", std::make_shared<DataTypeDateTime>()},
        {"remaining_dependencies", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"exception", std::make_shared<DataTypeString>()},
        {"progress", std::make_shared<DataTypeFloat64>()},
        {"elapsed", std::make_shared<DataTypeFloat64>()},
        {"read_rows", std::make_shared<DataTypeUInt64>()},
        {"read_bytes", std::make_shared<DataTypeUInt64>()},
        {"total_rows", std::make_shared<DataTypeUInt64>()},
        {"total_bytes", std::make_shared<DataTypeUInt64>()},
        {"written_rows", std::make_shared<DataTypeUInt64>()},
        {"written_bytes", std::make_shared<DataTypeUInt64>()},
        {"result_rows", std::make_shared<DataTypeUInt64>()},
        {"result_bytes", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemViewRefreshes::fillData(
    MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto access = context->getAccess();
    auto valid_access = AccessType::SHOW_TABLES;
    bool check_access_for_tables = !access->isGranted(valid_access);

    for (const auto & refresh : context->getRefreshSet().getInfo())
    {
        if (check_access_for_tables && !access->isGranted(valid_access, refresh.view_id.getDatabaseName(), refresh.view_id.getTableName()))
            continue;

        std::size_t i = 0;
        res_columns[i++]->insert(refresh.view_id.getDatabaseName());
        res_columns[i++]->insert(refresh.view_id.getTableName());
        res_columns[i++]->insert(toString(refresh.state));
        res_columns[i++]->insert(toString(refresh.last_refresh_result));

        if (refresh.last_refresh_time.has_value())
            res_columns[i++]->insert(refresh.last_refresh_time.value());
        else
            res_columns[i++]->insertDefault(); // NULL

        res_columns[i++]->insert(refresh.next_refresh_time);

        Array deps;
        for (const StorageID & id : refresh.remaining_dependencies)
            deps.push_back(id.getFullTableName());
        res_columns[i++]->insert(Array(deps));

        res_columns[i++]->insert(refresh.exception_message);
        res_columns[i++]->insert(Float64(refresh.progress.read_rows) / refresh.progress.total_rows_to_read);
        res_columns[i++]->insert(refresh.progress.elapsed_ns);
        res_columns[i++]->insert(refresh.progress.read_rows);
        res_columns[i++]->insert(refresh.progress.read_bytes);
        res_columns[i++]->insert(refresh.progress.total_rows_to_read);
        res_columns[i++]->insert(refresh.progress.total_bytes_to_read);
        res_columns[i++]->insert(refresh.progress.written_rows);
        res_columns[i++]->insert(refresh.progress.written_bytes);
        res_columns[i++]->insert(refresh.progress.result_rows);
        res_columns[i++]->insert(refresh.progress.result_bytes);
    }
}

}
