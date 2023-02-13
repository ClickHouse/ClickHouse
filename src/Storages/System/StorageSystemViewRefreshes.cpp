#include <Storages/System/StorageSystemViewRefreshes.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Storages/MaterializedView/RefreshSet.h>


namespace DB
{

NamesAndTypesList StorageSystemViewRefreshes::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"view", std::make_shared<DataTypeString>()},
        {"refresh_status", std::make_shared<DataTypeString>()},
        {"last_refresh_status", std::make_shared<DataTypeString>()},
        {"last_refresh_time", std::make_shared<DataTypeDateTime>()},
        {"next_refresh_time", std::make_shared<DataTypeDateTime>()},
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
    // TODO: Do we need to add new access type?
    auto valid_access = AccessType::SHOW_TABLES;
    bool check_access_for_tables = !access->isGranted(valid_access);


    for (const auto & refresh : context->getRefreshSet().getInfo())
    {
        if (check_access_for_tables && !access->isGranted(valid_access, refresh.database, refresh.view_name))
            continue;

        std::size_t i = 0;
        res_columns[i++]->insert(refresh.database);
        res_columns[i++]->insert(refresh.view_name);
        res_columns[i++]->insert(refresh.refresh_status);
        res_columns[i++]->insert(refresh.last_refresh_status);
        res_columns[i++]->insert(refresh.last_refresh_time);
        res_columns[i++]->insert(refresh.next_refresh_time);
        res_columns[i++]->insert(refresh.progress);
        res_columns[i++]->insert(refresh.elapsed_ns);
        res_columns[i++]->insert(refresh.read_rows);
        res_columns[i++]->insert(refresh.read_bytes);
        res_columns[i++]->insert(refresh.total_rows_to_read);
        res_columns[i++]->insert(refresh.total_bytes_to_read);
        res_columns[i++]->insert(refresh.written_rows);
        res_columns[i++]->insert(refresh.written_bytes);
        res_columns[i++]->insert(refresh.result_rows);
        res_columns[i++]->insert(refresh.result_bytes);
    }
}

}
