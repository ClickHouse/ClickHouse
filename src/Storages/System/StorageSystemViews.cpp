#include <Storages/System/StorageSystemViews.h>
#include <DataTypes/DataTypeString.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>

namespace DB
{

class Context;

NamesAndTypesList StorageSystemViews::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"table_database", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemViews::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    const auto access = context.getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & [table_id, view_ids] : DatabaseCatalog::instance().getViewDependencies())
    {
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, table_id.database_name);

        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, table_id.database_name, table_id.table_name))
            continue;

        size_t col_num;
        for (const auto & view_id : view_ids)
        {
            col_num = 0;
            res_columns[col_num++]->insert(table_id.database_name);
            res_columns[col_num++]->insert(table_id.table_name);
            res_columns[col_num++]->insert(view_id.database_name);
            res_columns[col_num++]->insert(view_id.table_name);
        }
    }
}

}
