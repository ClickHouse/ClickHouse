#include <Storages/System/StorageSystemTableViews.h>
#include <DataTypes/DataTypeString.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>

namespace DB
{

class Context;

NamesAndTypesList StorageSystemTableViews::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"view_database", std::make_shared<DataTypeString>()},
        {"view_table", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemTableViews::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & ) const
{
    const auto access = context.getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    ViewDependencies view_dependencies;
    DatabaseCatalog::instance().getViewDependencies(view_dependencies);

    for (const auto & [storage_id, view_ids] : view_dependencies)
    {
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, storage_id.database_name);

        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, storage_id.database_name, storage_id.table_name))
            continue;

        for (const auto & view_id : view_ids)
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(storage_id.database_name);
            res_columns[col_num++]->insert(storage_id.table_name);
            res_columns[col_num++]->insert(view_id.database_name);
            res_columns[col_num++]->insert(view_id.table_name);
        }
    }
}

}
