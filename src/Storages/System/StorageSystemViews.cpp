#include <Storages/System/StorageSystemViews.h>
#include <DataTypes/DataTypeString.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryViewsLog.h>
#include <DataTypes/DataTypeEnum.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/LiveView/StorageLiveView.h>

namespace DB
{

class Context;

NamesAndTypesList StorageSystemViews::getNamesAndTypes()
{
    auto view_type_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Default", static_cast<Int8>(QueryViewsLogElement::ViewType::DEFAULT)},
        {"Materialized", static_cast<Int8>(QueryViewsLogElement::ViewType::MATERIALIZED)},
        {"Live", static_cast<Int8>(QueryViewsLogElement::ViewType::LIVE)}});

    return {
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"table_database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"type", std::move(view_type_datatype)},
    };
}

void StorageSystemViews::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & [table_id, view_ids] : DatabaseCatalog::instance().getViewDependencies())
    {
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, table_id.database_name);

        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, table_id.database_name, table_id.table_name))
            continue;

        size_t col_num;
        for (const auto & view_id : view_ids)
        {
            auto view_ptr = DatabaseCatalog::instance().getTable(view_id, context);
            QueryViewsLogElement::ViewType type = QueryViewsLogElement::ViewType::DEFAULT;

            if (typeid_cast<const StorageMaterializedView *>(view_ptr.get()))
            {
                type = QueryViewsLogElement::ViewType::MATERIALIZED;
            }
            else if (typeid_cast<const StorageLiveView *>(view_ptr.get()))
            {
                type = QueryViewsLogElement::ViewType::LIVE;
            }

            col_num = 0;
            res_columns[col_num++]->insert(view_id.database_name);
            res_columns[col_num++]->insert(view_id.table_name);
            res_columns[col_num++]->insert(table_id.database_name);
            res_columns[col_num++]->insert(table_id.table_name);
            res_columns[col_num++]->insert(type);
        }
    }
}

}
