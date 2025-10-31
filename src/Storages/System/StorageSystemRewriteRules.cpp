#include <Storages/System/StorageSystemRewriteRules.h>

#include <base/EnumReflection.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Access/Common/AccessType.h>
#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnMap.h>
#include <Common/RewriteRules/RewriteRules.h>


namespace DB
{

ColumnsDescription StorageSystemRewriteRules::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the collection."},
        {"rule", std::make_shared<DataTypeString>(), "Create rule query"},
    };
}

StorageSystemRewriteRules::StorageSystemRewriteRules(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemRewriteRules::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto rules = RewriteRules::instance().getAll();
    for (const auto & [name, rule] : rules)
    {
        res_columns[0]->insert(name);
        res_columns[1]->insert(rule->getCreateQuery().whole_query);
    }
}

}
