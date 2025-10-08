#include <Storages/System/StorageSystemRewriteRulesLogs.h>

#include <base/EnumReflection.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Access/Common/AccessType.h>
#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnMap.h>
#include <Common/RewriteRules/RewriteRules.h>


namespace DB
{

ColumnsDescription StorageSystemRewriteRulesLogs::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"original_query", std::make_shared<DataTypeString>(), "Original query"},
        {"applied_rules", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "List of applied rules"},
        {"resulting_query", std::make_shared<DataTypeString>(), "Resulting query"},
    };
}

StorageSystemRewriteRulesLogs::StorageSystemRewriteRulesLogs(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemRewriteRulesLogs::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto logs = RewriteRules::instance().getLogs();
    for (const auto & log : logs)
    {
        res_columns[0]->insert(log->original_query);
        res_columns[1]->insert(log->applied_rules);
        res_columns[2]->insert(log->resulting_query);
    }
}

}
