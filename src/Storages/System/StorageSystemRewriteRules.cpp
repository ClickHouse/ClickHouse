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
#include <Common/RewriteRules/RewriteRuleObject.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>


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

void StorageSystemRewriteRules::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto rules = RewriteRules::instance().getAll();
    for (const auto & [name, rule] : rules)
    {
        res_columns[0]->insert(name);
        /// A rule template can embed nested queries whose table functions or settings hold
        /// secrets. Re-format the stored AST with secret hiding (gated by the display-secrets
        /// grant and setting) instead of returning the raw query text, so credentials are not
        /// leaked to every reader of this table.
        res_columns[1]->insert(format({.ctx = context, .query = rule->getCreateQuery()}));
    }
}

}
