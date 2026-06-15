#include <Interpreters/MutationPredicateColumnsAccess.h>

#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/IAST.h>
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

void addExpressionColumnsSelectAccess(
    AccessRightsElements & required_access,
    const IAST * expression,
    const String & database,
    const String & table,
    const VirtualColumnsDescription & virtual_columns)
{
    if (!expression)
        return;

    RequiredSourceColumnsVisitor::Data columns_context;
    auto expression_clone = expression->clone();
    RequiredSourceColumnsVisitor(columns_context).visit(expression_clone);

    Strings columns;
    const String db_table_prefix = database.empty() ? String{} : database + "." + table + ".";
    const String table_prefix = table + ".";
    for (const auto & qualified_name : columns_context.requiredColumns())
    {
        std::string_view name = qualified_name;
        if (!db_table_prefix.empty() && name.starts_with(db_table_prefix))
            name.remove_prefix(db_table_prefix.size());
        else if (name.starts_with(table_prefix))
            name.remove_prefix(table_prefix.size());

        /// Virtual columns are not real data and need no SELECT grant, matching a plain SELECT query.
        if (virtual_columns.has(String(name)))
            continue;

        columns.emplace_back(name);
    }

    if (!columns.empty())
        required_access.emplace_back(AccessType::SELECT, database, table, columns);
}

}
