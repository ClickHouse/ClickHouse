#include <Interpreters/MutationPredicateColumnsAccess.h>

#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

void addExpressionColumnsSelectAccess(
    AccessRightsElements & required_access,
    const IAST * expression,
    const String & database,
    const String & table,
    const StorageInMemoryMetadata & metadata)
{
    if (!expression)
        return;

    RequiredSourceColumnsVisitor::Data columns_context;
    auto expression_clone = expression->clone();
    RequiredSourceColumnsVisitor(columns_context).visit(expression_clone);

    Strings columns;
    const String db_table_prefix = database.empty() ? String{} : database + "." + table + ".";
    const String table_prefix = table + ".";
    for (const auto & name : columns_context.requiredColumns())
    {
        /// A real column (including a real dotted/quoted name like `t.id`) requires SELECT as-is.
        if (metadata.columns.has(name))
        {
            columns.emplace_back(name);
            continue;
        }

        /// A virtual column not shadowed by a real one needs no SELECT grant, as in a plain SELECT.
        if (metadata.isVirtualColumn(name))
            continue;

        /// Otherwise strip a `table.` / `db.table.` qualifier and resolve the bare name the same way.
        std::string_view bare = name;
        if (!db_table_prefix.empty() && bare.starts_with(db_table_prefix))
            bare.remove_prefix(db_table_prefix.size());
        else if (bare.starts_with(table_prefix))
            bare.remove_prefix(table_prefix.size());

        if (metadata.isVirtualColumn(String(bare)))
            continue;

        columns.emplace_back(bare);
    }

    if (!columns.empty())
        required_access.emplace_back(AccessType::SELECT, database, table, columns);
}

}
