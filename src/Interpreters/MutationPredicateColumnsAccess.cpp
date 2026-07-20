#include <Interpreters/MutationPredicateColumnsAccess.h>

#include <Access/Common/AccessRightsElement.h>
#include <Common/Exception.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

bool expressionContainsSubquery(const IAST * node)
{
    if (!node)
        return false;
    if (node->as<ASTSubquery>())
        return true;
    for (const auto & child : node->children)
        if (expressionContainsSubquery(child.get()))
            return true;
    return false;
}

}

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

void rejectMutationSubqueryWhenValidationDisabled(const IAST * expression, bool validate_mutation_query)
{
    if (validate_mutation_query || !expression)
        return;

    if (expressionContainsSubquery(expression))
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "A subquery in a mutation WHERE/SET expression requires validate_mutation_query=1, so that "
            "read access to the columns it reads can be verified");
}

}
