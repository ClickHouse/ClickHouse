#include <Interpreters/RejectTablePathsVisitor.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Common/Exception.h>
#include <Common/checkStackSize.h>
#include <Common/quoteString.h>

#include <optional>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_table_namespaces;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// A name is looked up in an enclosing scope only while `enable_global_with_statement` holds in the
/// subquery's own context, so a CTE name that a subquery does not see is a table name there.
/// The clause is clamped rather than rejected, so a subquery cannot widen the reader's constraints,
/// and it is applied to a copy, so the AST keeps the clause as written. This mirrors
/// `applyQueryLevelSettings` in `QueryTreeBuilder`, including `SETTINGS name = DEFAULT`, which is
/// parsed into `default_settings` rather than `changes`.
ContextPtr getSubqueryContext(const ASTPtr & settings_ast, const ContextPtr & context)
{
    if (!settings_ast)
        return context;

    const auto & set_query = settings_ast->as<const ASTSetQuery &>();
    auto subquery_context = Context::createCopy(context);
    subquery_context->applyNestedQuerySettingsClamped(set_query.changes, set_query.default_settings);
    return subquery_context;
}

/// A trailing `SETTINGS` clause of a set operation with several operands is query-level: it applies
/// to every operand, not only to the last one, where `ParserSelectQuery` leaves it. This mirrors
/// `QueryTreeBuilder::buildSelectWithUnionExpression`, so an element is substituted into an operand
/// exactly when the analyzer resolves the name there as that element.
ContextPtr getSetOperationContext(const ASTs & operands, const ASTPtr & settings_ast, const ContextPtr & context)
{
    ContextPtr result = getSubqueryContext(settings_ast, context);
    /// The clause may be nested into `INTERSECT` / `EXCEPT` nodes as the last operand of each, but not into
    /// a parenthesized operand, which keeps its clause to itself.
    if (operands.size() > 1)
        result = getSubqueryContext(ASTSelectIntersectExceptQuery::getTrailingSettings(operands.back()), result);
    return result;
}

/// The path with dots and its quoted spelling.
struct FoldedPath
{
    String with_dots;
    String quoted;
};

FoldedPath makePath(const String & database, const String & table)
{
    if (database.empty())
        return {table, backQuoteIfNeed(table)};
    return {database + "." + table, backQuoteIfNeed(database) + "." + backQuoteIfNeed(table)};
}

String identifierName(const ASTPtr & ast)
{
    String name;
    tryGetIdentifierNameInto(ast, name);
    return name;
}

/// The path recorded as folded by this node, if any.
std::optional<FoldedPath> getFoldedPath(const IAST & node)
{
    if (const auto * identifier = node.as<ASTTableIdentifier>())
    {
        if (identifier->is_folded_path)
            return FoldedPath{identifier->name(), identifier->formatForErrorMessage()};
        return {};
    }
    if (const auto * query = dynamic_cast<const ASTQueryWithTableAndOutput *>(&node))
    {
        if (query->table_is_folded_path)
            return makePath(query->getDatabase(), query->getTable());
        return {};
    }
    if (const auto * insert = node.as<ASTInsertQuery>())
    {
        if (insert->table_is_folded_path)
        {
            if (insert->table)
                return makePath(identifierName(insert->database), identifierName(insert->table));
            return makePath(insert->table_id.database_name, insert->table_id.table_name);
        }
        return {};
    }
    if (const auto * system = node.as<ASTSystemQuery>())
    {
        if (system->table_is_folded_path)
            return makePath(system->getDatabase(), system->getTable());
        return {};
    }
    if (const auto * show_columns = node.as<ASTShowColumnsQuery>())
    {
        if (show_columns->table_is_folded_path)
            return makePath(show_columns->database, show_columns->table);
        return {};
    }
    if (const auto * show_indexes = node.as<ASTShowIndexesQuery>())
    {
        if (show_indexes->table_is_folded_path)
            return makePath(show_indexes->database, show_indexes->table);
        return {};
    }
    if (const auto * rename = node.as<ASTRenameQuery>())
    {
        for (const auto & element : rename->getElements())
        {
            for (const auto * side : {&element.from, &element.to})
            {
                if (side->is_folded_path)
                    return makePath(side->getDatabase(), side->getTable());
            }
        }
        return {};
    }
    if (const auto * targets = node.as<ASTViewTargets>())
    {
        if (!targets->has_folded_table_path)
            return {};
        for (const auto & target : targets->targets)
        {
            if (!target.table_id.table_name.empty())
                return makePath(target.table_id.database_name, target.table_id.table_name);
            if (const auto * table_ast = target.table_ast ? target.table_ast->as<ASTTableIdentifier>() : nullptr)
                return FoldedPath{table_ast->name(), table_ast->formatForErrorMessage()};
        }
        return {};
    }
    return {};
}

bool containsFoldedPath(const ASTPtr & ast)
{
    checkStackSize();
    if (getFoldedPath(*ast))
        return true;
    for (const auto & child : ast->children)
        if (containsFoldedPath(child))
            return true;
    return false;
}

void visitScoped(const ASTPtr & ast, const ContextPtr & context)
{
    checkStackSize();

    if (auto path = getFoldedPath(*ast); path && !context->getSettingsRef()[Setting::allow_experimental_table_namespaces])
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Table path '{}' requires the setting allow_experimental_table_namespaces; write {} or enable the setting",
            path->with_dots,
            path->quoted);

    ContextPtr child_context = context;
    if (const auto * union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        if (union_query->list_of_selects)
            child_context = getSetOperationContext(union_query->list_of_selects->children, union_query->settings_ast, context);
    }
    else if (const auto * intersect_except = ast->as<ASTSelectIntersectExceptQuery>())
        child_context = getSetOperationContext(intersect_except->getListOfSelects(), nullptr, context);
    else if (const auto * select = ast->as<ASTSelectQuery>())
        child_context = getSubqueryContext(select->settings(), context);

    for (const auto & child : ast->children)
        visitScoped(child, child_context);
}

}

void RejectTablePathsVisitor::visit(const ASTPtr & ast, const ContextPtr & context)
{
    if (!containsFoldedPath(ast))
        return;
    visitScoped(ast, context);
}

}
