#include <Interpreters/getTableExpressions.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Storages/IStorage.h>

namespace DB
{

NameSet removeDuplicateColumns(NamesAndTypesList & columns)
{
    NameSet names;
    for (auto it = columns.begin(); it != columns.end();)
    {
        if (names.emplace(it->name).second)
            ++it;
        else
            it = columns.erase(it);
    }
    return names;
}

std::vector<const ASTTableExpression *> getTableExpressions(const ASTSelectQuery & select_query)
{
    if (!select_query.tables())
        return {};

    std::vector<const ASTTableExpression *> tables_expression;

    for (const auto & child : select_query.tables()->children)
    {
        const auto * tables_element = child->as<ASTTablesInSelectQueryElement>();

        if (tables_element && tables_element->table_expression)
            tables_expression.emplace_back(tables_element->table_expression->as<ASTTableExpression>());
    }

    return tables_expression;
}

const ASTTableExpression * getTableExpression(const ASTSelectQuery & select, size_t table_number)
{
    if (!select.tables())
        return {};

    const auto & tables_in_select_query = select.tables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.size() <= table_number)
        return {};

    const auto & tables_element = tables_in_select_query.children[table_number]->as<ASTTablesInSelectQueryElement &>();

    if (!tables_element.table_expression)
        return {};

    return tables_element.table_expression->as<ASTTableExpression>();
}

ASTPtr extractTableExpression(const ASTSelectQuery & select, size_t table_number)
{
    if (const ASTTableExpression * table_expression = getTableExpression(select, table_number))
    {
        if (table_expression->database_and_table_name)
            return table_expression->database_and_table_name;

        if (table_expression->table_function)
            return table_expression->table_function;

        if (table_expression->subquery)
            return table_expression->subquery->children[0];
    }

    return nullptr;
}

bool hasWithTotalsInAnySubqueryInFromClause(const ASTSelectQuery & query)
{
    if (query.group_by_with_totals)
        return true;

    /** NOTE You can also check that the table in the subquery is distributed, and that it only looks at one shard.
     * In other cases, totals will be computed on the initiating server of the query, and it is not necessary to read the data to the end.
     */
    if (auto query_table = extractTableExpression(query, 0))
    {
        if (const auto * ast_union = query_table->as<ASTSelectWithUnionQuery>())
        {
            /** NOTE
            * 1. For ASTSelectWithUnionQuery after normalization for union child node the height of the AST tree is at most 2.
            * 2. For ASTSelectIntersectExceptQuery after normalization in case there are intersect or except nodes,
            * the height of the AST tree can have any depth (each intersect/except adds a level), but the
            * number of children in those nodes is always 2.
            */
            std::function<bool(ASTPtr)> traverse_recursively = [&](ASTPtr child_ast) -> bool
            {
                if (const auto * select_child = child_ast->as<ASTSelectQuery>())
                {
                    if (hasWithTotalsInAnySubqueryInFromClause(select_child->as<ASTSelectQuery &>()))
                        return true;
                }
                else if (const auto * union_child = child_ast->as<ASTSelectWithUnionQuery>())
                {
                    for (const auto & subchild : union_child->list_of_selects->children)
                        if (traverse_recursively(subchild))
                            return true;
                }
                else if (const auto * intersect_child = child_ast->as<ASTSelectIntersectExceptQuery>())
                {
                    auto selects = intersect_child->getListOfSelects();
                    for (const auto & subchild : selects)
                        if (traverse_recursively(subchild))
                            return true;
                }
                return false;
            };

            for (const auto & elem : ast_union->list_of_selects->children)
                if (traverse_recursively(elem))
                    return true;
        }
    }

    return false;
}

namespace
{

bool rangeSelectNeedsTotalsDrain(const ASTSelectQuery & query)
{
    if (!query.limitAfter() && !query.limitUntil())
        return false;
    if (query.group_by_with_totals)
        return true;
    return hasWithTotalsInAnySubqueryInFromClause(query);
}

}

bool rangeBranchNeedsTotalsDrain(const ASTPtr & ast)
{
    if (const auto * select = ast->as<ASTSelectQuery>())
        return rangeSelectNeedsTotalsDrain(*select);

    if (const auto * union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        for (const auto & child : union_query->list_of_selects->children)
            if (rangeBranchNeedsTotalsDrain(child))
                return true;
        return false;
    }

    if (const auto * intersect_except_query = ast->as<ASTSelectIntersectExceptQuery>())
    {
        for (const auto & child : intersect_except_query->getListOfSelects())
            if (rangeBranchNeedsTotalsDrain(child))
                return true;
    }

    return false;
}

/// The parameter is_create_parameterized_view is used in getSampleBlock of the subquery.
/// If it is set to true, then query parameters are allowed in the subquery, and that expression is not evaluated.
static NamesAndTypesList getColumnsFromTableExpression(
    const ASTTableExpression & table_expression,
    ContextPtr context,
    NamesAndTypesList & materialized,
    NamesAndTypesList & aliases,
    NamesAndTypesList & virtuals,
    bool is_create_parameterized_view)
{
    NamesAndTypesList names_and_type_list;
    if (table_expression.subquery)
    {
        const auto & subquery = table_expression.subquery->children.at(0);
        names_and_type_list = InterpreterSelectWithUnionQuery::getSampleBlock(subquery, context, true, is_create_parameterized_view)->getNamesAndTypesList();
    }
    else if (table_expression.table_function)
    {
        const auto table_function = table_expression.table_function;
        auto query_context = context->getQueryContext();

        /// For parameterized views in refreshable materialized views, use the current context's database
        /// instead of the query context's database. This ensures unqualified parameterized view
        /// references resolve in the correct database (MV's database, not session's database).
        if (is_create_parameterized_view)
        {
            query_context = Context::createCopy(query_context);
            query_context->setCurrentDatabase(context->getCurrentDatabase());
        }

        const auto & function_storage = query_context->executeTableFunction(table_function);
        auto function_metadata_snapshot = function_storage->getInMemoryMetadataPtr(query_context, false);
        const auto & columns = function_metadata_snapshot->getColumns();
        names_and_type_list = columns.getOrdinary();
        materialized = columns.getMaterialized();
        aliases = columns.getAliases();
        virtuals = function_metadata_snapshot->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::All).getNamesAndTypesList();
    }
    else if (table_expression.database_and_table_name)
    {
        auto table_id = context->resolveStorageID(table_expression.database_and_table_name);
        const auto & table = DatabaseCatalog::instance().getTable(table_id, context);
        auto table_metadata_snapshot = table->getInMemoryMetadataPtr(context, false);
        const auto & columns = table_metadata_snapshot->getColumns();
        names_and_type_list = columns.getOrdinary();
        materialized = columns.getMaterialized();
        aliases = columns.getAliases();
        virtuals = table_metadata_snapshot->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::All).getNamesAndTypesList();
    }

    return names_and_type_list;
}

TablesWithColumns getDatabaseAndTablesWithColumns(
        const ASTTableExprConstPtrs & table_expressions,
        ContextPtr context,
        bool include_alias_cols,
        bool include_materialized_cols,
        bool is_create_parameterized_view)
{
    TablesWithColumns tables_with_columns;

    String current_database = context->getCurrentDatabase();

    for (const ASTTableExpression * table_expression : table_expressions)
    {
        NamesAndTypesList materialized;
        NamesAndTypesList aliases;
        NamesAndTypesList virtuals;
        NamesAndTypesList names_and_types = getColumnsFromTableExpression(
            *table_expression, context, materialized, aliases, virtuals, is_create_parameterized_view);

        removeDuplicateColumns(names_and_types);

        tables_with_columns.emplace_back(
            DatabaseAndTableWithAlias(*table_expression, current_database), names_and_types);

        auto & table = tables_with_columns.back();
        table.addHiddenColumns(materialized);
        table.addHiddenColumns(aliases);
        table.addHiddenColumns(virtuals);

        if (include_alias_cols)
        {
            table.addAliasColumns(aliases);
        }

        if (include_materialized_cols)
        {
            table.addMaterializedColumns(materialized);
        }
    }

    return tables_with_columns;
}

}
