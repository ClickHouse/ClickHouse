#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>

#include <Storages/IStorage.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

void AnalyzedJoin::addUsingKey(const ASTPtr & ast)
{
    key_names_left.push_back(ast->getColumnName());
    key_names_right.push_back(ast->getAliasOrColumnName());

    key_asts_left.push_back(ast);
    key_asts_right.push_back(ast);

    auto & right_key = key_names_right.back();
    if (renames.count(right_key))
        right_key = renames[right_key];
}

void AnalyzedJoin::addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast)
{
    with_using = false;
    key_names_left.push_back(left_table_ast->getColumnName());
    key_names_right.push_back(right_table_ast->getAliasOrColumnName());

    key_asts_left.push_back(left_table_ast);
    key_asts_right.push_back(right_table_ast);
}

/// @return how many times right key appears in ON section.
size_t AnalyzedJoin::rightKeyInclusion(const String & name) const
{
    if (with_using)
        return 0;

    size_t count = 0;
    for (const auto & key_name : key_names_right)
        if (name == key_name)
            ++count;
    return count;
}

ExpressionActionsPtr AnalyzedJoin::createJoinedBlockActions(
    const NamesAndTypesList & columns_added_by_join,
    const ASTSelectQuery * select_query_with_join,
    const Context & context) const
{
    if (!select_query_with_join)
        return nullptr;

    const ASTTablesInSelectQueryElement * join = select_query_with_join->join();

    if (!join)
        return nullptr;

    const auto & join_params = join->table_join->as<ASTTableJoin &>();

    /// Create custom expression list with join keys from right table.
    auto expression_list = std::make_shared<ASTExpressionList>();
    ASTs & children = expression_list->children;

    if (join_params.on_expression)
        for (const auto & join_right_key : key_asts_right)
            children.emplace_back(join_right_key);

    NameSet required_columns_set(key_names_right.begin(), key_names_right.end());
    for (const auto & joined_column : columns_added_by_join)
        required_columns_set.insert(joined_column.name);
    Names required_columns(required_columns_set.begin(), required_columns_set.end());

    ASTPtr query = expression_list;
    auto syntax_result = SyntaxAnalyzer(context).analyze(query, columns_from_joined_table, required_columns);
    ExpressionAnalyzer analyzer(query, syntax_result, context, required_columns_set);
    return analyzer.getActions(true, false);
}

void AnalyzedJoin::deduplicateAndQualifyColumnNames(const NameSet & left_table_columns, const String & right_table_prefix)
{
    NameSet joined_columns;
    NamesAndTypesList dedup_columns;

    for (auto & column : columns_from_joined_table)
    {
        if (joined_columns.count(column.name))
            continue;

        joined_columns.insert(column.name);

        dedup_columns.push_back(column);
        auto & inserted = dedup_columns.back();

        if (left_table_columns.count(column.name))
            inserted.name = right_table_prefix + column.name;

        original_names[inserted.name] = column.name;
        if (inserted.name != column.name)
            renames[column.name] = inserted.name;
    }

    columns_from_joined_table.swap(dedup_columns);
}

NameSet AnalyzedJoin::getQualifiedColumnsSet() const
{
    NameSet out;
    for (const auto & names : original_names)
        out.insert(names.first);
    return out;
}

NameSet AnalyzedJoin::getOriginalColumnsSet() const
{
    NameSet out;
    for (const auto & names : original_names)
        out.insert(names.second);
    return out;
}

std::unordered_map<String, String> AnalyzedJoin::getOriginalColumnsMap(const NameSet & required_columns) const
{
    std::unordered_map<String, String> out;
    for (const auto & column : required_columns)
    {
        auto it = original_names.find(column);
        if (it != original_names.end())
            out.insert(*it);
    }
    return out;
}

void AnalyzedJoin::calculateAvailableJoinedColumns(bool make_nullable)
{
    if (!make_nullable)
    {
        available_joined_columns = columns_from_joined_table;
        return;
    }

    for (auto & column : columns_from_joined_table)
    {
        auto type = column.type->canBeInsideNullable() ? makeNullable(column.type) : column.type;
        available_joined_columns.emplace_back(NameAndTypePair(column.name, std::move(type)));
    }
}


NamesAndTypesList getNamesAndTypeListFromTableExpression(const ASTTableExpression & table_expression, const Context & context)
{
    NamesAndTypesList names_and_type_list;
    if (table_expression.subquery)
    {
        const auto & subquery = table_expression.subquery->children.at(0);
        names_and_type_list = InterpreterSelectWithUnionQuery::getSampleBlock(subquery, context).getNamesAndTypesList();
    }
    else if (table_expression.table_function)
    {
        const auto table_function = table_expression.table_function;
        auto query_context = const_cast<Context *>(&context.getQueryContext());
        const auto & function_storage = query_context->executeTableFunction(table_function);
        names_and_type_list = function_storage->getSampleBlockNonMaterialized().getNamesAndTypesList();
    }
    else if (table_expression.database_and_table_name)
    {
        DatabaseAndTableWithAlias database_table(table_expression.database_and_table_name);
        const auto & table = context.getTable(database_table.database, database_table.table);
        names_and_type_list = table->getSampleBlockNonMaterialized().getNamesAndTypesList();
    }

    return names_and_type_list;
}

}
