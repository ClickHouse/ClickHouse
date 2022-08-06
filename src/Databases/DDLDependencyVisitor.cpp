#include <Databases/DDLDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Poco/String.h>


namespace DB
{

namespace
{
    /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW or CREATE TEMPORARY TABLE or CREATE DATABASE query.
    void visitCreateQuery(const ASTCreateQuery & create, DDLDependencyVisitor::Data & data)
    {
        QualifiedTableName to_table{create.to_table_id.database_name, create.to_table_id.table_name};
        if (!to_table.table.empty())
        {
            if (to_table.database.empty())
                to_table.database = data.default_database;
            data.dependencies.emplace(to_table);
        }
    }

    /// ASTTableExpression represents a reference to a table in SELECT query.
    /// DDLDependencyVisitor should handle ASTTableExpression because some CREATE queries can contain SELECT queries after AS
    /// (for example, CREATE VIEW).
    void visitTableExpression(const ASTTableExpression & expr, DDLDependencyVisitor::Data & data)
    {
        if (!expr.database_and_table_name)
            return;

        const ASTIdentifier * identifier = dynamic_cast<const ASTIdentifier *>(expr.database_and_table_name.get());
        if (!identifier)
            return;

        auto table_identifier = identifier->createTable();
        if (!table_identifier)
            return;

        QualifiedTableName qualified_name{table_identifier->getDatabaseName(), table_identifier->shortName()};
        if (qualified_name.table.empty())
            return;

        if (qualified_name.database.empty())
        {
            /// It can be table/dictionary from default database or XML dictionary, but we cannot distinguish it here.
            qualified_name.database = data.default_database;
        }

        data.dependencies.emplace(qualified_name);
    }
}


TableNamesSet getDependenciesSetFromCreateQuery(ContextPtr global_context, const QualifiedTableName & table, const ASTPtr & ast)
{
    assert(global_context == global_context->getGlobalContext());
    TableLoadingDependenciesVisitor::Data data;
    data.default_database = global_context->getCurrentDatabase();
    data.create_query = ast;
    data.global_context = global_context;
    TableLoadingDependenciesVisitor visitor{data};
    visitor.visit(ast);
    data.dependencies.erase(table);
    return data.dependencies;
}

void DDLDependencyVisitor::visit(const ASTPtr & ast, Data & data)
{
    /// Looking for functions in column default expressions and dictionary source definition
    if (auto * create = ast->as<ASTCreateQuery>())
        visitCreateQuery(*create, data);
    else if (const auto * function = ast->as<ASTFunction>())
        visit(*function, data);
    else if (const auto * dict_source = ast->as<ASTFunctionWithKeyValueArguments>())
        visit(*dict_source, data);
    else if (const auto * storage = ast->as<ASTStorage>())
        visit(*storage, data);
    else if (auto * expr = ast->as<ASTTableExpression>())
        visitTableExpression(*expr, data);
}

bool DDLDependencyVisitor::needChildVisit(const ASTPtr &, const ASTPtr &)
{
    return true;
}

void DDLDependencyVisitor::visit(const ASTFunction & function, Data & data)
{
    if (function.name == "joinGet" || function.name == "dictHas" || function.name == "dictIsIn" || function.name.starts_with("dictGet"))
    {
        /// dictGet('dict_name', attr_names, id_expr)
        /// dictHas('dict_name', id_expr)
        /// joinGet(join_storage_table_name, `value_column`, join_keys)
        extractTableNameFromArgument(function, data, 0);
    }
    else if (function.name == "in" || function.name == "notIn" || function.name == "globalIn" || function.name == "globalNotIn")
    {
        /// in(x, table_name) - function for evaluating (x IN table_name)
        extractTableNameFromArgument(function, data, 1);
    }
    else if (function.name == "dictionary")
    {
        /// dictionary(dict_name)
        extractTableNameFromArgument(function, data, 0);
    }
}

void DDLDependencyVisitor::visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data)
{
    if (dict_source.name != "clickhouse")
        return;
    if (!dict_source.elements)
        return;

    auto config = getDictionaryConfigurationFromAST(data.create_query->as<ASTCreateQuery &>(), data.global_context);
    auto info = getInfoIfClickHouseDictionarySource(config, data.global_context);

    if (!info || !info->is_local)
        return;

    if (info->table_name.database.empty())
        info->table_name.database = data.default_database;
    data.dependencies.emplace(std::move(info->table_name));
}

void DDLDependencyVisitor::visit(const ASTStorage & storage, Data & data)
{
    if (!storage.engine)
        return;
    if (storage.engine->name != "Dictionary")
        return;

    extractTableNameFromArgument(*storage.engine, data, 0);
}


void DDLDependencyVisitor::extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx)
{
    /// Just ignore incorrect arguments, proper exception will be thrown later
    if (!function.arguments || function.arguments->children.size() <= arg_idx)
        return;

    QualifiedTableName qualified_name;

    const auto * expr_list = function.arguments->as<ASTExpressionList>();
    if (!expr_list)
        return;

    const auto * arg = expr_list->children[arg_idx].get();
    if (const auto * literal = arg->as<ASTLiteral>())
    {
        if (literal->value.getType() != Field::Types::String)
            return;

        auto maybe_qualified_name = QualifiedTableName::tryParseFromString(literal->value.get<String>());
        /// Just return if name if invalid
        if (!maybe_qualified_name)
            return;

        qualified_name = std::move(*maybe_qualified_name);
    }
    else if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(arg))
    {
        /// ASTIdentifier or ASTTableIdentifier
        auto table_identifier = identifier->createTable();
        /// Just return if table identified is invalid
        if (!table_identifier)
            return;

        qualified_name.database = table_identifier->getDatabaseName();
        qualified_name.table = table_identifier->shortName();
    }
    else
    {
        /// Just return because we don't validate AST in this function.
        return;
    }

    if (qualified_name.database.empty())
    {
        /// It can be table/dictionary from default database or XML dictionary, but we cannot distinguish it here.
        qualified_name.database = data.default_database;
    }
    data.dependencies.emplace(std::move(qualified_name));
}

}
