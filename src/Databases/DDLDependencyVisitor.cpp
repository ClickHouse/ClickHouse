#include <Databases/DDLDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Poco/String.h>

namespace DB
{

using TableLoadingDependenciesVisitor = DDLDependencyVisitor::Visitor;

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
    if (const auto * function = ast->as<ASTFunction>())
        visit(*function, data);
    else if (const auto * dict_source = ast->as<ASTFunctionWithKeyValueArguments>())
        visit(*dict_source, data);
    else if (const auto * storage = ast->as<ASTStorage>())
        visit(*storage, data);
}

bool DDLMatcherBase::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    if (node->as<ASTStorage>())
        return false;

    if (auto * create = node->as<ASTCreateQuery>())
    {
        if (child.get() == create->select)
            return false;
    }

    return true;
}

ssize_t DDLMatcherBase::getPositionOfTableNameArgument(const ASTFunction & function)
{
    if (function.name == "joinGet" ||
        function.name == "dictHas" ||
        function.name == "dictIsIn" ||
        function.name.starts_with("dictGet"))
        return 0;

    if (Poco::toLower(function.name) == "in")
        return 1;

    return -1;
}

void DDLDependencyVisitor::visit(const ASTFunction & function, Data & data)
{
    ssize_t table_name_arg_idx = getPositionOfTableNameArgument(function);
    if (table_name_arg_idx < 0)
        return;
    extractTableNameFromArgument(function, data, table_name_arg_idx);
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

    const auto * arg = function.arguments->as<ASTExpressionList>()->children[arg_idx].get();
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
        assert(false);
        return;
    }

    if (qualified_name.database.empty())
    {
        /// It can be table/dictionary from default database or XML dictionary, but we cannot distinguish it here.
        qualified_name.database = data.default_database;
    }
    data.dependencies.emplace(std::move(qualified_name));
}


void NormalizeAndEvaluateConstants::visit(const ASTPtr & ast, Data & data)
{
    assert(data.create_query_context->hasQueryContext());

    /// Looking for functions in column default expressions and dictionary source definition
    if (const auto * function = ast->as<ASTFunction>())
        visit(*function, data);
    else if (const auto * dict_source = ast->as<ASTFunctionWithKeyValueArguments>())
        visit(*dict_source, data);
}

void NormalizeAndEvaluateConstants::visit(const ASTFunction & function, Data & data)
{
    /// Replace expressions like "dictGet(currentDatabase() || '.dict', 'value', toUInt32(1))"
    /// with "dictGet('db_name.dict', 'value', toUInt32(1))"
    ssize_t table_name_arg_idx = getPositionOfTableNameArgument(function);
    if (table_name_arg_idx < 0)
        return;

    if (!function.arguments || function.arguments->children.size() <= static_cast<size_t>(table_name_arg_idx))
        return;

    auto & arg = function.arguments->as<ASTExpressionList &>().children[table_name_arg_idx];
    if (arg->as<ASTFunction>())
        arg = evaluateConstantExpressionAsLiteral(arg, data.create_query_context);
}


void NormalizeAndEvaluateConstants::visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data)
{
    if (!dict_source.elements)
        return;

    auto & expr_list = dict_source.elements->as<ASTExpressionList &>();
    for (auto & child : expr_list.children)
    {
        ASTPair * pair = child->as<ASTPair>();
        if (pair->second->as<ASTFunction>())
        {
            auto ast_literal = evaluateConstantExpressionAsLiteral(pair->children[0], data.create_query_context);
            pair->replace(pair->second, ast_literal);
        }
    }
}

}
