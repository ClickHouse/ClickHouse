#include <Databases/DDLLoadingDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Poco/String.h>


namespace DB
{

using TableLoadingDependenciesVisitor = DDLLoadingDependencyVisitor::Visitor;

TableNamesSet getLoadingDependenciesFromCreateQuery(ContextPtr global_context, const QualifiedTableName & table, const ASTPtr & ast)
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

void DDLLoadingDependencyVisitor::visit(const ASTPtr & ast, Data & data)
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

ssize_t DDLMatcherBase::getPositionOfTableNameArgumentToEvaluate(const ASTFunction & function)
{
    if (functionIsJoinGet(function.name) || functionIsDictGet(function.name))
        return 0;

    return -1;
}

ssize_t DDLMatcherBase::getPositionOfTableNameArgumentToVisit(const ASTFunction & function)
{
    ssize_t maybe_res = getPositionOfTableNameArgumentToEvaluate(function);
    if (0 <= maybe_res)
        return maybe_res;

    if (functionIsInOrGlobalInOperator(function.name))
    {
        if (function.children.empty())
            return -1;

        const auto * args = function.children[0]->as<ASTExpressionList>();
        if (!args || args->children.size() != 2)
            return -1;

        if (args->children[1]->as<ASTFunction>())
            return -1;

        return 1;
    }

    return -1;
}

void DDLLoadingDependencyVisitor::visit(const ASTFunction & function, Data & data)
{
    ssize_t table_name_arg_idx = getPositionOfTableNameArgumentToVisit(function);
    if (table_name_arg_idx < 0)
        return;
    extractTableNameFromArgument(function, data, table_name_arg_idx);
}

void DDLLoadingDependencyVisitor::visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data)
{
    if (dict_source.name != "clickhouse")
        return;
    if (!dict_source.elements)
        return;

    auto config = getDictionaryConfigurationFromAST(data.create_query->as<ASTCreateQuery &>(), data.global_context);
    auto info = getInfoIfClickHouseDictionarySource(config, data.global_context);

    if (!info || !info->is_local || info->table_name.table.empty())
        return;

    if (info->table_name.database.empty())
        info->table_name.database = data.default_database;
    data.dependencies.emplace(std::move(info->table_name));
}

void DDLLoadingDependencyVisitor::visit(const ASTStorage & storage, Data & data)
{
    if (!storage.engine)
        return;

    if (storage.engine->name == "Distributed")
        /// Checks that dict* expression was used as sharding_key and builds dependency between the dictionary and current table.
        /// Distributed(logs, default, hits[, sharding_key[, policy_name]])
        extractTableNameFromArgument(*storage.engine, data, 3);
    else if (storage.engine->name == "Dictionary")
        extractTableNameFromArgument(*storage.engine, data, 0);
}


void DDLLoadingDependencyVisitor::extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx)
{
    /// Just ignore incorrect arguments, proper exception will be thrown later
    if (!function.arguments || function.arguments->children.size() <= arg_idx)
        return;

    QualifiedTableName qualified_name;

    const auto * arg = function.arguments->as<ASTExpressionList>()->children[arg_idx].get();

    if (const auto * dict_function = arg->as<ASTFunction>())
    {
        if (!functionIsDictGet(dict_function->name))
            return;

        /// Get the dictionary name from `dict*` function.
        const auto * literal_arg = dict_function->arguments->as<ASTExpressionList>()->children[0].get();
        const auto * dictionary_name = literal_arg->as<ASTLiteral>();

        if (!dictionary_name)
            return;

        if (dictionary_name->value.getType() != Field::Types::String)
            return;

        auto maybe_qualified_name = QualifiedTableName::tryParseFromString(dictionary_name->value.get<String>());
        if (!maybe_qualified_name)
            return;

        qualified_name = std::move(*maybe_qualified_name);
    }
    else if (const auto * literal = arg->as<ASTLiteral>())
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
}
