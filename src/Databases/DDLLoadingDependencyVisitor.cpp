#include <Poco/Util/AbstractConfiguration.h>
#include <Databases/DDLLoadingDependencyVisitor.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include "config.h"
#if USE_LIBPQXX
#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#endif
#include <Interpreters/Context.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Poco/String.h>


namespace DB
{

using TableLoadingDependenciesVisitor = DDLLoadingDependencyVisitor::Visitor;

TableNamesSet getLoadingDependenciesFromCreateQuery(ContextPtr global_context, const QualifiedTableName & table, const ASTPtr & ast, const String & current_database, bool can_throw)
{
    chassert(global_context == global_context->getGlobalContext());
    TableLoadingDependenciesVisitor::Data data;
    data.default_database = global_context->getCurrentDatabase();
    data.current_database = current_database;
    data.create_query = ast;
    data.global_context = global_context;
    data.table_name = table;
    data.can_throw = can_throw;
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
    else if (const auto * constraint = ast->as<ASTConstraintDeclaration>())
        visit(*constraint, data);
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

    if (!info || !info->is_local)
        return;

    if (!info->table_name.table.empty())
    {
        /// If database is not specified in dictionary source, use database of the dictionary itself, not the current/default database.
        if (info->table_name.database.empty())
            info->table_name.database = data.table_name.database;
        data.dependencies.emplace(std::move(info->table_name));
    }
    else
    {
        /// We don't have a table name, we have a select query instead that will be executed during dictionary loading.
        /// We need to find all tables used in this select query and add them to dependencies.
        auto select_query_dependencies = getDependenciesFromDictionaryNestedSelectQuery(data.global_context, data.table_name, data.create_query, info->query, data.default_database, data.can_throw);
        data.dependencies.merge(select_query_dependencies);
    }
}

void DDLLoadingDependencyVisitor::visit(const ASTConstraintDeclaration & constraint, Data & data)
{
    if (!constraint.expr)
        return;

    /// Attaching a table analyzes its constraints (`InterpreterCreateQuery::getConstraintsDescription`),
    /// and the analysis executes the scalar subqueries of a constraint expression, so the tables which
    /// such a subquery reads have to be loaded before this table.
    addDependenciesOfExecutedSubqueries(constraint.expr->ptr(), data);
}

void DDLLoadingDependencyVisitor::addDependenciesOfExecutedSubqueries(const ASTPtr & ast, Data & data)
{
    if (ast->as<ASTSubquery>())
    {
        /// The subquery is executed as a whole, so everything it reads is a dependency.
        auto subquery_dependencies = getDependenciesFromCreateQuery(data.global_context, data.table_name, ast, data.current_database);
        data.dependencies.merge(subquery_dependencies.dependencies);
        return;
    }

    /// A subquery in the right argument of `IN` and the argument of `exists` is not executed during
    /// the analysis, see `ExecuteScalarSubqueriesMatcher::visit`. What it reads is needed only when
    /// the constraint is checked, so the table attaches fine without it.
    const auto * function = ast->as<ASTFunction>();
    std::optional<size_t> not_executed_argument;
    if (function)
    {
        if (functionIsInOrGlobalInOperator(function->name))
            not_executed_argument = 1;
        else if (function->name == "exists")
            not_executed_argument = 0;
    }

    for (const auto & child : ast->children)
    {
        if (function && child == function->arguments)
        {
            const auto & arguments = child->children;
            for (size_t i = 0; i < arguments.size(); ++i)
                if (not_executed_argument != i || !arguments[i]->as<ASTSubquery>())
                    addDependenciesOfExecutedSubqueries(arguments[i], data);
        }
        else
            addDependenciesOfExecutedSubqueries(child, data);
    }
}

void DDLLoadingDependencyVisitor::visit(const ASTStorage & storage, Data & data)
{
    if (storage.ttl_table)
    {
        auto ttl_dependencies = getDependenciesFromCreateQuery(data.global_context, data.table_name, storage.ttl_table->ptr(), data.default_database);
        data.dependencies.merge(ttl_dependencies.dependencies);
    }

    if (!storage.engine)
        return;

    if (storage.engine->name == "Distributed")
        /// Checks that dict* expression was used as sharding_key and builds dependency between the dictionary and current table.
        /// Distributed(logs, default, hits[, sharding_key[, policy_name]])
        extractTableNameFromArgument(*storage.engine, data, 3);
    else if (storage.engine->name == "Dictionary")
        extractTableNameFromArgument(*storage.engine, data, 0);
#if USE_LIBPQXX
    else if (storage.engine->name == "MaterializedPostgreSQL")
    {
        const auto * create_query = data.create_query->as<ASTCreateQuery>();
        auto nested_table = toString(create_query->uuid) + StorageMaterializedPostgreSQL::NESTED_TABLE_SUFFIX;
        data.dependencies.emplace(QualifiedTableName{ .database = create_query->getDatabase(), .table = nested_table });
    }
#endif
}


void DDLLoadingDependencyVisitor::extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx)
{
    /// Just ignore incorrect arguments, proper exception will be thrown later
    if (!function.arguments || function.arguments->children.size() <= arg_idx)
        return;

    QualifiedTableName qualified_name;

    const auto * arg = function.arguments->as<ASTExpressionList>()->children[arg_idx].get();

    if (const auto * function_arg = arg->as<ASTFunction>())
    {
        if (!functionIsJoinGet(function_arg->name) && !functionIsDictGet(function_arg->name))
            return;

        /// Get the dictionary name from `dict*` function or the table name from 'joinGet' function.
        const auto * literal_arg = function_arg->arguments->as<ASTExpressionList>()->children[0].get();
        const auto * name = literal_arg->as<ASTLiteral>();

        if (!name)
            return;

        if (name->value.getType() != Field::Types::String)
            return;

        auto maybe_qualified_name = QualifiedTableName::tryParseFromString(name->value.safeGet<String>());
        if (!maybe_qualified_name)
            return;

        qualified_name = std::move(*maybe_qualified_name);
    }
    else if (const auto * literal = arg->as<ASTLiteral>())
    {
        if (literal->value.getType() != Field::Types::String)
            return;

        auto maybe_qualified_name = QualifiedTableName::tryParseFromString(literal->value.safeGet<String>());
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
    else if (arg->as<ASTSubquery>())
    {
        /// Allow IN subquery.
        /// Do not add tables from the subquery into dependencies,
        /// because CREATE will succeed anyway.
        return;
    }
    else
    {
        /// Just return if the argument has unexpected type.
        return;
    }

    if (qualified_name.database.empty())
    {
        /// It can be table/dictionary from the database against which the unqualified names of this
        /// CREATE query resolve, or an XML dictionary, but we cannot distinguish it here. When the
        /// definition is read back from metadata written before the names were qualified at CREATE
        /// time, that database is the one owning the table, which is what
        /// `qualifyNamesFromLegacyMetadata` resolves the very same name to when the definition is
        /// attached — the graph must not disagree with the attached storage. Note that this is not
        /// the default database of the server: that one is only for a nested query executed with
        /// the global context rather than with the context of the CREATE query.
        qualified_name.database = data.current_database;
    }
    data.dependencies.emplace(std::move(qualified_name));
}
}
