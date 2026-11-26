#include <Interpreters/Access/RowPolicyAlterColumns.h>

#include <Access/AccessControl.h>
#include <Access/RowPolicy.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Core/Names.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageProxy.h>

#include <algorithm>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}

namespace
{

/// if exists on a missing column is a no-op (ignore), clear column keeps the column
bool removesColumn(const AlterCommand & command)
{
    if (command.ignore)
        return false;
    return (command.type == AlterCommand::DROP_COLUMN && !command.clear) || command.type == AlterCommand::RENAME_COLUMN;
}

/// the identifiers a filter reads, SQL UDF bodies included, lambda parameters and the subqueries over other tables excluded
struct FilterNames
{
    const std::vector<StorageID> & tables;
    const ColumnsDescription & columns;
    NameSet names;
    NameSet visited_udfs;

    /// an unqualified table binds in the database of the reader, so it counts in every database
    bool readsCheckedTable(const ASTPtr & ast) const
    {
        if (const auto * table = ast->as<ASTTableIdentifier>())
        {
            auto id = table->getTableId();
            return std::ranges::any_of(tables, [&](const auto & checked)
            {
                return checked.table_name == id.table_name && (id.database_name.empty() || checked.database_name == id.database_name);
            });
        }
        return std::ranges::any_of(ast->children, [&](const auto & child) { return readsCheckedTable(child); });
    }

    void collect(const ASTPtr & ast, NameSet bound = {})
    {
        /// a filter cannot be correlated and cannot read its own table (see `buildFilterInfo`), so the names
        /// of a subquery belong to other tables unless the subquery reads one of the checked tables
        if (ast->as<ASTSubquery>() && !readsCheckedTable(ast))
            return;

        String name;
        if (const auto * identifier = ast->as<ASTIdentifier>())
        {
            name = identifier->name();
            if (std::ranges::any_of(bound, [&](const auto & parameter) { return name == parameter || name.starts_with(parameter + "."); }))
                return;
            names.insert(name);
            /// a column takes priority over a SQL UDF of the same name, which a bare name calls only as a lambda, `arrayExists(udf, arr)`
            if (columns.has(name))
                return;
        }
        else if (const auto * function = ast->as<ASTFunction>())
        {
            name = function->name;
            /// x -> body is lambda(tuple(x), body), x is local to the body
            if (name == "lambda")
            {
                for (const auto & parameter : RequiredSourceColumnsMatcher::extractNamesFromLambda(*function))
                    bound.insert(parameter);
                collect(function->arguments->children[1], bound);
                return;
            }
        }
        if (auto body = UserDefinedSQLFunctionFactory::instance().tryGet(name); body && visited_udfs.insert(name).second)
            collect(body);

        for (const auto & child : ast->children)
            collect(child, bound);
    }
};

/// the storage column a name reads is its longest dotted prefix that is a column: j for j.user.name, n.x for a nested child n.x
std::optional<String> storageColumn(String name, const ColumnsDescription & columns)
{
    while (!columns.has(name))
    {
        auto dot = name.rfind('.');
        if (dot == String::npos)
            return {};
        name.resize(dot);
    }
    return name;
}

/// a name binds as written first, an exact column or a column with a subcolumn path, and with the `table.` or `db.table.`
/// qualifier removed only after that; an alter of a column removes the column itself, or every child of a nested one
/// when no column has that name (see `getNameRange`)
bool mentions(const String & name, const StorageID & table_id, const String & column, const ColumnsDescription & columns)
{
    std::vector<std::string_view> candidates{name};
    for (const auto & qualifier : {table_id.table_name + ".", table_id.database_name + "." + table_id.table_name + "."})
        if (name.starts_with(qualifier))
            candidates.push_back(std::string_view(name).substr(qualifier.size()));
    for (const auto & candidate : candidates)
        if (auto storage_column = storageColumn(String(candidate), columns))
            return columns.has(column) ? *storage_column == column : storage_column->starts_with(column + ".");
    return false;
}

/// a read through an `Alias` also applies the policies of its target chain (see `getRowPolicyFilterForStorage`),
/// so the policies of the table, of its chain, and of every alias that reaches them read the same columns
std::vector<StorageID> tablesSharingColumns(const StoragePtr & table, const ContextPtr & context)
{
    std::vector<StorageID> pending{table->getStorageID()};
    if (const auto * alias = resolveStorageProxyLoading(table)->as<StorageAlias>())
        for (const auto & [database, name] : alias->getTargetChain())
            pending.emplace_back(database, name);

    std::vector<StorageID> tables;
    auto & catalog = DatabaseCatalog::instance();
    while (!pending.empty())
    {
        auto id = std::move(pending.back());
        pending.pop_back();
        if (std::ranges::any_of(tables, [&](const auto & seen) { return seen.database_name == id.database_name && seen.table_name == id.table_name; }))
            continue;
        for (const auto & dependent : catalog.getReferentialDependents(id))
            if (auto storage = catalog.tryGetTable(dependent, context); storage && resolveStorageProxyLoading(storage)->as<StorageAlias>())
                pending.push_back(dependent);
        tables.push_back(std::move(id));
    }
    return tables;
}

}

void checkRowPoliciesBeforeAlter(const StoragePtr & table, const ColumnsDescription & columns, const AlterCommands & commands, const ContextPtr & context)
{
    /// row policies are local to a server, so a replica must apply what its initiator already committed
    if (isSecondaryDDLReplay(context) || std::none_of(commands.begin(), commands.end(), removesColumn))
        return;

    const auto tables = tablesSharingColumns(table, context);
    const auto & access_control = context->getAccessControl();
    for (const auto & id : access_control.findAll<RowPolicy>())
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy)
            continue;
        /// the qualifier of a name is the table the policy is on, for `ON db.*` each table of db
        std::vector<StorageID> policy_tables;
        for (const auto & candidate : tables)
            if (candidate.database_name == policy->getDatabase() && (policy->isForDatabase() || candidate.table_name == policy->getTableName()))
                policy_tables.push_back(candidate);
        if (policy_tables.empty())
            continue;

        FilterNames filter_names{tables, columns, {}, {}};
        for (const auto & filter : policy->filters)
        {
            if (filter.empty())
                continue;
            ParserExpression parser;
            filter_names.collect(parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS));
        }

        for (const auto & command : commands)
        {
            if (!removesColumn(command))
                continue;
            if (std::ranges::any_of(filter_names.names, [&](const auto & name)
                {
                    return std::ranges::any_of(policy_tables, [&](const auto & table_id) { return mentions(name, table_id, command.column_name, columns); });
                }))
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Cannot {} column {}: row policy {} mentions it. Change or drop the policy first",
                    command.type == AlterCommand::DROP_COLUMN ? "drop" : "rename", backQuote(command.column_name), policy->getFullName().toString());
        }
    }
}

}
