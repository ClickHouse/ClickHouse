#include <Interpreters/Access/RowPolicyAlterColumns.h>

#include <Access/AccessControl.h>
#include <Access/RowPolicy.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Core/Names.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/AlterCommands.h>

#include <algorithm>

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

/// every identifier of the filter, subqueries and SQL UDF bodies included
void collectIdentifiers(const ASTPtr & ast, NameSet & names, NameSet & visited_udfs)
{
    if (const auto * identifier = ast->as<ASTIdentifier>())
        names.insert(identifier->name());
    else if (const auto * function = ast->as<ASTFunction>())
        if (auto body = UserDefinedSQLFunctionFactory::instance().tryGet(function->name); body && visited_udfs.insert(function->name).second)
            collectIdentifiers(body, names, visited_udfs);

    for (const auto & child : ast->children)
        collectIdentifiers(child, names, visited_udfs);
}

/// tenant, t.tenant, db.t.tenant, tenant.sub and n.x for a nested n all mention the column
bool mentions(String name, const StorageID & table_id, const String & column)
{
    for (const auto & qualifier : {table_id.database_name + "." + table_id.table_name + ".", table_id.table_name + "."})
    {
        if (name.starts_with(qualifier))
        {
            name = name.substr(qualifier.size());
            break;
        }
    }
    return name == column || name.starts_with(column + ".");
}

}

void checkRowPoliciesBeforeAlter(const StorageID & table_id, const AlterCommands & commands, const ContextPtr & context)
{
    /// row policies are local to a server, so a replica must apply what its initiator already committed
    if (isSecondaryDDLReplay(context) || std::none_of(commands.begin(), commands.end(), removesColumn))
        return;

    const auto & access_control = context->getAccessControl();
    for (const auto & id : access_control.findAll<RowPolicy>())
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy || policy->getDatabase() != table_id.database_name
            || (!policy->isForDatabase() && policy->getTableName() != table_id.table_name))
            continue;

        NameSet names;
        NameSet visited_udfs;
        for (const auto & filter : policy->filters)
        {
            if (filter.empty())
                continue;
            ParserExpression parser;
            collectIdentifiers(parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS), names, visited_udfs);
        }

        for (const auto & command : commands)
        {
            if (!removesColumn(command))
                continue;
            if (std::ranges::any_of(names, [&](const auto & name) { return mentions(name, table_id, command.column_name); }))
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Cannot {} column {}: row policy {} mentions it. Change or drop the policy first",
                    command.type == AlterCommand::DROP_COLUMN ? "drop" : "rename", backQuote(command.column_name), policy->getFullName().toString());
        }
    }
}

}
