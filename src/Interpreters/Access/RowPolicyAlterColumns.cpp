#include <Interpreters/Access/RowPolicyAlterColumns.h>

#include <Access/AccessControl.h>
#include <Access/RowPolicy.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Interpreters/Context.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/expressionSourceColumns.h>

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

/// n.x is also a use of the nested column n
bool usesColumn(const Names & used_columns, const String & column)
{
    return std::ranges::any_of(used_columns, [&](const String & used) { return used == column || used.starts_with(column + "."); });
}

Names columnsReadByFilter(
    const String & filter, const String & policy_name, const StorageID & table_id, const ColumnsDescription & columns, const ContextPtr & context)
{
    try
    {
        ParserExpression parser;
        auto ast = parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        return expressionSourceColumns(ast, columns, context, /*keep_alias_columns=*/ true, table_id);
    }
    catch (Exception & e)
    {
        e.addMessage("while checking row policy {} before ALTER TABLE", policy_name);
        throw;
    }
}

}

void checkRowPoliciesBeforeAlter(
    const StorageID & table_id, const ColumnsDescription & columns, const AlterCommands & commands, const ContextPtr & context)
{
    if (std::none_of(commands.begin(), commands.end(), removesColumn))
        return;

    const auto & access_control = context->getAccessControl();
    for (const auto & id : access_control.findAll<RowPolicy>())
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy || policy->getDatabase() != table_id.getDatabaseName()
            || (!policy->isForDatabase() && policy->getTableName() != table_id.getTableName()))
            continue;

        const auto policy_name = policy->getFullName().toString();
        for (const auto & filter : policy->filters)
        {
            if (filter.empty())
                continue;

            const auto used_columns = columnsReadByFilter(filter, policy_name, table_id, columns, context);
            for (const auto & command : commands)
            {
                if (removesColumn(command) && usesColumn(used_columns, command.column_name))
                    throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Cannot {} column {}: it is used by row policy {}. Change or drop the policy first",
                        command.type == AlterCommand::DROP_COLUMN ? "drop" : "rename", backQuote(command.column_name), policy_name);
            }
        }
    }
}

}
