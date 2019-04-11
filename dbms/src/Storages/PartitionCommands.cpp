#include <Storages/PartitionCommands.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

std::optional<PartitionCommand> PartitionCommand::parse(const ASTAlterCommand * command_ast)
{
    if (command_ast->type == ASTAlterCommand::DROP_PARTITION)
    {
        PartitionCommand res;
        res.type = DROP_PARTITION;
        res.partition = command_ast->partition;
        res.detach = command_ast->detach;
        return res;
    }
    else if (command_ast->type == ASTAlterCommand::ATTACH_PARTITION)
    {
        PartitionCommand res;
        res.type = ATTACH_PARTITION;
        res.partition = command_ast->partition;
        res.part = command_ast->part;
        return res;
    }
    else if (command_ast->type == ASTAlterCommand::REPLACE_PARTITION)
    {
        PartitionCommand res;
        res.type = REPLACE_PARTITION;
        res.partition = command_ast->partition;
        res.replace = command_ast->replace;
        res.from_database = command_ast->from_database;
        res.from_table = command_ast->from_table;
        return res;
    }
    else if (command_ast->type == ASTAlterCommand::FETCH_PARTITION)
    {
        PartitionCommand res;
        res.type = FETCH_PARTITION;
        res.partition = command_ast->partition;
        res.from_zookeeper_path = command_ast->from;
        return res;
    }
    else if (command_ast->type == ASTAlterCommand::FREEZE_PARTITION)
    {
        PartitionCommand res;
        res.type = FREEZE_PARTITION;
        res.partition = command_ast->partition;
        res.with_name = command_ast->with_name;
        return res;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_COLUMN && command_ast->partition)
    {
        if (!command_ast->clear_column)
            throw Exception("Can't DROP COLUMN from partition. It is possible only to CLEAR COLUMN in partition", ErrorCodes::BAD_ARGUMENTS);

        PartitionCommand res;
        res.type = CLEAR_COLUMN;
        res.partition = command_ast->partition;
        res.column_name = *getIdentifierName(command_ast->column);
        return res;
    }
    else if (command_ast->type == ASTAlterCommand::FREEZE_ALL)
    {
        PartitionCommand command;
        command.type = PartitionCommand::FREEZE_ALL_PARTITIONS;
        command.with_name = command_ast->with_name;
        return command;
    }
    else
        return {};
}

void PartitionCommands::validate(const IStorage & table)
{
    for (const PartitionCommand & command : *this)
    {
        if (command.type == PartitionCommand::CLEAR_COLUMN)
        {
            String column_name = command.column_name.safeGet<String>();

            if (!table.getColumns().hasPhysical(column_name))
            {
                throw Exception("Wrong column name. Cannot find column " + column_name + " to clear it from partition",
                    DB::ErrorCodes::ILLEGAL_COLUMN);
            }
        }
    }
}

}
