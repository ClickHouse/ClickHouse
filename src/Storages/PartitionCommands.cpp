#include <Storages/PartitionCommands.h>
#include <Storages/IStorage.h>
#include <Storages/DataDestinationType.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

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
    else if (command_ast->type == ASTAlterCommand::DROP_DETACHED_PARTITION)
    {
        PartitionCommand res;
        res.type = DROP_DETACHED_PARTITION;
        res.partition = command_ast->partition;
        res.part = command_ast->part;
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
    else if (command_ast->type == ASTAlterCommand::MOVE_PARTITION)
    {
        PartitionCommand res;
        res.type = MOVE_PARTITION;
        res.partition = command_ast->partition;
        res.part = command_ast->part;
        switch (command_ast->move_destination_type)
        {
            case DataDestinationType::DISK:
                res.move_destination_type = PartitionCommand::MoveDestinationType::DISK;
                break;
            case DataDestinationType::VOLUME:
                res.move_destination_type = PartitionCommand::MoveDestinationType::VOLUME;
                break;
            case DataDestinationType::TABLE:
                res.move_destination_type = PartitionCommand::MoveDestinationType::TABLE;
                res.to_database = command_ast->to_database;
                res.to_table = command_ast->to_table;
                break;
            default:
                break;
        }
        if (res.move_destination_type != PartitionCommand::MoveDestinationType::TABLE)
            res.move_destination_name = command_ast->move_destination_name;
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

}
