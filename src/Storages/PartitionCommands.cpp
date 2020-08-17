#include <Storages/PartitionCommands.h>
#include <Storages/IStorage.h>
#include <Storages/DataDestinationType.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Chunk.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


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

std::string PartitionCommand::typeToString() const
{
    switch (type)
    {
    case PartitionCommand::Type::ATTACH_PARTITION:
        if (part)
            return "ATTACH PART";
        else
            return "ATTACH PARTITION";
    case PartitionCommand::Type::MOVE_PARTITION:
        return "MOVE PARTITION";
    case PartitionCommand::Type::DROP_PARTITION:
        if (detach)
            return "DETACH PARTITION";
        else
            return "DROP PARTITION";
    case PartitionCommand::Type::DROP_DETACHED_PARTITION:
        if (part)
            return "DROP DETACHED PART";
        else
            return "DROP DETACHED PARTITION";
    case PartitionCommand::Type::FETCH_PARTITION:
        return "FETCH PARTITION";
    case PartitionCommand::Type::FREEZE_ALL_PARTITIONS:
        return "FREEZE ALL";
    case PartitionCommand::Type::FREEZE_PARTITION:
        return "FREEZE PARTITION";
    case PartitionCommand::Type::REPLACE_PARTITION:
        return "REPLACE PARTITION";
    }
    __builtin_unreachable();
}

Pipe convertCommandsResultToSource(const PartitionCommandsResultInfo & commands_result)
{
    Block header {
         ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "command_type"),
         ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "partition_id"),
         ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "part_name"),
    };

    for (const auto & command_result : commands_result)
    {
        if (!command_result.old_part_name.empty() && !header.has("old_part_name"))
            header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "old_part_name"));

        if (!command_result.backup_name.empty() && !header.has("backup_name"))
            header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "backup_name"));

        if (!command_result.backup_path.empty() && !header.has("backup_path"))
            header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "backup_path"));
        if (!command_result.backup_path.empty() && !header.has("part_backup_path"))
            header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "part_backup_path"));
    }

    MutableColumns res_columns = header.cloneEmptyColumns();

    for (const auto & command_result : commands_result)
    {
        res_columns[0]->insert(command_result.command_type);
        res_columns[1]->insert(command_result.partition_id);
        res_columns[2]->insert(command_result.part_name);
        if (header.has("old_part_name"))
        {
            size_t pos = header.getPositionByName("old_part_name");
            res_columns[pos]->insert(command_result.old_part_name);
        }
        if (header.has("backup_name"))
        {
            size_t pos = header.getPositionByName("backup_name");
            res_columns[pos]->insert(command_result.backup_name);
        }
        if (header.has("backup_path"))
        {
            size_t pos = header.getPositionByName("backup_path");
            res_columns[pos]->insert(command_result.backup_path);
        }
        if (header.has("part_backup_path"))
        {
            size_t pos = header.getPositionByName("part_backup_path");
            res_columns[pos]->insert(command_result.part_backup_path);
        }
    }

    Chunk chunk(std::move(res_columns), commands_result.size());
    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}

}
