#include <Parsers/ASTAlterQuery.h>

#include <Storages/ReplicaCommands.h>
#include <Storages/StorageReplicatedMergeTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
}

std::optional<ReplicaCommand> ReplicaCommand::parse(const ASTAlterCommand * command_ast)
{
    if (command_ast->type == ASTAlterCommand::DROP_REPLICA)
    {
        ReplicaCommand res;
        res.type = DROP_REPLICA;
        res.replica = command_ast->replica;
        res.replica_name = command_ast->replica_name;
        return res;
    }
    else
        return {};
}

void ReplicaCommands::validate(const IStorage & table)
{
    for (const ReplicaCommand & command : *this)
    {
        if (command.type == ReplicaCommand::DROP_REPLICA)
        {
            if (!empty() && !dynamic_cast<const StorageReplicatedMergeTree *>(&table))
                throw Exception("Wrong storage type. Must be StorageReplicateMergeTree", DB::ErrorCodes::UNKNOWN_STORAGE);
        }
    }
}

}
