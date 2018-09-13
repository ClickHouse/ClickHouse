#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Parsers/ASTAlterQuery.h>
#include <Common/typeid_cast.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}


InterpreterAlterQuery::InterpreterAlterQuery(const ASTPtr & query_ptr_, const Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}

BlockIO InterpreterAlterQuery::execute()
{
    auto & alter = typeid_cast<ASTAlterQuery &>(*query_ptr);

    if (!alter.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, {alter.table});

    const String & table_name = alter.table;
    String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;
    StoragePtr table = context.getTable(database_name, table_name);

    AlterCommands alter_commands;
    PartitionCommands partition_commands;
    MutationCommands mutation_commands;
    for (ASTAlterCommand * command_ast : alter.command_list->commands)
    {
        if (auto alter_command = AlterCommand::parse(command_ast))
            alter_commands.emplace_back(std::move(*alter_command));
        else if (auto partition_command = PartitionCommand::parse(command_ast))
            partition_commands.emplace_back(std::move(*partition_command));
        else if (auto mut_command = MutationCommand::parse(command_ast))
            mutation_commands.emplace_back(std::move(*mut_command));
        else
            throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
    }

    if (!mutation_commands.empty())
    {
        MutationsInterpreter(table, mutation_commands, context).validate();
        table->mutate(mutation_commands, context);
    }

    partition_commands.validate(*table);
    for (const PartitionCommand & command : partition_commands)
    {
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
                table->checkPartitionCanBeDropped(command.partition);
                table->dropPartition(query_ptr, command.partition, command.detach, context);
                break;

            case PartitionCommand::ATTACH_PARTITION:
                table->attachPartition(command.partition, command.part, context);
                break;

            case PartitionCommand::REPLACE_PARTITION:
                {
                    table->checkPartitionCanBeDropped(command.partition);
                    String from_database = command.from_database.empty() ? context.getCurrentDatabase() : command.from_database;
                    auto from_storage = context.getTable(from_database, command.from_table);
                    table->replacePartitionFrom(from_storage, command.partition, command.replace, context);
                }
                break;

            case PartitionCommand::FETCH_PARTITION:
                table->fetchPartition(command.partition, command.from_zookeeper_path, context);
                break;

            case PartitionCommand::FREEZE_PARTITION:
                table->freezePartition(command.partition, command.with_name, context);
                break;

            case PartitionCommand::CLEAR_COLUMN:
                table->clearColumnInPartition(command.partition, command.column_name, context);
                break;
        }
    }

    if (!alter_commands.empty())
    {
        alter_commands.validate(*table, context);
        table->alter(alter_commands, database_name, table_name, context);
    }

    return {};
}

}
