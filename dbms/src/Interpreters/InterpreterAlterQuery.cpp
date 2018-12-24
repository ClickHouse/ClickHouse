/* Some modifications Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Parsers/ASTAlterQuery.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageLiveChannel.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int UNKNOWN_STORAGE;
}


InterpreterAlterQuery::InterpreterAlterQuery(const ASTPtr & query_ptr_, const Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}

BlockIO InterpreterAlterQuery::execute()
{
    auto & alter = typeid_cast<ASTAlterQuery &>(*query_ptr);

    if (!alter.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, {alter.database});

    const String & table_name = alter.table;
    String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;
    StoragePtr table = context.getTable(database_name, table_name);

    /// Add default database to table identifiers that we can encounter in e.g. default expressions,
    /// mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(database_name);
    ASTPtr command_list_ptr = alter.command_list->ptr();
    visitor.visit(command_list_ptr);

    AlterCommands alter_commands;
    PartitionCommands partition_commands;
    MutationCommands mutation_commands;
    ParameterCommands parameter_commands;
    ChannelCommands channel_commands;
    for (ASTAlterCommand * command_ast : alter.command_list->commands)
    {
        if (auto alter_command = AlterCommand::parse(command_ast))
            alter_commands.emplace_back(std::move(*alter_command));
        else if (auto partition_command = PartitionCommand::parse(command_ast))
            partition_commands.emplace_back(std::move(*partition_command));
        else if (auto mut_command = MutationCommand::parse(command_ast))
            mutation_commands.emplace_back(std::move(*mut_command));
        else if (auto param_command = ParameterCommand::parse(command_ast))
            parameter_commands.emplace_back(std::move(*param_command));
        else if (auto channel_command = ChannelCommand::parse(command_ast))
            channel_commands.emplace_back(std::move(*channel_command));
        else
            throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
    }

    if (!mutation_commands.empty())
    {
        MutationsInterpreter(table, mutation_commands, context).validate();
        table->mutate(mutation_commands, context);
    }

    if (!partition_commands.empty())
    {
        partition_commands.validate(*table);
        table->alterPartition(query_ptr, partition_commands, context);
    }

    parameter_commands.validate(*table);
    for (const ParameterCommand & command : parameter_commands)
    {
        switch (command.type)
        {
            case ParameterCommand::ADD_TO_PARAMETER:
                table->addToParameter(command.parameter, command.values, context);
                break;

            case ParameterCommand::DROP_FROM_PARAMETER:
                table->dropFromParameter(command.parameter, command.values, context);
                break;

            case ParameterCommand::MODIFY_PARAMETER:
                table->modifyParameter(command.parameter, command.values, context);
                break;
        }
    }

    channel_commands.validate(*table);
    for (const ChannelCommand & command : channel_commands)
    {
        auto channel = std::dynamic_pointer_cast<StorageLiveChannel>(table);
        switch (command.type)
        {
            case ChannelCommand::ADD:
                channel->addToChannel(command.values, context);
                break;

            case ChannelCommand::DROP:
                channel->dropFromChannel(command.values, context);
                break;

            case ChannelCommand::SUSPEND:
                channel->suspendInChannel(command.values, context);
                break;

            case ChannelCommand::RESUME:
                channel->resumeInChannel(command.values, context);
                break;

            case ChannelCommand::REFRESH:
                channel->refreshInChannel(command.values, context);
                break;

            case ChannelCommand::MODIFY:
                channel->modifyChannel(command.values, context);
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

void ParameterCommands::validate(const IStorage & /*table*/)
{
    //FIXME: add check
}

void ChannelCommands::validate(const IStorage & table)
{
    if ( !dynamic_cast<const StorageLiveChannel *>(&table))
        throw Exception("Wrong storage type. Must be StorageLiveChannel", DB::ErrorCodes::UNKNOWN_STORAGE);
}

}
