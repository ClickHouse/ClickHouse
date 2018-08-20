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
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/DDLWorker.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTWeightedZooKeeperPath.h>

#include <Parsers/ParserCreateQuery.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <Storages/StorageLiveChannel.h>

#include <Poco/FileStream.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
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
        return executeDDLQueryOnCluster(query_ptr, context);

    const String & table_name = alter.table;
    String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;
    StoragePtr table = context.getTable(database_name, table_name);

    AlterCommands alter_commands;
    PartitionCommands partition_commands;
    ParameterCommands parameter_commands;
    ChannelCommands channel_commands;

    parseAlter(alter.parameters, alter_commands, partition_commands, parameter_commands, channel_commands);

    partition_commands.validate(table.get());
    for (const PartitionCommand & command : partition_commands)
    {
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
                table->dropPartition(query_ptr, command.partition, command.detach, context);
                break;

            case PartitionCommand::ATTACH_PARTITION:
                table->attachPartition(command.partition, command.part, context);
                break;

            case PartitionCommand::FETCH_PARTITION:
                table->fetchPartition(command.partition, command.from, context);
                break;

            case PartitionCommand::FREEZE_PARTITION:
                table->freezePartition(command.partition, command.with_name, context);
                break;

            case PartitionCommand::RESHARD_PARTITION:
                table->reshardPartitions(query_ptr, database_name, command.partition,
                    command.weighted_zookeeper_paths, command.sharding_key_expr, command.do_copy,
                    command.coordinator, context);
                break;

            case PartitionCommand::CLEAR_COLUMN:
                table->clearColumnInPartition(command.partition, command.column_name, context);
                break;
        }
    }

    parameter_commands.validate(table.get());
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

    channel_commands.validate(table.get());
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

    if (alter_commands.empty())
        return {};

    alter_commands.validate(table.get(), context);
    table->alter(alter_commands, database_name, table_name, context);

    return {};
}

void InterpreterAlterQuery::parseAlter(
    const ASTAlterQuery::ParameterContainer & params_container,
    AlterCommands & out_alter_commands, PartitionCommands & out_partition_commands,
    ParameterCommands & out_parameter_commands, ChannelCommands & out_channel_commands)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    for (const auto & params : params_container)
    {
        if (params.type == ASTAlterQuery::ADD_COLUMN)
        {
            AlterCommand command;
            command.type = AlterCommand::ADD_COLUMN;

            const auto & ast_col_decl = typeid_cast<const ASTColumnDeclaration &>(*params.col_decl);

            command.column_name = ast_col_decl.name;
            if (ast_col_decl.type)
            {
                StringRange type_range = ast_col_decl.type->range;
                String type_string(type_range.first, type_range.second - type_range.first);
                command.data_type = data_type_factory.get(type_string);
            }
            if (ast_col_decl.default_expression)
            {
                command.default_type = columnDefaultTypeFromString(ast_col_decl.default_specifier);
                command.default_expression = ast_col_decl.default_expression;
            }

            if (params.column)
                command.after_column = typeid_cast<const ASTIdentifier &>(*params.column).name;

            out_alter_commands.emplace_back(std::move(command));
        }
        else if (params.type == ASTAlterQuery::DROP_COLUMN)
        {
            if (params.partition)
            {
                if (!params.clear_column)
                    throw Exception("Can't DROP COLUMN from partition. It is possible only CLEAR COLUMN in partition", ErrorCodes::BAD_ARGUMENTS);

                const Field & column_name = typeid_cast<const ASTIdentifier &>(*(params.column)).name;

                out_partition_commands.emplace_back(PartitionCommand::clearColumn(params.partition, column_name));
            }
            else
            {
                if (params.clear_column)
                    throw Exception("\"ALTER TABLE table CLEAR COLUMN column\" queries are not supported yet. Use \"CLEAR COLUMN column IN PARTITION\".", ErrorCodes::NOT_IMPLEMENTED);

                AlterCommand command;
                command.type = AlterCommand::DROP_COLUMN;
                command.column_name = typeid_cast<const ASTIdentifier &>(*(params.column)).name;

                out_alter_commands.emplace_back(std::move(command));
            }
        }
        else if (params.type == ASTAlterQuery::MODIFY_COLUMN)
        {
            AlterCommand command;
            command.type = AlterCommand::MODIFY_COLUMN;

            const auto & ast_col_decl = typeid_cast<const ASTColumnDeclaration &>(*params.col_decl);

            command.column_name = ast_col_decl.name;
            if (ast_col_decl.type)
            {
                StringRange type_range = ast_col_decl.type->range;
                String type_string(type_range.first, type_range.second - type_range.first);
                command.data_type = data_type_factory.get(type_string);
            }

            if (ast_col_decl.default_expression)
            {
                command.default_type = columnDefaultTypeFromString(ast_col_decl.default_specifier);
                command.default_expression = ast_col_decl.default_expression;
            }

            out_alter_commands.emplace_back(std::move(command));
        }
        else if (params.type == ASTAlterQuery::MODIFY_PRIMARY_KEY)
        {
            AlterCommand command;
            command.type = AlterCommand::MODIFY_PRIMARY_KEY;
            command.primary_key = params.primary_key;
            out_alter_commands.emplace_back(std::move(command));
        }
        else if (params.type == ASTAlterQuery::ADD_TO_PARAMETER)
        {
            out_parameter_commands.emplace_back(ParameterCommand::addToParameter(params.parameter, params.values));
        }
        else if (params.type == ASTAlterQuery::DROP_FROM_PARAMETER)
        {
            out_parameter_commands.emplace_back(ParameterCommand::dropFromParameter(params.parameter, params.values));
        }
        else if (params.type == ASTAlterQuery::MODIFY_PARAMETER)
        {
            out_parameter_commands.emplace_back(ParameterCommand::modifyParameter(params.parameter, params.values));
        }
        else if (params.type == ASTAlterQuery::CHANNEL_ADD)
        {
            out_channel_commands.emplace_back(ChannelCommand::add(params.values));
        }
        else if (params.type == ASTAlterQuery::CHANNEL_DROP)
        {
            out_channel_commands.emplace_back(ChannelCommand::drop(params.values));
        }
        else if (params.type == ASTAlterQuery::CHANNEL_SUSPEND)
        {
            out_channel_commands.emplace_back(ChannelCommand::suspend(params.values));
        }
        else if (params.type == ASTAlterQuery::CHANNEL_RESUME)
        {
            out_channel_commands.emplace_back(ChannelCommand::resume(params.values));
        }
        else if (params.type == ASTAlterQuery::CHANNEL_REFRESH)
        {
            out_channel_commands.emplace_back(ChannelCommand::refresh(params.values));
        }
        else if (params.type == ASTAlterQuery::CHANNEL_MODIFY)
        {
            out_channel_commands.emplace_back(ChannelCommand::modify(params.values));
        }
        else if (params.type == ASTAlterQuery::DROP_PARTITION)
        {
            out_partition_commands.emplace_back(PartitionCommand::dropPartition(params.partition, params.detach));
        }
        else if (params.type == ASTAlterQuery::ATTACH_PARTITION)
        {
            out_partition_commands.emplace_back(PartitionCommand::attachPartition(params.partition, params.part));
        }
        else if (params.type == ASTAlterQuery::FETCH_PARTITION)
        {
            out_partition_commands.emplace_back(PartitionCommand::fetchPartition(params.partition, params.from));
        }
        else if (params.type == ASTAlterQuery::FREEZE_PARTITION)
        {
            out_partition_commands.emplace_back(PartitionCommand::freezePartition(params.partition, params.with_name));
        }
        else if (params.type == ASTAlterQuery::RESHARD_PARTITION)
        {
            WeightedZooKeeperPaths weighted_zookeeper_paths;

            const ASTs & ast_weighted_zookeeper_paths = typeid_cast<const ASTExpressionList &>(*params.weighted_zookeeper_paths).children;
            for (size_t i = 0; i < ast_weighted_zookeeper_paths.size(); ++i)
            {
                const auto & weighted_zookeeper_path = typeid_cast<const ASTWeightedZooKeeperPath &>(*ast_weighted_zookeeper_paths[i]);
                weighted_zookeeper_paths.emplace_back(weighted_zookeeper_path.path, weighted_zookeeper_path.weight);
            }

            Field coordinator;
            if (params.coordinator)
                coordinator = dynamic_cast<const ASTLiteral &>(*params.coordinator).value;

            out_partition_commands.emplace_back(PartitionCommand::reshardPartitions(
                params.partition, weighted_zookeeper_paths, params.sharding_key_expr,
                params.do_copy, coordinator));
        }
        else
            throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
    }
}

void InterpreterAlterQuery::PartitionCommands::validate(const IStorage * table)
{
    for (const PartitionCommand & command : *this)
    {
        if (command.type == PartitionCommand::CLEAR_COLUMN)
        {
            String column_name = command.column_name.safeGet<String>();

            if (!table->hasRealColumn(column_name))
            {
                throw Exception("Wrong column name. Cannot find column " + column_name + " to clear it from partition",
                    DB::ErrorCodes::ILLEGAL_COLUMN);
            }
        }
    }
}

void InterpreterAlterQuery::ParameterCommands::validate(const IStorage * table)
{
    //FIXME: add check
}

void InterpreterAlterQuery::ChannelCommands::validate(const IStorage * table)
{
    if ( !dynamic_cast<const StorageLiveChannel *>(table))
        throw Exception("Wrong storage type. Must be StorageLiveChannel", DB::ErrorCodes::UNKNOWN_STORAGE);
}

}
