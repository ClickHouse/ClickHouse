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

#include <Poco/FileStream.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ARGUMENT_OUT_OF_BOUND;
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
    parseAlter(alter.parameters, alter_commands, partition_commands);

    for (const PartitionCommand & command : partition_commands)
    {
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
                table->dropPartition(query_ptr, command.partition, command.detach, context.getSettingsRef());
                break;

            case PartitionCommand::ATTACH_PARTITION:
                table->attachPartition(query_ptr, command.partition, command.part, context.getSettingsRef());
                break;

            case PartitionCommand::FETCH_PARTITION:
                table->fetchPartition(command.partition, command.from, context.getSettingsRef());
                break;

            case PartitionCommand::FREEZE_PARTITION:
                table->freezePartition(command.partition, command.with_name, context.getSettingsRef());
                break;

            case PartitionCommand::RESHARD_PARTITION:
                table->reshardPartitions(query_ptr, database_name, command.partition, command.last_partition,
                    command.weighted_zookeeper_paths, command.sharding_key_expr, command.do_copy,
                    command.coordinator, context);
                break;

            case PartitionCommand::DROP_COLUMN:
                table->dropColumnFromPartition(query_ptr, command.partition, command.column_name, context.getSettingsRef());
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
    AlterCommands & out_alter_commands, PartitionCommands & out_partition_commands)
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
                const Field & partition = typeid_cast<const ASTLiteral &>(*(params.partition)).value;
                const Field & column_name = typeid_cast<const ASTIdentifier &>(*(params.column)).name;

                out_partition_commands.emplace_back(PartitionCommand::dropColumnFromPartition(partition, column_name));
            }
            else
            {
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
        else if (params.type == ASTAlterQuery::DROP_PARTITION)
        {
            const Field & partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;
            out_partition_commands.emplace_back(PartitionCommand::dropPartition(partition, params.detach));
        }
        else if (params.type == ASTAlterQuery::ATTACH_PARTITION)
        {
            const Field & partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;
            out_partition_commands.emplace_back(PartitionCommand::attachPartition(partition, params.part));
        }
        else if (params.type == ASTAlterQuery::FETCH_PARTITION)
        {
            const Field & partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;
            out_partition_commands.emplace_back(PartitionCommand::fetchPartition(partition, params.from));
        }
        else if (params.type == ASTAlterQuery::FREEZE_PARTITION)
        {
            const Field & partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;
            out_partition_commands.emplace_back(PartitionCommand::freezePartition(partition, params.with_name));
        }
        else if (params.type == ASTAlterQuery::RESHARD_PARTITION)
        {
            Field first_partition;
            if (params.partition)
                first_partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;

            Field last_partition;
            if (params.last_partition)
                last_partition = dynamic_cast<const ASTLiteral &>(*params.last_partition).value;
            else
                last_partition = first_partition;

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
                first_partition, last_partition, weighted_zookeeper_paths, params.sharding_key_expr,
                params.do_copy, coordinator));
        }
        else
            throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
    }
}

}
