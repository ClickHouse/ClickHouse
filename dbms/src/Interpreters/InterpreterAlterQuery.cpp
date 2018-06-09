#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/DDLWorker.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

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
    extern const int BAD_ARGUMENTS;
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
    parseAlter(alter.command_list->commands, alter_commands, partition_commands, mutation_commands);

    if (!mutation_commands.commands.empty())
    {
        mutation_commands.validate(*table, context);
        table->mutate(mutation_commands, context);
    }

    partition_commands.validate(*table);
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

            case PartitionCommand::REPLACE_PARTITION:
                {
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

void InterpreterAlterQuery::parseAlter(
    const std::vector<ASTAlterCommand *> & command_asts,
    AlterCommands & out_alter_commands,
    PartitionCommands & out_partition_commands,
    MutationCommands & out_mutation_commands)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    for (const auto & command_ast : command_asts)
    {
        if (command_ast->type == ASTAlterCommand::ADD_COLUMN)
        {
            AlterCommand command;
            command.type = AlterCommand::ADD_COLUMN;

            const auto & ast_col_decl = typeid_cast<const ASTColumnDeclaration &>(*command_ast->col_decl);

            command.column_name = ast_col_decl.name;
            if (ast_col_decl.type)
            {
                command.data_type = data_type_factory.get(ast_col_decl.type);
            }
            if (ast_col_decl.default_expression)
            {
                command.default_kind = columnDefaultKindFromString(ast_col_decl.default_specifier);
                command.default_expression = ast_col_decl.default_expression;
            }

            if (command_ast->column)
                command.after_column = typeid_cast<const ASTIdentifier &>(*command_ast->column).name;

            out_alter_commands.emplace_back(std::move(command));
        }
        else if (command_ast->type == ASTAlterCommand::DROP_COLUMN)
        {
            if (command_ast->partition)
            {
                if (!command_ast->clear_column)
                    throw Exception("Can't DROP COLUMN from partition. It is possible only CLEAR COLUMN in partition", ErrorCodes::BAD_ARGUMENTS);

                const Field & column_name = typeid_cast<const ASTIdentifier &>(*(command_ast->column)).name;

                out_partition_commands.emplace_back(PartitionCommand::clearColumn(command_ast->partition, column_name));
            }
            else
            {
                if (command_ast->clear_column)
                    throw Exception("\"ALTER TABLE table CLEAR COLUMN column\" queries are not supported yet. Use \"CLEAR COLUMN column IN PARTITION\".", ErrorCodes::NOT_IMPLEMENTED);

                AlterCommand command;
                command.type = AlterCommand::DROP_COLUMN;
                command.column_name = typeid_cast<const ASTIdentifier &>(*(command_ast->column)).name;

                out_alter_commands.emplace_back(std::move(command));
            }
        }
        else if (command_ast->type == ASTAlterCommand::MODIFY_COLUMN)
        {
            AlterCommand command;
            command.type = AlterCommand::MODIFY_COLUMN;

            const auto & ast_col_decl = typeid_cast<const ASTColumnDeclaration &>(*command_ast->col_decl);

            command.column_name = ast_col_decl.name;
            if (ast_col_decl.type)
            {
                command.data_type = data_type_factory.get(ast_col_decl.type);
            }

            if (ast_col_decl.default_expression)
            {
                command.default_kind = columnDefaultKindFromString(ast_col_decl.default_specifier);
                command.default_expression = ast_col_decl.default_expression;
            }

            out_alter_commands.emplace_back(std::move(command));
        }
        else if (command_ast->type == ASTAlterCommand::MODIFY_PRIMARY_KEY)
        {
            AlterCommand command;
            command.type = AlterCommand::MODIFY_PRIMARY_KEY;
            command.primary_key = command_ast->primary_key;
            out_alter_commands.emplace_back(std::move(command));
        }
        else if (command_ast->type == ASTAlterCommand::DROP_PARTITION)
        {
            out_partition_commands.emplace_back(PartitionCommand::dropPartition(command_ast->partition, command_ast->detach));
        }
        else if (command_ast->type == ASTAlterCommand::ATTACH_PARTITION)
        {
            out_partition_commands.emplace_back(PartitionCommand::attachPartition(command_ast->partition, command_ast->part));
        }
        else if (command_ast->type == ASTAlterCommand::REPLACE_PARTITION)
        {
            out_partition_commands.emplace_back(
                PartitionCommand::replacePartition(command_ast->partition, command_ast->replace, command_ast->from_database, command_ast->from_table));
        }
        else if (command_ast->type == ASTAlterCommand::FETCH_PARTITION)
        {
            out_partition_commands.emplace_back(PartitionCommand::fetchPartition(command_ast->partition, command_ast->from));
        }
        else if (command_ast->type == ASTAlterCommand::FREEZE_PARTITION)
        {
            out_partition_commands.emplace_back(PartitionCommand::freezePartition(command_ast->partition, command_ast->with_name));
        }
        else if (command_ast->type == ASTAlterCommand::DELETE)
        {
            out_mutation_commands.commands.emplace_back(MutationCommand::delete_(command_ast->predicate));
        }
        else
            throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
    }
}


void InterpreterAlterQuery::PartitionCommands::validate(const IStorage & table)
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
