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
    parseAlter(alter.parameters, alter_commands, partition_commands);

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

            case PartitionCommand::CLEAR_COLUMN:
                table->clearColumnInPartition(command.partition, command.column_name, context);
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
                command.data_type = data_type_factory.get(ast_col_decl.type);
            }
            if (ast_col_decl.default_expression)
            {
                command.default_kind = columnDefaultKindFromString(ast_col_decl.default_specifier);
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
                command.data_type = data_type_factory.get(ast_col_decl.type);
            }

            if (ast_col_decl.default_expression)
            {
                command.default_kind = columnDefaultKindFromString(ast_col_decl.default_specifier);
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

            if (!table->getColumns().hasPhysical(column_name))
            {
                throw Exception("Wrong column name. Cannot find column " + column_name + " to clear it from partition",
                    DB::ErrorCodes::ILLEGAL_COLUMN);
            }
        }
    }
}


}
