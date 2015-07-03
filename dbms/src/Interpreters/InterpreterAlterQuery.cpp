#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Parsers/ASTAlterQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/IO/copyData.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/parseQuery.h>

#include <Poco/FileStream.h>

#include <algorithm>


using namespace DB;

InterpreterAlterQuery::InterpreterAlterQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}

BlockIO InterpreterAlterQuery::execute()
{
	auto & alter = typeid_cast<ASTAlterQuery &>(*query_ptr);
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
				table->dropPartition(command.partition, command.detach, command.unreplicated, context.getSettingsRef());
				break;

			case PartitionCommand::ATTACH_PARTITION:
				table->attachPartition(command.partition, command.unreplicated, command.part, context.getSettingsRef());
				break;

			case PartitionCommand::FETCH_PARTITION:
				table->fetchPartition(command.partition, command.from, context.getSettingsRef());
				break;

			case PartitionCommand::FREEZE_PARTITION:
				table->freezePartition(command.partition, context.getSettingsRef());
				break;

			default:
				throw Exception("Bad PartitionCommand::Type: " + toString(command.type), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
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
			command.type = AlterCommand::ADD;

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

			out_alter_commands.push_back(command);
		}
		else if (params.type == ASTAlterQuery::DROP_COLUMN)
		{
			AlterCommand command;
			command.type = AlterCommand::DROP;
			command.column_name = typeid_cast<const ASTIdentifier &>(*(params.column)).name;

			out_alter_commands.push_back(command);
		}
		else if (params.type == ASTAlterQuery::MODIFY_COLUMN)
		{
			AlterCommand command;
			command.type = AlterCommand::MODIFY;

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

			out_alter_commands.push_back(command);
		}
		else if (params.type == ASTAlterQuery::DROP_PARTITION)
		{
			const Field & partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;
			out_partition_commands.push_back(PartitionCommand::dropPartition(partition, params.detach, params.unreplicated));
		}
		else if (params.type == ASTAlterQuery::ATTACH_PARTITION)
		{
			const Field & partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;
			out_partition_commands.push_back(PartitionCommand::attachPartition(partition, params.unreplicated, params.part));
		}
		else if (params.type == ASTAlterQuery::FETCH_PARTITION)
		{
			const Field & partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;
			out_partition_commands.push_back(PartitionCommand::fetchPartition(partition, params.from));
		}
		else if (params.type == ASTAlterQuery::FREEZE_PARTITION)
		{
			const Field & partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;
			out_partition_commands.push_back(PartitionCommand::freezePartition(partition));
		}
		else
			throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
	}
}

void InterpreterAlterQuery::updateMetadata(
	const String & database_name,
	const String & table_name,
	const NamesAndTypesList & columns,
	const NamesAndTypesList & materialized_columns,
	const NamesAndTypesList & alias_columns,
	const ColumnDefaults & column_defaults,
	Context & context)
{
	String path = context.getPath();

	String database_name_escaped = escapeForFileName(database_name);
	String table_name_escaped = escapeForFileName(table_name);

	String metadata_path = path + "metadata/" + database_name_escaped + "/" + table_name_escaped + ".sql";
	String metadata_temp_path = metadata_path + ".tmp";

	StringPtr query = new String();
	{
		ReadBufferFromFile in(metadata_path);
		WriteBufferFromString out(*query);
		copyData(in, out);
	}

	ParserCreateQuery parser;
	ASTPtr ast = parseQuery(parser, query->data(), query->data() + query->size(), "in file " + metadata_path);

	ast->query_string = query;

	ASTCreateQuery & attach = typeid_cast<ASTCreateQuery &>(*ast);

	ASTPtr new_columns = InterpreterCreateQuery::formatColumns(columns, materialized_columns, alias_columns, column_defaults);
	*std::find(attach.children.begin(), attach.children.end(), attach.columns) = new_columns;
	attach.columns = new_columns;

	{
		Poco::FileOutputStream ostr(metadata_temp_path);
		formatAST(attach, ostr, 0, false);
	}

	Poco::File(metadata_temp_path).renameTo(metadata_path);
}
