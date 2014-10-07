#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Parsers/ASTAlterQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTIdentifier.h>

#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/IO/copyData.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Storages/StorageMerge.h>
#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>

#include <Poco/FileStream.h>

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/bind/placeholders.hpp>


using namespace DB;

InterpreterAlterQuery::InterpreterAlterQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}

void InterpreterAlterQuery::execute()
{
	auto & alter = typeid_cast<ASTAlterQuery &>(*query_ptr);
	const String & table_name = alter.table;
	String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;
	StoragePtr table = context.getTable(database_name, table_name);
	validateColumnChanges(alter.parameters, table, context);

	AlterCommands alter_commands;
	PartitionCommands partition_commands;
	parseAlter(alter.parameters, context.getDataTypeFactory(), alter_commands, partition_commands);

	for (const PartitionCommand & command : partition_commands)
	{
		if (command.type == PartitionCommand::DROP_PARTITION)
			table->dropPartition(command.partition, command.detach);
		else if (command.type == PartitionCommand::ATTACH_PARTITION)
			table->attachPartition(command.partition, command.unreplicated, command.part);
		else
			throw Exception("Bad PartitionCommand::Type: " + toString(command.type), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
	}

	if (!alter_commands.empty())
		table->alter(alter_commands, database_name, table_name, context);
}

void InterpreterAlterQuery::parseAlter(
	const ASTAlterQuery::ParameterContainer & params_container, const DataTypeFactory & data_type_factory,
	AlterCommands & out_alter_commands, PartitionCommands & out_partition_commands)
{
	for (const auto & params : params_container)
	{
		if (params.type == ASTAlterQuery::ADD_COLUMN)
		{
			AlterCommand command;
			command.type = AlterCommand::ADD;

			const auto & ast_col_decl = typeid_cast<const ASTColumnDeclaration &>(*params.col_decl);
			StringRange type_range = ast_col_decl.type->range;
			String type_string = String(type_range.first, type_range.second - type_range.first);

			command.column_name = ast_col_decl.name;
			command.data_type = data_type_factory.get(type_string);

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
			StringRange type_range = ast_col_decl.type->range;
			String type_string = String(type_range.first, type_range.second - type_range.first);

			command.column_name = ast_col_decl.name;
			command.data_type = data_type_factory.get(type_string);

			out_alter_commands.push_back(command);
		}
		else if (params.type == ASTAlterQuery::DROP_PARTITION)
		{
			const Field & partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;
			out_partition_commands.push_back(PartitionCommand::dropPartition(partition, params.detach));
		}
		else if (params.type == ASTAlterQuery::ATTACH_PARTITION)
		{
			const Field & partition = dynamic_cast<const ASTLiteral &>(*params.partition).value;
			out_partition_commands.push_back(PartitionCommand::attachPartition(partition, params.unreplicated, params.part));
		}
		else
			throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
	}
}

void InterpreterAlterQuery::validateColumnChanges(ASTAlterQuery::ParameterContainer & params_container, const StoragePtr & table, const Context & context)
{
	auto columns = table->getColumnsList();
	columns.insert(std::end(columns), std::begin(table->materialized_columns), std::end(table->materialized_columns));
	columns.insert(std::end(columns), std::begin(table->alias_columns), std::end(table->alias_columns));
	auto defaults = table->column_defaults;

	std::vector<std::pair<IDataType *, ASTColumnDeclaration *>> defaulted_columns{};

	ASTPtr default_expr_list{new ASTExpressionList};
	default_expr_list->children.reserve(table->column_defaults.size());

	for (auto & params : params_container)
	{
		if (params.type == ASTAlterQuery::ADD_COLUMN || params.type == ASTAlterQuery::MODIFY_COLUMN)
		{
			auto & col_decl = typeid_cast<ASTColumnDeclaration &>(*params.col_decl);
			const auto & column_name = col_decl.name;

			if (params.type == ASTAlterQuery::MODIFY_COLUMN)
			{
				const auto it = std::find_if(std::begin(columns), std::end(columns),
					std::bind(AlterCommand::namesEqual, std::cref(column_name), std::placeholders::_1));

				if (it == std::end(columns))
					throw Exception("Wrong column name. Cannot find column " + column_name + " to modify.",
									DB::ErrorCodes::ILLEGAL_COLUMN);

				columns.erase(it);
				defaults.erase(column_name);
			}

			if (col_decl.type)
			{
				const StringRange & type_range = col_decl.type->range;
				columns.emplace_back(col_decl.name,
					context.getDataTypeFactory().get({type_range.first, type_range.second}));
			}

			if (col_decl.default_expression)
			{

				if (col_decl.type)
				{
					const auto tmp_column_name = col_decl.name + "_tmp";
					const auto & final_column_name = col_decl.name;
					const auto conversion_function_name = "to" + columns.back().type->getName();

					default_expr_list->children.emplace_back(setAlias(
						makeASTFunction(conversion_function_name, ASTPtr{new ASTIdentifier{{}, tmp_column_name}}),
						final_column_name));

					default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), tmp_column_name));

					defaulted_columns.emplace_back(columns.back().type.get(), &col_decl);
				}
				else
				{
					default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), col_decl.name));

					defaulted_columns.emplace_back(nullptr, &col_decl);
				}
			}
		}
		else if (params.type == ASTAlterQuery::DROP_COLUMN)
		{
			const auto & column_name = typeid_cast<const ASTIdentifier &>(*(params.column)).name;

			auto found = false;
			for (auto it = std::begin(columns); it != std::end(columns);)
				if (AlterCommand::namesEqual(column_name, *it))
				{
					found = true;
					it = columns.erase(it);
				}
				else
					++it;

			for (auto it = std::begin(defaults); it != std::end(defaults);)
				if (AlterCommand::namesEqual(column_name, { it->first, nullptr }))
					it = defaults.erase(it);
				else
					++it;

			if (!found)
				throw Exception("Wrong column name. Cannot find column " + column_name + " to drop.",
								DB::ErrorCodes::ILLEGAL_COLUMN);
		}
	}

	for (const auto & col_def : defaults)
		default_expr_list->children.emplace_back(setAlias(col_def.second.expression->clone(), col_def.first));

	const auto actions = ExpressionAnalyzer{default_expr_list, context, columns}.getActions(true);
	const auto block = actions->getSampleBlock();

	for (auto & column : defaulted_columns)
	{
		const auto type_ptr = column.first;
		const auto col_decl_ptr = column.second;

		if (type_ptr)
		{
			const auto & tmp_column = block.getByName(col_decl_ptr->name + "_tmp");

			/// type mismatch between explicitly specified and deduced type, add conversion
			if (typeid(*type_ptr) != typeid(*tmp_column.type))
			{
				col_decl_ptr->default_expression = makeASTFunction(
					"to" + type_ptr->getName(),
					col_decl_ptr->default_expression);

				col_decl_ptr->children.clear();
				col_decl_ptr->children.push_back(col_decl_ptr->type);
				col_decl_ptr->children.push_back(col_decl_ptr->default_expression);
			}
		}
		else
		{
			col_decl_ptr->type = new ASTIdentifier{};
			col_decl_ptr->query_string = new String{block.getByName(col_decl_ptr->name).type->getName()};
			col_decl_ptr->range = {
				col_decl_ptr->query_string->data(),
				col_decl_ptr->query_string->data() + col_decl_ptr->query_string->size()
			};
			static_cast<ASTIdentifier &>(*col_decl_ptr->type).name = *col_decl_ptr->query_string;
		}

		defaults.emplace(col_decl_ptr->name, ColumnDefault{
			columnDefaultTypeFromString(col_decl_ptr->default_specifier),
			setAlias(col_decl_ptr->default_expression, col_decl_ptr->name)
		});
	}
}

void InterpreterAlterQuery::updateMetadata(
	const String & database_name, const String & table_name, const NamesAndTypesList & columns, Context & context)
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

	const char * begin = query->data();
	const char * end = begin + query->size();
	const char * pos = begin;

	ParserCreateQuery parser;
	ASTPtr ast;
	Expected expected = "";
	bool parse_res = parser.parse(pos, end, ast, expected);

	/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
	if (!parse_res || (pos != end && *pos != ';'))
		throw Exception(getSyntaxErrorMessage(parse_res, begin, end, pos, expected, "in file " + metadata_path),
			DB::ErrorCodes::SYNTAX_ERROR);

	ast->query_string = query;

	ASTCreateQuery & attach = typeid_cast<ASTCreateQuery &>(*ast);

	ASTPtr new_columns = InterpreterCreateQuery::formatColumns(columns);
	*std::find(attach.children.begin(), attach.children.end(), attach.columns) = new_columns;
	attach.columns = new_columns;

	{
		Poco::FileOutputStream ostr(metadata_temp_path);
		formatAST(attach, ostr, 0, false);
	}

	Poco::File(metadata_temp_path).renameTo(metadata_path);
}
