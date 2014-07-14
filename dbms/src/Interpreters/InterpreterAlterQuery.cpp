#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
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
	ASTAlterQuery & alter = typeid_cast<ASTAlterQuery &>(*query_ptr);
	String & table_name = alter.table;
	String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;
	AlterCommands commands = parseAlter(alter.parameters, context.getDataTypeFactory());

	StoragePtr table = context.getTable(database_name, table_name);
	table->alter(commands, database_name, table_name, context);
}

AlterCommands InterpreterAlterQuery::parseAlter(
	const ASTAlterQuery::ParameterContainer & params_container, const DataTypeFactory & data_type_factory)
{
	AlterCommands res;

	for (const auto & params : params_container)
	{
		res.push_back(AlterCommand());
		AlterCommand & command = res.back();

		if (params.type == ASTAlterQuery::ADD)
		{
			command.type = AlterCommand::ADD;

			const ASTNameTypePair & ast_name_type = typeid_cast<const ASTNameTypePair &>(*params.name_type);
			StringRange type_range = ast_name_type.type->range;
			String type_string = String(type_range.first, type_range.second - type_range.first);

			command.column_name = ast_name_type.name;
			command.data_type = data_type_factory.get(type_string);

			if (params.column)
				command.after_column = typeid_cast<const ASTIdentifier &>(*params.column).name;
		}
		else if (params.type == ASTAlterQuery::DROP)
		{
			command.type = AlterCommand::DROP;
			command.column_name = typeid_cast<const ASTIdentifier &>(*(params.column)).name;
		}
		else if (params.type == ASTAlterQuery::MODIFY)
		{
			command.type = AlterCommand::MODIFY;

			const ASTNameTypePair & ast_name_type = typeid_cast<const ASTNameTypePair &>(*params.name_type);
			StringRange type_range = ast_name_type.type->range;
			String type_string = String(type_range.first, type_range.second - type_range.first);

			command.column_name = ast_name_type.name;
			command.data_type = data_type_factory.get(type_string);
		}
		else
			throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
	}

	return res;
}

void InterpreterAlterQuery::updateMetadata(
	const String& database_name, const String& table_name, const NamesAndTypesList& columns, Context& context)
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

void InterpreterAlterQuery::updateMetadata(
	const String & database_name, const String & table_name, const NamesAndTypesList & columns, Context & context)
{
	String path = context.getPath();

	String database_name_escaped = escapeForFileName(database_name);
	String table_name_escaped = escapeForFileName(table_name);

	String metadata_path = path + "metadata/" + database_name_escaped + "/" + table_name_escaped + ".sql";

	ASTPtr attach_ptr = context.getCreateQuery(database_name, table_name);
	ASTCreateQuery & attach = typeid_cast<ASTCreateQuery &>(*attach_ptr);
	attach.attach = true;
	ASTs & columns = typeid_cast<ASTExpressionList &>(*attach.columns).children;

	/// Различные проверки, на возможность выполнения запроса
	ASTs columns_copy;
	for (const auto & ast : columns)
		columns_copy.push_back(ast->clone());

	IdentifierNameSet identifier_names;
	attach.storage->collectIdentifierNames(identifier_names);
	for (ASTAlterQuery::ParameterContainer::const_iterator alter_it = alter.parameters.begin();
			alter_it != alter.parameters.end(); ++alter_it)
	{
		const ASTAlterQuery::Parameters & params = *alter_it;

		if (params.type == ASTAlterQuery::ADD)
		{
			addColumnToAST(table, columns_copy, params.name_type, params.column);
		}
		else if (params.type == ASTAlterQuery::DROP)
		{
			const ASTIdentifier & drop_column = typeid_cast<const ASTIdentifier &>(*params.column);

			/// Проверяем, что поле не является ключевым
			if (identifier_names.find(drop_column.name) != identifier_names.end())
				throw Exception("Cannot drop key column", DB::ErrorCodes::ILLEGAL_COLUMN);

			dropColumnFromAST(drop_column, columns_copy);
		}
		else if (params.type == ASTAlterQuery::MODIFY)
		{
			const ASTNameTypePair & name_type = typeid_cast<const ASTNameTypePair &>(*params.name_type);
			StringRange type_range = name_type.type->range;

			/// проверяем корректность типа. В случае некоректного типа будет исключение
			String type(type_range.first, type_range.second - type_range.first);
			data_type_factory.get(type);

			/// проверяем, что колонка существует
			auto modified_column = std::find_if(columns_copy.begin(), columns_copy.end(), boost::bind(namesEqual, name_type.name, _1));
			if ( modified_column == columns_copy.end())
				throw Exception("Wrong column name. Column " + name_type.name  + " not exists", DB::ErrorCodes::ILLEGAL_COLUMN);

			/// Проверяем, что поле не является ключевым
			if (identifier_names.find(name_type.name) != identifier_names.end())
				throw Exception("Modification of primary column not supported", DB::ErrorCodes::ILLEGAL_COLUMN);

			/// к сожалению, проверить на возможно ли это приведение типов можно только во время выполнения
		}
	}

	/// Пока разрешим читать из таблицы. Запретим при первой попытке изменить структуру таблицы.
	/// Это позволит сделать большую часть первого MODIFY, не останавливая чтение из таблицы.
	IStorage::TableStructureWriteLockPtr table_hard_lock;

	/// todo cycle over sub tables and tables
	/// Применяем изменения
	for (ASTAlterQuery::ParameterContainer::const_iterator alter_it = alter.parameters.begin();
			alter_it != alter.parameters.end(); ++alter_it)
	{
		const ASTAlterQuery::Parameters & params = *alter_it;

		if (params.type == ASTAlterQuery::MODIFY)
		{
			table->prepareAlterModify(params);

			if (!table_hard_lock)
				table_hard_lock = table->lockStructureForAlter();

			table->commitAlterModify(params);
		}
		else
		{
			if (!table_hard_lock)
				table_hard_lock = table->lockStructureForAlter();

			table->alter(params);
		}

		if (params.type == ASTAlterQuery::ADD)
		{
			addColumnToAST(table, columns, params.name_type, params.column);
		}
		else if (params.type == ASTAlterQuery::DROP)
		{
			const ASTIdentifier & drop_column = typeid_cast<const ASTIdentifier &>(*params.column);
			dropColumnFromAST(drop_column, columns);
		}
		else if (params.type == ASTAlterQuery::MODIFY)
		{
			const ASTNameTypePair & name_type = typeid_cast<const ASTNameTypePair &>(*params.name_type);
			ASTs::iterator modify_it = std::find_if(columns.begin(), columns.end(), boost::bind(namesEqual, name_type.name, _1));
			ASTNameTypePair & modified_column = typeid_cast<ASTNameTypePair &>(**modify_it);
			modified_column.type = name_type.type;
		}

		/// Перезаписываем файл метадата каждую итерацию
		Poco::FileOutputStream ostr(metadata_path);
		formatAST(attach, ostr, 0, false);
	}
}
