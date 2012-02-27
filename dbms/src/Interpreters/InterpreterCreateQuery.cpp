#include <Poco/File.h>
#include <Poco/FileStream.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/MaterializingBlockInputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTNameTypePair.h>

#include <DB/Storages/StorageLog.h>
#include <DB/Storages/StorageSystemNumbers.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>


namespace DB
{


InterpreterCreateQuery::InterpreterCreateQuery(ASTPtr query_ptr_, Context & context_, size_t max_threads_, size_t max_block_size_)
	: query_ptr(query_ptr_), context(context_), max_threads(max_threads_), max_block_size(max_block_size_)
{
}
	

StoragePtr InterpreterCreateQuery::execute()
{
	Poco::ScopedLock<Poco::FastMutex> lock(*context.mutex);
	
	ASTCreateQuery & create = dynamic_cast<ASTCreateQuery &>(*query_ptr);

	String database_name = create.database.empty() ? context.current_database : create.database;
	String database_name_escaped = escapeForFileName(database_name);
	String table_name = create.table;
	String table_name_escaped = escapeForFileName(table_name);
	String as_database_name = create.as_database.empty() ? context.current_database : create.as_database;
	String as_table_name = create.as_table;
	
	NamesAndTypesListPtr columns = new NamesAndTypesList;
	String data_path = context.path + "data/" + database_name_escaped + "/";
	String metadata_path = context.path + "metadata/" + database_name_escaped + "/" + (!table_name.empty() ?  table_name_escaped + ".sql" : "");

	/// CREATE|ATTACH DATABASE
	if (!database_name.empty() && table_name.empty())
	{
		if (context.databases->end() != context.databases->find(database_name))
		{
			if (!create.if_not_exists)
				throw Exception("Database " + database_name + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
			return NULL;
		}

		if (create.attach)
		{
			if (!Poco::File(data_path).exists())
				throw Exception("Directory " + data_path + " doesn't exist.", ErrorCodes::DIRECTORY_DOESNT_EXIST);
		}
		else
		{
			if (!create.if_not_exists && Poco::File(metadata_path).exists())
				throw Exception("Directory " + metadata_path + " already exists.", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
			if (!create.if_not_exists && Poco::File(data_path).exists())
				throw Exception("Directory " + data_path + " already exists.", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
			
			Poco::File(metadata_path).createDirectory();
			Poco::File(data_path).createDirectory();
		}

		(*context.databases)[database_name];
		return NULL;
	}

	if (context.databases->end() == context.databases->find(database_name))
		throw Exception("Database " + database_name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
		
	if ((*context.databases)[database_name].end() != (*context.databases)[database_name].find(table_name))
	{
		if (create.if_not_exists)
			return (*context.databases)[database_name][table_name];
		else
			throw Exception("Table " + database_name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
	}

	if (!create.as_table.empty()
		&& ((*context.databases).end() == (*context.databases).find(as_database_name)
			|| (*context.databases)[as_database_name].end() == (*context.databases)[as_database_name].find(as_table_name)))
		throw Exception("Table " + as_database_name + "." + as_table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

	SharedPtr<InterpreterSelectQuery> interpreter_select;
	if (create.select)
		interpreter_select = new InterpreterSelectQuery(create.select, context, max_threads, max_block_size);

	/// Получаем список столбцов
	if (create.columns)
	{
		ASTExpressionList & columns_list = dynamic_cast<ASTExpressionList &>(*create.columns);
		for (ASTs::iterator it = columns_list.children.begin(); it != columns_list.children.end(); ++it)
		{
			ASTNameTypePair & name_and_type_pair = dynamic_cast<ASTNameTypePair &>(**it);
			StringRange type_range = name_and_type_pair.type->range;
			columns->push_back(NameAndTypePair(
				name_and_type_pair.name,
				context.data_type_factory->get(String(type_range.first, type_range.second - type_range.first))));
		}
	}
	else if (!create.as_table.empty())
		columns = new NamesAndTypesList((*context.databases)[as_database_name][as_table_name]->getColumnsList());
	else if (create.select)
	{
		Block sample = interpreter_select->getSampleBlock();
		columns = new NamesAndTypesList;
		for (size_t i = 0; i < sample.columns(); ++i)
			columns->push_back(NameAndTypePair(sample.getByPosition(i).name, sample.getByPosition(i).type));
	}
	else
		throw Exception("Incorrect CREATE query: required list of column descriptions or AS section or SELECT.", ErrorCodes::INCORRECT_QUERY);

	/// Выбор нужного движка таблицы

	String storage_name;
	if (create.storage)
		storage_name = dynamic_cast<ASTFunction &>(*create.storage).name;
	else if (!create.as_table.empty())
		storage_name = (*context.databases)[as_database_name][as_table_name]->getName();
	else
		throw Exception("Incorrect CREATE query: required ENGINE.", ErrorCodes::INCORRECT_QUERY);
		
	StoragePtr res = context.storage_factory->get(storage_name, data_path, table_name, columns);

	/// Проверка наличия метаданных таблицы на диске и создание метаданных

	if (Poco::File(metadata_path).exists())
	{
		/** Запрос ATTACH TABLE может использоваться, чтобы создать в оперативке ссылку на уже существующую таблицу.
		  * Это используется, например, при загрузке сервера.
		  */
		if (!create.attach)
			throw Exception("Metadata for table " + database_name + "." + table_name + " already exists.",
				ErrorCodes::TABLE_METADATA_ALREADY_EXISTS);
	}
	else
	{
		Poco::FileOutputStream metadata_file(metadata_path);
		metadata_file << "ATTACH TABLE " << table_name << "\n"
			<< "(\n";

		for (NamesAndTypesList::const_iterator it = columns->begin(); it != columns->end(); ++it)
		{
			String quoted_column_name;
			{
				WriteBufferFromString buf(quoted_column_name);
				writeProbablyBackQuotedString(it->first, buf);
			}
			
			metadata_file << (it != columns->begin() ? ",\n" : "") << "\t" << quoted_column_name << " " << it->second->getName();
		}
			
		metadata_file << "\n) ENGINE = " << storage_name << "\n";
	}

	(*context.databases)[database_name][table_name] = res;

	/// Если запрос CREATE SELECT, то вставим в таблицу данные
	if (create.select)
	{
		BlockInputStreamPtr from = new MaterializingBlockInputStream(interpreter_select->execute());
		copyData(*from, *res->write(query_ptr));
	}
	
	return res;
}


}
