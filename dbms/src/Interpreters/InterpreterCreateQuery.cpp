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

#include <DB/Parsers/formatAST.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>


namespace DB
{


InterpreterCreateQuery::InterpreterCreateQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}
	

StoragePtr InterpreterCreateQuery::execute()
{
	String path = context.getPath();
	String current_database = context.getCurrentDatabase();
	
	ASTCreateQuery & create = dynamic_cast<ASTCreateQuery &>(*query_ptr);

	String database_name = create.database.empty() ? current_database : create.database;
	String database_name_escaped = escapeForFileName(database_name);
	String table_name = create.table;
	String table_name_escaped = escapeForFileName(table_name);
	String as_database_name = create.as_database.empty() ? current_database : create.as_database;
	String as_table_name = create.as_table;
	
	String data_path = path + "data/" + database_name_escaped + "/";
	String metadata_path = path + "metadata/" + database_name_escaped + "/" + (!table_name.empty() ?  table_name_escaped + ".sql" : "");

	/// CREATE|ATTACH DATABASE
	if (!database_name.empty() && table_name.empty())
	{
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

		context.addDatabase(database_name);
		return NULL;
	}

	StoragePtr res;
	SharedPtr<InterpreterSelectQuery> interpreter_select;

	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		context.assertDatabaseExists(database_name);

		if (context.isTableExist(database_name, table_name))
		{
			if (create.if_not_exists)
				return context.getTable(database_name, table_name);
			else
				throw Exception("Table " + database_name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
		}

		if (!create.as_table.empty())
			context.assertTableExists(as_database_name, as_table_name);

		if (create.select)
			interpreter_select = new InterpreterSelectQuery(create.select, context);

		NamesAndTypesListPtr columns = new NamesAndTypesList;

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
					context.getDataTypeFactory().get(String(type_range.first, type_range.second - type_range.first))));
			}
		}
		else if (!create.as_table.empty())
			columns = new NamesAndTypesList(context.getTable(as_database_name, as_table_name)->getColumnsList());
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
			storage_name = context.getTable(as_database_name, as_table_name)->getName();
		else
			throw Exception("Incorrect CREATE query: required ENGINE.", ErrorCodes::INCORRECT_QUERY);

		res = context.getStorageFactory().get(storage_name, data_path, table_name, context, query_ptr, columns);

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
			/// Меняем CREATE на ATTACH и пишем запрос в файл.
			ASTPtr attach = query_ptr->clone();
			dynamic_cast<ASTCreateQuery &>(*attach).attach = true;

			Poco::FileOutputStream metadata_file(metadata_path);
			formatAST(*attach, metadata_file, 0, false);
			metadata_file << "\n";
		}

		context.addTable(database_name, table_name, res);
	}

	/// Если запрос CREATE SELECT, то вставим в таблицу данные
	if (create.select)
	{
		BlockInputStreamPtr from = new MaterializingBlockInputStream(interpreter_select->execute());
		copyData(*from, *res->write(query_ptr));
	}
	
	return res;
}


}
