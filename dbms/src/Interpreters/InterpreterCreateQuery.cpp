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

#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/formatAST.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/DataTypes/DataTypeNested.h>


namespace DB
{


InterpreterCreateQuery::InterpreterCreateQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}


StoragePtr InterpreterCreateQuery::execute(bool assume_metadata_exists)
{
	String path = context.getPath();
	String current_database = context.getCurrentDatabase();

	ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_ptr);

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

		if (!create.if_not_exists || !context.isDatabaseExist(database_name))
			context.addDatabase(database_name);

		return StoragePtr();
	}

	SharedPtr<InterpreterSelectQuery> interpreter_select;
	Block select_sample;
	/// Для таблиц типа view, чтобы получить столбцы, может понадобиться sample_block.
	if (create.select && (!create.attach || (!create.columns && (create.is_view || create.is_materialized_view))))
		select_sample = InterpreterSelectQuery(create.select, context).getSampleBlock();

	StoragePtr res;
	String storage_name;
	NamesAndTypesListPtr columns = new NamesAndTypesList;

	StoragePtr as_storage;
	IStorage::TableStructureReadLockPtr as_storage_lock;
	if (!as_table_name.empty())
	{
		as_storage = context.getTable(as_database_name, as_table_name);
		as_storage_lock = as_storage->lockStructure(false);
	}

	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		if (!create.is_temporary)
		{
			context.assertDatabaseExists(database_name);

			if (context.isTableExist(database_name, table_name))
			{
				if (create.if_not_exists)
					return context.getTable(database_name, table_name);
				else
					throw Exception("Table " + database_name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
			}
		}

		/// Получаем список столбцов
		if (create.columns)
		{
			columns = new NamesAndTypesList(parseColumns(create.columns, context.getDataTypeFactory()));
		}
		else if (!create.as_table.empty())
			columns = new NamesAndTypesList(as_storage->getColumnsList());
		else if (create.select)
		{
			columns = new NamesAndTypesList;
			for (size_t i = 0; i < select_sample.columns(); ++i)
				columns->push_back(NameAndTypePair(select_sample.getByPosition(i).name, select_sample.getByPosition(i).type));
		}
		else
			throw Exception("Incorrect CREATE query: required list of column descriptions or AS section or SELECT.", ErrorCodes::INCORRECT_QUERY);

		/// Даже если в запросе был список столбцов, на всякий случай приведем его к стандартному виду (развернем Nested).
		ASTPtr new_columns = formatColumns(*columns);
		if (create.columns)
		{
			auto it = std::find(create.children.begin(), create.children.end(), create.columns);
			if (it != create.children.end())
				*it = new_columns;
			else
				create.children.push_back(new_columns);
		}
		else
			create.children.push_back(new_columns);
		create.columns = new_columns;

		/// Выбор нужного движка таблицы
		if (create.storage)
		{
			storage_name = typeid_cast<ASTFunction &>(*create.storage).name;
		}
		else if (!create.as_table.empty())
		{
			storage_name = as_storage->getName();
			create.storage = typeid_cast<const ASTCreateQuery &>(*context.getCreateQuery(as_database_name, as_table_name)).storage;
		}
		else if (create.is_temporary)
		{
			storage_name = "Memory";
			ASTFunction * func = new ASTFunction();
			func->name = storage_name;
			create.storage = func;
		}
		else if (create.is_view)
		{
			storage_name = "View";
			ASTFunction * func = new ASTFunction();
			func->name = storage_name;
			create.storage = func;
		}
		else if (create.is_materialized_view)
		{
			storage_name = "MaterializedView";
			ASTFunction * func = new ASTFunction();
			func->name = storage_name;
			create.storage = func;
		}
		else
			throw Exception("Incorrect CREATE query: required ENGINE.", ErrorCodes::ENGINE_REQUIRED);

		res = context.getStorageFactory().get(
			storage_name, data_path, table_name, database_name, context,
			context.getGlobalContext(), query_ptr, columns, create.attach);

		/// Проверка наличия метаданных таблицы на диске и создание метаданных
		if (!assume_metadata_exists && !create.is_temporary)
		{
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
				ASTPtr attach_ptr = query_ptr->clone();
				ASTCreateQuery & attach = typeid_cast<ASTCreateQuery &>(*attach_ptr);

				attach.attach = true;
				attach.database.clear();
				attach.as_database.clear();
				attach.as_table.clear();
				attach.if_not_exists = false;
				attach.is_populate = false;

				/// Для engine VIEW необходимо сохранить сам селект запрос, для остальных - наоборот
				if (storage_name != "View" && storage_name != "MaterializedView")
					attach.select = nullptr;

				Poco::FileOutputStream metadata_file(metadata_path);
				formatAST(attach, metadata_file, 0, false);
				metadata_file << "\n";
			}
		}

		if (create.is_temporary)
		{
			context.getSessionContext().addExternalTable(table_name, res);
		}
		else
			context.addTable(database_name, table_name, res);
	}

	/// Если запрос CREATE SELECT, то вставим в таблицу данные
	if (create.select && storage_name != "View" && (storage_name != "MaterializedView" || create.is_populate))
	{
		BlockInputStreamPtr from = new MaterializingBlockInputStream(interpreter_select->execute());
		copyData(*from, *res->write(query_ptr));
	}

	return res;
}

NamesAndTypesList InterpreterCreateQuery::parseColumns(ASTPtr expression_list, const DataTypeFactory & data_type_factory)
{
	NamesAndTypesList columns;
	ASTExpressionList & columns_list = typeid_cast<ASTExpressionList &>(*expression_list);
	for (const ASTPtr & ast : columns_list.children)
	{
		const ASTNameTypePair & name_and_type_pair = typeid_cast<const ASTNameTypePair &>(*ast);
		StringRange type_range = name_and_type_pair.type->range;
		columns.push_back(NameAndTypePair(
			name_and_type_pair.name,
			data_type_factory.get(String(type_range.first, type_range.second - type_range.first))));
	}
	columns = *DataTypeNested::expandNestedColumns(columns);
	return columns;
}

ASTPtr InterpreterCreateQuery::formatColumns(const NamesAndTypesList & columns)
{
	ASTPtr columns_list_ptr = new ASTExpressionList;
	ASTExpressionList & columns_list = typeid_cast<ASTExpressionList &>(*columns_list_ptr);

	for (const NameAndTypePair & it : columns)
	{
		ASTPtr name_and_type_pair_ptr = new ASTNameTypePair;
		ASTNameTypePair & name_and_type_pair = typeid_cast<ASTNameTypePair &>(*name_and_type_pair_ptr);
		name_and_type_pair.name = it.name;
		StringPtr type_name = new String(it.type->getName());

		ParserIdentifierWithOptionalParameters storage_p;
		Expected expected = "";
		const char * pos = type_name->data();
		const char * end = pos + type_name->size();

		if (!storage_p.parse(pos, end, name_and_type_pair.type, expected))
			throw Exception("Cannot parse data type.", ErrorCodes::SYNTAX_ERROR);

		name_and_type_pair.type->query_string = type_name;
		columns_list.children.push_back(name_and_type_pair_ptr);
	}

	return columns_list_ptr;
}

}
