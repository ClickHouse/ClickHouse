#include <Poco/FileStream.h>

#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTNameTypePair.h>

#include <DB/Storages/StorageLog.h>
#include <DB/Storages/StorageSystemNumbers.h>

#include <DB/Interpreters/InterpreterCreateQuery.h>


namespace DB
{


StoragePtr InterpreterCreateQuery::execute(ASTPtr query, Context & context)
{
	ASTCreateQuery & create = dynamic_cast<ASTCreateQuery &>(*query);

	String database_name = create.database.empty() ? context.current_database : create.database;
	String table_name = create.table;
	SharedPtr<NamesAndTypes> columns = new NamesAndTypes;
	String data_path = context.path + "data/" + database_name + "/";	/// TODO: эскейпинг
	String metadata_path = context.path + "metadata/" + database_name + "/" + table_name + ".sql";

	{
		Poco::ScopedLock<Poco::FastMutex> lock(*context.mutex);
		
		if ((*context.databases)[database_name].end() != (*context.databases)[database_name].find(table_name))
		{
			if (create.if_not_exists)
				return (*context.databases)[database_name][table_name];
			else
				throw Exception("Table " + database_name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
		}
	}
	
	ASTExpressionList & columns_list = dynamic_cast<ASTExpressionList &>(*create.columns);
	for (ASTs::iterator it = columns_list.children.begin(); it != columns_list.children.end(); ++it)
	{
		ASTNameTypePair & name_and_type_pair = dynamic_cast<ASTNameTypePair &>(**it);
		StringRange type_range = name_and_type_pair.type->range;
		(*columns)[name_and_type_pair.name] = context.data_type_factory->get(String(type_range.first, type_range.second - type_range.first));
	}

	ASTFunction & storage_expr = dynamic_cast<ASTFunction &>(*create.storage);
	String storage_str(storage_expr.range.first, storage_expr.range.second - storage_expr.range.first);
	StoragePtr res;

	/// Выбор нужного движка таблицы

	if (storage_expr.name == "Log")
	{
		if (storage_expr.arguments)
			throw Exception("Storage Log doesn't allow parameters", ErrorCodes::STORAGE_DOESNT_ALLOW_PARAMETERS);

		res = new StorageLog(data_path, table_name, columns);
	}
	else if (storage_expr.name == "SystemNumbers")
	{
		if (storage_expr.arguments)
			throw Exception("Storage SystemNumbers doesn't allow parameters", ErrorCodes::STORAGE_DOESNT_ALLOW_PARAMETERS);
		if (columns->size() != 1 || columns->begin()->first != "number" || columns->begin()->second->getName() != "UInt64")
			throw Exception("Storage SystemNumbers only allows one column with name 'number' and type 'UInt64'", ErrorCodes::ILLEGAL_COLUMN);

		res = new StorageSystemNumbers(table_name);
	}
	else
		throw Exception("Unknown storage " + storage_str, ErrorCodes::UNKNOWN_STORAGE);

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
		metadata_file << String(create.range.first, create.range.second - create.range.first) << std::endl;
	}

	{
		Poco::ScopedLock<Poco::FastMutex> lock(*context.mutex);

		(*context.databases)[database_name][table_name] = res;
	}
	
	return res;
}


}
