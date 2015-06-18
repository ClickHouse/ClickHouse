#include <Poco/File.h>

#include <DB/Common/escapeForFileName.h>
#include <DB/Parsers/ASTDropQuery.h>
#include <DB/Interpreters/InterpreterDropQuery.h>
#include <DB/Storages/IStorage.h>


namespace DB
{


InterpreterDropQuery::InterpreterDropQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterDropQuery::execute()
{
	String path = context.getPath();
	String current_database = context.getCurrentDatabase();

	ASTDropQuery & drop = typeid_cast<ASTDropQuery &>(*query_ptr);

	String database_name = drop.database.empty() ? current_database : drop.database;
	String database_name_escaped = escapeForFileName(database_name);

	String data_path = path + "data/" + database_name_escaped + "/";
	String metadata_path = path + "metadata/" + database_name_escaped + "/";

	StorageVector tables_to_drop;

	if (!drop.table.empty())
	{
		StoragePtr table;

		if (drop.if_exists)
			table = context.tryGetTable(database_name, drop.table);
		else
			table = context.getTable(database_name, drop.table);

		if (table)
			tables_to_drop.push_back(table);
		else
			return {};
	}
	else
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		if (!drop.if_exists)
			context.assertDatabaseExists(database_name);
		else if (!context.isDatabaseExist(database_name))
			return {};

		Tables tables = context.getDatabases()[database_name];

		for (auto & it : tables)
		{
			tables_to_drop.push_back(it.second);
		}
	}

	for (StoragePtr table : tables_to_drop)
	{
		table->shutdown();

		/// Если кто-то успел удалить эту таблицу, выбросит исключение.
		auto table_lock = table->lockForAlter();

		String current_table_name = table->getTableName();

		/// Удаляем информацию о таблице из оперативки
		context.detachTable(database_name, current_table_name);

		/// Удаляем данные таблицы
		if (!drop.detach)
		{
			String current_data_path = data_path + escapeForFileName(current_table_name);
			String current_metadata_path = metadata_path + escapeForFileName(current_table_name) + ".sql";

			/// Для таблиц типа ChunkRef, файла с метаданными не существует.
			if (Poco::File(current_metadata_path).exists())
				Poco::File(current_metadata_path).remove();

			table->drop();
			table->is_dropped = true;

			if (Poco::File(current_data_path).exists())
				Poco::File(current_data_path).remove(true);
		}
	}

	if (drop.table.empty())
	{
		/// Удаление базы данных. Таблицы в ней уже удалены.

		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

		/// Кто-то мог успеть удалить БД до нас.
		context.assertDatabaseExists(database_name);

		/// Кто-то мог успеть создать таблицу в удаляемой БД, пока мы удаляли таблицы без лока контекста.
		if (!context.getDatabases()[database_name].empty())
			throw Exception("New table appeared in database being dropped. Try dropping it again.", ErrorCodes::DATABASE_NOT_EMPTY);

		/// Удаляем информацию о БД из оперативки
		context.detachDatabase(database_name);

		Poco::File(data_path).remove(false);
		Poco::File(metadata_path).remove(false);
	}

	return {};
}

void InterpreterDropQuery::dropDetachedTable(String database_name, StoragePtr table, Context & context)
{
	table->shutdown();

	auto table_lock = table->lockForAlter();

	String table_name = table->getTableName();

	String path = context.getPath();
	String database_name_escaped = escapeForFileName(database_name);

	String data_path = path + "data/" + database_name_escaped + "/" + escapeForFileName(table_name);
	String metadata_path = path + "metadata/" + database_name_escaped + "/" + escapeForFileName(table_name) + ".sql";

	if (Poco::File(metadata_path).exists())
		Poco::File(metadata_path).remove();

	table->drop();
	table->is_dropped = true;

	if (Poco::File(data_path).exists())
		Poco::File(data_path).remove(true);
}


}
