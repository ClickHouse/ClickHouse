#include <Poco/File.h>

#include <DB/Common/escapeForFileName.h>
#include <DB/Parsers/ASTDropQuery.h>
#include <DB/Interpreters/InterpreterDropQuery.h>
#include <DB/Storages/IStorage.h>
#include <DB/Databases/IDatabase.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int TABLE_WAS_NOT_DROPPED;
	extern const int DATABASE_NOT_EMPTY;
	extern const int UNKNOWN_DATABASE;
}


InterpreterDropQuery::InterpreterDropQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterDropQuery::execute()
{
	String path = context.getPath();
	String current_database = context.getCurrentDatabase();

	ASTDropQuery & drop = typeid_cast<ASTDropQuery &>(*query_ptr);

	bool drop_database = drop.table.empty() && !drop.database.empty();

	if (drop_database && drop.detach)
	{
		context.detachDatabase(drop.database);
		return {};
	}

	String database_name = drop.database.empty() ? current_database : drop.database;
	String database_name_escaped = escapeForFileName(database_name);

	String data_path = path + "data/" + database_name_escaped + "/";
	String metadata_path = path + "metadata/" + database_name_escaped + "/";

	auto database = context.tryGetDatabase(database_name);
	if (!database && !drop.if_exists)
		throw Exception("Database " + database_name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

	std::vector<std::pair<
		StoragePtr,
		std::unique_ptr<DDLGuard>>> tables_to_drop;

	if (!drop_database)
	{
		StoragePtr table;

		if (drop.if_exists)
			table = context.tryGetTable(database_name, drop.table);
		else
			table = context.getTable(database_name, drop.table);

		if (table)
			tables_to_drop.emplace_back(table, context.getDDLGuard(database_name, drop.table,
				"Table " + database_name + "." + drop.table + " is dropping or detaching right now"));
		else
			return {};
	}
	else
	{
		if (!database)
		{
			if (!drop.if_exists)
				throw Exception("Database " + database_name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
			return {};
		}

		for (auto iterator = database->getIterator(); iterator->isValid(); iterator->next())
			tables_to_drop.emplace_back(iterator->table(), context.getDDLGuard(database_name, iterator->name(),
				"Table " + database_name + "." + iterator->name() + " is dropping or detaching right now"));
	}

	for (auto & table : tables_to_drop)
	{
		table.first->shutdown();

		/// Если кто-то успел удалить эту таблицу, выбросит исключение.
		auto table_lock = table.first->lockForAlter();

		String current_table_name = table.first->getTableName();

		if (drop.detach)
		{
			/// Удаляем таблицу из оперативки, метаданные и данные не трогаем.
			database->detachTable(current_table_name);
		}
		else
		{
			/// Удаляем метаданные и саму таблицу из оперативки.
			database->removeTable(current_table_name);

			/// Удаляем данные таблицы
			table.first->drop();		/// TODO Не удалять метаданные, если таблицу не получилось удалить.
			table.first->is_dropped = true;

			String current_data_path = data_path + escapeForFileName(current_table_name);

			if (Poco::File(current_data_path).exists())
				Poco::File(current_data_path).remove(true);
		}
	}

	if (drop_database)
	{
		/// Удаление базы данных. Таблицы в ней уже удалены.

		auto lock = context.getLock();

		/// Кто-то мог успеть удалить БД до нас.
		context.assertDatabaseExists(database_name);

		/// Кто-то мог успеть создать таблицу в удаляемой БД, пока мы удаляли таблицы без лока контекста.
		if (!context.getDatabase(database_name)->empty())
			throw Exception("New table appeared in database being dropped. Try dropping it again.", ErrorCodes::DATABASE_NOT_EMPTY);

		/// Удаляем информацию о БД из оперативки
		auto database = context.detachDatabase(database_name);

		/// Удаляем БД.
		database->drop();

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
