#include <Poco/File.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/Parsers/ASTDropQuery.h>

#include <DB/Interpreters/InterpreterDropQuery.h>


namespace DB
{


InterpreterDropQuery::InterpreterDropQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}


void InterpreterDropQuery::execute()
{
	Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
	
	String path = context.getPath();
	String current_database = context.getCurrentDatabase();
	
	ASTDropQuery & drop = dynamic_cast<ASTDropQuery &>(*query_ptr);

	String database_name = drop.database.empty() ? current_database : drop.database;
	String database_name_escaped = escapeForFileName(database_name);
	String table_name = drop.table;
	String table_name_escaped = escapeForFileName(table_name);

	String data_path = path + "data/" + database_name_escaped + "/" + table_name_escaped;
	String metadata_path = path + "metadata/" + database_name_escaped + "/" + (!table_name.empty() ?  table_name_escaped + ".sql" : "");

	if (!drop.if_exists)
		context.assertDatabaseExists(database_name);

	if (!drop.table.empty())
	{
		/// Удаление таблицы
		if (!context.isTableExist(database_name, table_name))
		{
			if (!drop.if_exists)
				throw Exception("Table " + database_name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
		}
		else
		{
			/// Удаляем данные таблицы
			if (!drop.detach)
			{
				StoragePtr table = context.getTable(database_name, table_name);
				DatabaseDropperPtr database_dropper = context.getDatabaseDropper(database_name);
				table->path_to_remove_on_drop = data_path;
				/// Присвоим database_to_drop на случай, если БД попробуют удалить до завершения удаления этой таблицы.
				table->database_to_drop = database_dropper;
				table->drop();

				Poco::File(metadata_path).remove();
			}

			/// Удаляем информацию о таблице из оперативки
			context.detachTable(database_name, table_name);
		}
	}
	else
	{
		if (context.isDatabaseExist(database_name))
		{
			/// Удаление базы данных
			if (!drop.detach)
			{
				/// Тот, кто удалит директорию с БД, когда все ее таблицы будут удалены.
				DatabaseDropperPtr database_dropper = context.getDatabaseDropper(database_name);
				database_dropper->drop_on_destroy = true;
				
				/// Удаление всех таблиц
				for (Tables::iterator it = context.getDatabases()[database_name].begin(); it != context.getDatabases()[database_name].end(); ++it)
				{
					StoragePtr table = it->second;
					table->path_to_remove_on_drop = data_path + escapeForFileName(it->first);
					table->database_to_drop = database_dropper;
					table->drop();
				}

				Poco::File(metadata_path).remove(true);
			}

			/// Удаляем информацию о БД из оперативки
			context.detachDatabase(database_name);
		}
	}
}


}
