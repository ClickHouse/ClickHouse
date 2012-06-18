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
	Poco::ScopedLock<Poco::Mutex> lock(*context.mutex);
	
	ASTDropQuery & drop = dynamic_cast<ASTDropQuery &>(*query_ptr);

	String database_name = drop.database.empty() ? context.current_database : drop.database;
	String database_name_escaped = escapeForFileName(database_name);
	String table_name = drop.table;
	String table_name_escaped = escapeForFileName(table_name);

	String data_path = context.path + "data/" + database_name_escaped + "/" + table_name_escaped;
	String metadata_path = context.path + "metadata/" + database_name_escaped + "/" + (!table_name.empty() ?  table_name_escaped + ".sql" : "");

	if (!drop.if_exists && context.databases->end() == context.databases->find(database_name))
		throw Exception("Database " + database_name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

	if (!drop.table.empty())
	{
		/// Удаление таблицы
		if ((*context.databases)[database_name].end() == (*context.databases)[database_name].find(table_name))
		{
			if (!drop.if_exists)
				throw Exception("Table " + database_name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
		}
		else
		{
			/// Удаляем данные таблицы
			if (!drop.detach)
			{
				StoragePtr table = (*context.databases)[database_name][table_name];
				table->drop();

				Poco::File(metadata_path).remove();

				if (Poco::File(data_path).exists())
					Poco::File(data_path).remove();
			}

			/// Удаляем информацию о таблице из оперативки
			(*context.databases)[database_name].erase((*context.databases)[database_name].find(table_name));
		}
	}
	else
	{
		if (context.databases->end() != context.databases->find(database_name))
		{
			/// Удаление базы данных
			if (!drop.detach)
			{
				/// Удаление всех таблиц
				for (Tables::iterator it = (*context.databases)[database_name].begin(); it != (*context.databases)[database_name].end(); ++it)
					it->second->drop();

				Poco::File(metadata_path).remove(true);
				Poco::File(data_path).remove(true);
			}

			/// Удаляем информацию о БД из оперативки
			context.databases->erase(context.databases->find(database_name));
		}
	}
}


}
