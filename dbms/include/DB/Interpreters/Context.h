#pragma once

#include <map>
#include <set>

#include <Poco/SharedPtr.h>
#include <Poco/Mutex.h>

#include <DB/Core/NamesAndTypes.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/Storages/IStorage.h>
#include <DB/Functions/FunctionsLibrary.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/Storages/StorageFactory.h>
#include <DB/Interpreters/Settings.h>


namespace DB
{

using Poco::SharedPtr;

/// имя таблицы -> таблица
typedef std::map<String, StoragePtr> Tables;

/// имя БД -> таблицы
typedef std::map<String, Tables> Databases;


/** Набор известных объектов, которые могут быть использованы в запросе.
  */
struct Context
{
	String path;											/// Путь к директории с данными, со слешем на конце.
	SharedPtr<Databases> databases;							/// Список БД и таблиц в них.
	String current_database;								/// Текущая БД.
	SharedPtr<Functions> functions;							/// Обычные функции.
	AggregateFunctionFactoryPtr aggregate_function_factory; /// Агрегатные функции.
	DataTypeFactoryPtr data_type_factory;					/// Типы данных.
	StorageFactoryPtr storage_factory;						/// Движки таблиц.
	FormatFactoryPtr format_factory;						/// Форматы.
	NamesAndTypesList columns;								/// Столбцы текущей обрабатываемой таблицы.
	Settings settings;										/// Настройки выполнения запроса.
	Logger * log;											/// Логгер.

	Context * session_context;								/// Контекст сессии или NULL, если его нет. (Возможно, равен this.)
	Context * global_context;								/// Глобальный контекст или NULL, если его нет. (Возможно, равен this.)

	mutable SharedPtr<Poco::Mutex> mutex;					/// Для доступа и модификации разделяемых объектов.

	Context() : databases(new Databases), functions(new Functions),
		log(&Logger::get("Context")),
		session_context(NULL), global_context(NULL),
		mutex(new Poco::Mutex) {}

	/** В сервере есть глобальный контекст.
	  * При соединении, он копируется в контекст сессии.
	  * Для каждого запроса, контекст сессии копируется в контекст запроса.
	  * Блокировка нужна, так как запрос может модифицировать глобальный контекст (SET GLOBAL ...).
	  */
	Context(const Context & rhs)
	{
		Poco::ScopedLock<Poco::Mutex> lock(*rhs.mutex);

		path 						= rhs.path;
		databases 					= rhs.databases;
		current_database 			= rhs.current_database;
		functions 					= rhs.functions;
		aggregate_function_factory 	= rhs.aggregate_function_factory;
		data_type_factory			= rhs.data_type_factory;
		storage_factory				= rhs.storage_factory;
		format_factory				= rhs.format_factory;
		columns						= rhs.columns;
		settings					= rhs.settings;
		log							= rhs.log;
		mutex						= rhs.mutex;
		session_context				= rhs.session_context;
		global_context				= rhs.global_context;
	}


	/// Проверка существования таблицы. database может быть пустой - в этом случае используется текущая БД.
	bool isTableExist(const String & database_name, const String & table_name)
	{
		Poco::ScopedLock<Poco::Mutex> lock(*mutex);

		String db = database_name.empty() ? current_database : database_name;

		return databases->end() == databases->find(db)
			|| (*databases)[db].end() == (*databases)[db].find(table_name);
	}


	void assertTableExists(const String & database_name, const String & table_name)
	{
		Poco::ScopedLock<Poco::Mutex> lock(*mutex);

		String db = database_name.empty() ? current_database : database_name;

		if (databases->end() == databases->find(db))
			throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

		if ((*databases)[db].end() == (*databases)[db].find(table_name))
			throw Exception("Table " + db + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
	}


	void assertTableDoesntExist(const String & database_name, const String & table_name)
	{
		Poco::ScopedLock<Poco::Mutex> lock(*mutex);

		String db = database_name.empty() ? current_database : database_name;

		if (databases->end() != databases->find(db)
			&& (*databases)[db].end() != (*databases)[db].find(table_name))
			throw Exception("Table " + db + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
	}


	void assertDatabaseExists(const String & database_name)
	{
		Poco::ScopedLock<Poco::Mutex> lock(*mutex);

		String db = database_name.empty() ? current_database : database_name;

		if (databases->end() == databases->find(db))
			throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
	}
};


}
