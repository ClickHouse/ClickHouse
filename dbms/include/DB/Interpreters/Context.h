#pragma once

#include <map>
#include <set>

#include <Poco/SharedPtr.h>
#include <Poco/Mutex.h>

#include <Yandex/logger_useful.h>

#include <DB/Core/NamesAndTypes.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/Storages/IStorage.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/Storages/StorageFactory.h>
#include <DB/Interpreters/Settings.h>
#include <DB/Interpreters/Users.h>
#include <DB/Interpreters/Quota.h>
#include <DB/Interpreters/Dictionaries.h>


namespace DB
{

using Poco::SharedPtr;

/// имя таблицы -> таблица
typedef std::map<String, StoragePtr> Tables;

/// имя БД -> таблицы
typedef std::map<String, Tables> Databases;

/// имя БД -> dropper
typedef std::map<String, DatabaseDropperPtr> DatabaseDroppers;


/** Набор известных объектов, которые могут быть использованы в запросе.
  * Разделяемая часть.
  */
struct ContextShared
{
	String path;											/// Путь к директории с данными, со слешем на конце.
	Databases databases;									/// Список БД и таблиц в них.
	DatabaseDroppers database_droppers;						/// Reference counter'ы для ленивого удаления БД.
	FunctionFactory function_factory;						/// Обычные функции.
	AggregateFunctionFactory aggregate_function_factory; 	/// Агрегатные функции.
	DataTypeFactory data_type_factory;						/// Типы данных.
	StorageFactory storage_factory;							/// Движки таблиц.
	FormatFactory format_factory;							/// Форматы.
	mutable SharedPtr<Dictionaries> dictionaries;			/// Словари Метрики. Инициализируются лениво.
	Users users;											/// Известные пользователи.
	Quotas quotas;											/// Известные квоты на использование ресурсов.
	Logger * log;											/// Логгер.

	mutable Poco::Mutex mutex;								/// Для доступа и модификации разделяемых объектов.

	ContextShared() : log(&Logger::get("Context")) {};
};


/** Набор известных объектов, которые могут быть использованы в запросе.
  * Состоит из разделяемой части (всегда общей для всех сессий и запросов)
  *  и копируемой части (которая может быть своей для каждой сессии или запроса).
  *
  * Всё инкапсулировано для всяких проверок и блокировок.
  */
class Context
{
private:
	typedef SharedPtr<ContextShared> Shared;
	Shared shared;

	String user;						/// Текущий пользователь.
	QuotaForIntervals * quota;			/// Текущая квота.
	String current_database;			/// Текущая БД.
	NamesAndTypesList columns;			/// Столбцы текущей обрабатываемой таблицы.
	Settings settings;					/// Настройки выполнения запроса.
	ProgressCallback progress_callback;	/// Колбек для отслеживания прогресса выполнения запроса.

	String default_format;				/// Формат, используемый, если сервер сам форматирует данные, и если в запросе не задан FORMAT.
										/// То есть, используется в HTTP-интерфейсе. Может быть не задан - тогда используется некоторый глобальный формат по-умолчанию.
	
	Context * session_context;			/// Контекст сессии или NULL, если его нет. (Возможно, равен this.)
	Context * global_context;			/// Глобальный контекст или NULL, если его нет. (Возможно, равен this.)

public:
	Context() : shared(new ContextShared), quota(NULL), session_context(NULL), global_context(NULL) {}

	String getPath() const;
	void setPath(const String & path);

	void initUsersFromConfig();
	void setUser(const String & name, const String & password, const Poco::Net::IPAddress & address, const String & quota_key);

	void initQuotasFromConfig();
	void setQuota(const String & name, const String & quota_key, const Poco::Net::IPAddress & address);
	QuotaForIntervals & getQuota();

	/// Проверка существования таблицы/БД. database может быть пустой - в этом случае используется текущая БД.
	bool isTableExist(const String & database_name, const String & table_name) const;
	bool isDatabaseExist(const String & database_name) const;
	void assertTableExists(const String & database_name, const String & table_name) const;
	void assertTableDoesntExist(const String & database_name, const String & table_name) const;
	void assertDatabaseExists(const String & database_name) const;
	void assertDatabaseDoesntExist(const String & database_name) const;

	StoragePtr getTable(const String & database_name, const String & table_name) const;
	StoragePtr tryGetTable(const String & database_name, const String & table_name) const;
	void addTable(const String & database_name, const String & table_name, StoragePtr table);
	void addDatabase(const String & database_name);

	void detachTable(const String & database_name, const String & table_name);
	void detachDatabase(const String & database_name);

	String getCurrentDatabase() const;
	void setCurrentDatabase(const String & name);

	String getDefaultFormat() const;	/// Если default_format не задан - возвращается некоторый глобальный формат по-умолчанию.
	void setDefaultFormat(const String & name);

	DatabaseDropperPtr getDatabaseDropper(const String & name);
	
	Settings getSettings() const;
	void setSettings(const Settings & settings_);

	Limits getLimits() const;

	/// Установить настройку по имени.
	void setSetting(const String & name, const Field & value);
	
	const FunctionFactory & getFunctionFactory() const						{ return shared->function_factory; }
	const AggregateFunctionFactory & getAggregateFunctionFactory() const	{ return shared->aggregate_function_factory; }
	const DataTypeFactory & getDataTypeFactory() const						{ return shared->data_type_factory; }
	const StorageFactory & getStorageFactory() const						{ return shared->storage_factory; }
	const FormatFactory & getFormatFactory() const							{ return shared->format_factory; }
	const Dictionaries & getDictionaries() const;

	/// Получить запрос на CREATE таблицы.
	ASTPtr getCreateQuery(const String & database_name, const String & table_name) const;

	/// Для методов ниже может быть необходимо захватывать mutex самостоятельно.
	Poco::Mutex & getMutex() const 											{ return shared->mutex; }

	/// Метод getDatabases не потокобезопасен. При работе со списком БД и таблиц, вы должны захватить mutex.
	const Databases & getDatabases() const 									{ return shared->databases; }
	Databases & getDatabases() 												{ return shared->databases; }

	/// При работе со списком столбцов, используйте локальный контекст, чтобы никто больше его не менял.
	const NamesAndTypesList & getColumns() const							{ return columns; }
	NamesAndTypesList & getColumns()										{ return columns; }
	void setColumns(const NamesAndTypesList & columns_)						{ columns = columns_; }

	Context & getSessionContext();
	Context & getGlobalContext();

	void setSessionContext(Context & context_)								{ session_context = &context_; }
	void setGlobalContext(Context & context_)								{ global_context = &context_; }

	const Settings & getSettingsRef() const { return settings; };
	Settings & getSettingsRef() { return settings; };

	void setProgressCallback(ProgressCallback callback);
	/// Используется в InterpreterSelectQuery, чтобы передать его в IProfilingBlockInputStream.
	ProgressCallback getProgressCallback() const;
};


}
