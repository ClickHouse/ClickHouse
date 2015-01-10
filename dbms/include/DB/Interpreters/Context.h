#pragma once

#include <map>
#include <set>

#include <Poco/SharedPtr.h>
#include <Poco/Mutex.h>

#include <Yandex/logger_useful.h>

#include <DB/Core/NamesAndTypes.h>
#include <DB/Common/Macros.h>
#include <DB/IO/UncompressedCache.h>
#include <DB/Storages/MarkCache.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/Storages/IStorage.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/Storages/StorageFactory.h>
#include <DB/Storages/MergeTree/BackgroundProcessingPool.h>
#include <DB/Storages/MergeTree/MergeList.h>
#include <DB/TableFunctions/TableFunctionFactory.h>
#include <DB/Interpreters/Settings.h>
#include <DB/Interpreters/Users.h>
#include <DB/Interpreters/Quota.h>
#include <DB/Interpreters/Dictionaries.h>
#include <DB/Interpreters/ProcessList.h>
#include <DB/Interpreters/Cluster.h>
#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/Interpreters/Compiler.h>
#include <DB/Client/ConnectionPool.h>
#include <statdaemons/ConfigProcessor.h>
#include <zkutil/ZooKeeper.h>


namespace DB
{

class TableFunctionFactory;

using Poco::SharedPtr;

/// имя таблицы -> таблица
typedef std::map<String, StoragePtr> Tables;

/// имя БД -> таблицы
typedef std::map<String, Tables> Databases;

/// (имя базы данных, имя таблицы)
typedef std::pair<String, String> DatabaseAndTableName;

/// Таблица -> множество таблиц-представлений, которые деляют SELECT из неё.
typedef std::map<DatabaseAndTableName, std::set<DatabaseAndTableName>> ViewDependencies;
typedef std::vector<DatabaseAndTableName> Dependencies;

/** Набор известных объектов, которые могут быть использованы в запросе.
  * Разделяемая часть. Порядок членов (порядок их уничтожения) очень важен.
  */
struct ContextShared
{
	Logger * log = &Logger::get("Context");					/// Логгер.

	struct AfterDestroy
	{
		Logger * log;

		AfterDestroy(Logger * log_) : log(log_) {}
		~AfterDestroy()
		{
#ifndef DBMS_CLIENT
			LOG_INFO(log, "Uninitialized shared context.");
#endif
		}
	} after_destroy {log};

	mutable Poco::Mutex mutex;								/// Для доступа и модификации разделяемых объектов.

	mutable zkutil::ZooKeeperPtr zookeeper;					/// Клиент для ZooKeeper.

	String interserver_io_host;								/// Имя хоста         по которым это сервер доступен для других серверов.
	int interserver_io_port;								///           и порт,

	String path;											/// Путь к директории с данными, со слешем на конце.
	String tmp_path;										/// Путь ко временным файлам, возникающим при обработке запроса.
	Databases databases;									/// Список БД и таблиц в них.
	TableFunctionFactory table_function_factory;			/// Табличные функции.
	AggregateFunctionFactory aggregate_function_factory; 	/// Агрегатные функции.
	DataTypeFactory data_type_factory;						/// Типы данных.
	StorageFactory storage_factory;							/// Движки таблиц.
	FormatFactory format_factory;							/// Форматы.
	mutable SharedPtr<Dictionaries> dictionaries;			/// Словари Метрики. Инициализируются лениво.
	Users users;											/// Известные пользователи.
	Quotas quotas;											/// Известные квоты на использование ресурсов.
	mutable UncompressedCachePtr uncompressed_cache;		/// Кэш разжатых блоков.
	mutable MarkCachePtr mark_cache;						/// Кэш засечек в сжатых файлах.
	ProcessList process_list;								/// Исполняющиеся в данный момент запросы.
	MergeList merge_list;									/// Список выполняемых мерджей (для (Replicated)?MergeTree)
	ViewDependencies view_dependencies;						/// Текущие зависимости
	ConfigurationPtr users_config;							/// Конфиг с секциями users, profiles и quotas.
	InterserverIOHandler interserver_io_handler;			/// Обработчик для межсерверной передачи данных.
	BackgroundProcessingPoolPtr background_pool;			/// Пул потоков для фоновой работы, выполняемой таблицами.
	Macros macros;											/// Подстановки из конфига.
	Compiler compiler { path + "build/", 1 };				/// Для динамической компиляции частей запроса, при необходимости.

	/// Кластеры для distributed таблиц
	/// Создаются при создании Distributed таблиц, так как нужно дождаться пока будут выставлены Settings
	Poco::SharedPtr<Clusters> clusters;

	bool shutdown_called = false;


	~ContextShared()
	{
#ifndef DBMS_CLIENT
		LOG_INFO(log, "Uninitializing shared context.");
#endif

		try
		{
			shutdown();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}


	/** Выполнить сложную работу по уничтожению объектов заранее.
	  */
	void shutdown()
	{
		if (shutdown_called)
			return;
		shutdown_called = true;

		/** В этот момент, некоторые таблицы могут иметь потоки,
		  *  которые модифицируют список таблиц, и блокируют наш mutex (см. StorageChunkMerger).
		  * Чтобы корректно их завершить, скопируем текущий список таблиц,
		  *  и попросим их всех закончить свою работу.
		  * Потом удалим все объекты с таблицами.
		  */

		Databases current_databases;

		{
			Poco::ScopedLock<Poco::Mutex> lock(mutex);
			current_databases = databases;
		}

		for (Databases::iterator it = current_databases.begin(); it != current_databases.end(); ++it)
			for (Tables::iterator jt = it->second.begin(); jt != it->second.end(); ++jt)
				jt->second->shutdown();

		{
			Poco::ScopedLock<Poco::Mutex> lock(mutex);
			databases.clear();
		}
	}
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
	Shared shared = new ContextShared;

	String user;						/// Текущий пользователь.
	Poco::Net::IPAddress ip_address;	/// IP-адрес, с которого задан запрос.
	QuotaForIntervalsPtr quota = new QuotaForIntervals;	/// Текущая квота. По-умолчанию - пустая квота, которая ничего не ограничивает.
	String current_database;			/// Текущая БД.
	String current_query_id;			/// Id текущего запроса.
	NamesAndTypesList columns;			/// Столбцы текущей обрабатываемой таблицы.
	Settings settings;					/// Настройки выполнения запроса.
	ProgressCallback progress_callback;	/// Колбек для отслеживания прогресса выполнения запроса.
	ProcessList::Element * process_list_elem = nullptr;	/// Для отслеживания общего количества потраченных на запрос ресурсов.

	String default_format;				/// Формат, используемый, если сервер сам форматирует данные, и если в запросе не задан FORMAT.
										/// То есть, используется в HTTP-интерфейсе. Может быть не задан - тогда используется некоторый глобальный формат по-умолчанию.
	Tables external_tables;				/// Временные таблицы.
	Context * session_context = nullptr;	/// Контекст сессии или nullptr, если его нет. (Возможно, равен this.)
	Context * global_context = nullptr;		/// Глобальный контекст или nullptr, если его нет. (Возможно, равен this.)

public:
	String getPath() const;
	String getTemporaryPath() const;
	void setPath(const String & path);
	void setTemporaryPath(const String & path);

	/** Забрать список пользователей, квот и профилей настроек из этого конфига.
	  * Список пользователей полностью заменяется.
	  * Накопленные значения у квоты не сбрасываются, если квота не удалена.
	  */
	void setUsersConfig(ConfigurationPtr config);

	ConfigurationPtr getUsersConfig();

	void setUser(const String & name, const String & password, const Poco::Net::IPAddress & address, const String & quota_key);
	String getUser() const { return user; }
	Poco::Net::IPAddress getIPAddress() const { return ip_address; }

	void setQuota(const String & name, const String & quota_key, const String & user_name, const Poco::Net::IPAddress & address);
	QuotaForIntervals & getQuota();

	void addDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where);
	void removeDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where);
	Dependencies getDependencies(const String & database_name, const String & table_name) const;

	/// Проверка существования таблицы/БД. database может быть пустой - в этом случае используется текущая БД.
	bool isTableExist(const String & database_name, const String & table_name) const;
	bool isDatabaseExist(const String & database_name) const;
	void assertTableExists(const String & database_name, const String & table_name) const;
	void assertTableDoesntExist(const String & database_name, const String & table_name) const;
	void assertDatabaseExists(const String & database_name) const;
	void assertDatabaseDoesntExist(const String & database_name) const;

	Tables getExternalTables() const;
	StoragePtr tryGetExternalTable(const String & table_name) const;
	StoragePtr getTable(const String & database_name, const String & table_name) const;
	StoragePtr tryGetTable(const String & database_name, const String & table_name) const;
	void addExternalTable(const String & table_name, StoragePtr storage);
	void addTable(const String & database_name, const String & table_name, StoragePtr table);
	void addDatabase(const String & database_name);

	/// Возвращает отцепленную таблицу.
	StoragePtr detachTable(const String & database_name, const String & table_name);

	void detachDatabase(const String & database_name);

	String getCurrentDatabase() const;
	String getCurrentQueryId() const;
	void setCurrentDatabase(const String & name);
	void setCurrentQueryId(const String & query_id);

	String getDefaultFormat() const;	/// Если default_format не задан - возвращается некоторый глобальный формат по-умолчанию.
	void setDefaultFormat(const String & name);

	const Macros & getMacros() const;
	void setMacros(Macros && macros);

	Settings getSettings() const;
	void setSettings(const Settings & settings_);

	Limits getLimits() const;

	/// Установить настройку по имени.
	void setSetting(const String & name, const Field & value);

	/// Установить настройку по имени. Прочитать значение в текстовом виде из строки (например, из конфига, или из параметра URL).
	void setSetting(const String & name, const std::string & value);

	const TableFunctionFactory & getTableFunctionFactory() const			{ return shared->table_function_factory; }
	const AggregateFunctionFactory & getAggregateFunctionFactory() const	{ return shared->aggregate_function_factory; }
	const DataTypeFactory & getDataTypeFactory() const						{ return shared->data_type_factory; }
	const StorageFactory & getStorageFactory() const						{ return shared->storage_factory; }
	const FormatFactory & getFormatFactory() const							{ return shared->format_factory; }
	const Dictionaries & getDictionaries() const;

	InterserverIOHandler & getInterserverIOHandler()						{ return shared->interserver_io_handler; }

	/// Как другие серверы могут обратиться к этому для скачивания реплицируемых данных.
	void setInterserverIOAddress(const String & host, UInt16 port);
	std::pair<String, UInt16> getInterserverIOAddress() const;

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

	/** Устанавливается в executeQuery и InterpreterSelectQuery. Затем используется в IProfilingBlockInputStream,
	  *  чтобы обновлять и контролировать информацию об общем количестве потраченных на запрос ресурсов.
	  */
	void setProcessListElement(ProcessList::Element * elem);
	/// Может вернуть nullptr, если запрос не был вставлен в ProcessList.
	ProcessList::Element * getProcessListElement();

	/// Список всех запросов.
	ProcessList & getProcessList()											{ return shared->process_list; }
	const ProcessList & getProcessList() const								{ return shared->process_list; }

	MergeList & getMergeList() { return shared->merge_list; }
	const MergeList & getMergeList() const { return shared->merge_list; }

	/// Создать кэш разжатых блоков указанного размера. Это можно сделать только один раз.
	void setUncompressedCache(size_t max_size_in_bytes);
	UncompressedCachePtr getUncompressedCache() const;

	void setZooKeeper(zkutil::ZooKeeperPtr zookeeper);
	/// Если в момент вызова текущая сессия просрочена, синхронно создает и возвращает новую вызовом startNewSession().
	zkutil::ZooKeeperPtr getZooKeeper() const;

	/// Создать кэш засечек указанного размера. Это можно сделать только один раз.
	void setMarkCache(size_t cache_size_in_bytes);
	MarkCachePtr getMarkCache() const;

	BackgroundProcessingPool & getBackgroundPool();

	/** Очистить кэши разжатых блоков и засечек.
	  * Обычно это делается при переименовании таблиц, изменении типа столбцов, удалении таблицы.
	  *  - так как кэши привязаны к именам файлов, и становятся некорректными.
	  *  (при удалении таблицы - нужно, так как на её месте может появиться другая)
	  * const - потому что изменение кэша не считается существенным.
	  */
	void resetCaches() const;

	void initClusters();
	Cluster & getCluster(const std::string & cluster_name);

	Compiler & getCompiler();

	void shutdown() { shared->shutdown(); }
};


}
