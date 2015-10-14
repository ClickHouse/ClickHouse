#include <map>
#include <set>
#include <chrono>

#include <Poco/SharedPtr.h>
#include <Poco/Mutex.h>
#include <Poco/File.h>
#include <Poco/UUIDGenerator.h>

#include <common/logger_useful.h>

#include <DB/Common/Macros.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/TableFunctions/TableFunctionFactory.h>
#include <DB/Storages/IStorage.h>
#include <DB/Storages/MarkCache.h>
#include <DB/Storages/MergeTree/BackgroundProcessingPool.h>
#include <DB/Storages/MergeTree/MergeList.h>
#include <DB/Storages/MergeTree/MergeTreeSettings.h>
#include <DB/Storages/CompressionMethodSelector.h>
#include <DB/Interpreters/Settings.h>
#include <DB/Interpreters/Users.h>
#include <DB/Interpreters/Quota.h>
#include <DB/Interpreters/Dictionaries.h>
#include <DB/Interpreters/ExternalDictionaries.h>
#include <DB/Interpreters/ProcessList.h>
#include <DB/Interpreters/Cluster.h>
#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/Interpreters/Compiler.h>
#include <DB/Interpreters/QueryLog.h>
#include <DB/Interpreters/Context.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/copyData.h>
#include <DB/IO/UncompressedCache.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/Client/ConnectionPoolWithFailover.h>

#include <DB/Common/ConfigProcessor.h>
#include <zkutil/ZooKeeper.h>


namespace DB
{


class TableFunctionFactory;

using Poco::SharedPtr;


/** Набор известных объектов, которые могут быть использованы в запросе.
  * Разделяемая часть. Порядок членов (порядок их уничтожения) очень важен.
  */
struct ContextShared
{
	Logger * log = &Logger::get("Context");					/// Логгер.

	mutable Poco::Mutex mutex;								/// Для доступа и модификации разделяемых объектов.
	mutable Poco::Mutex external_dictionaries_mutex;		/// Для доступа к внешним словарям. Отдельный мьютекс, чтобы избежать локов при обращении сервера к самому себе.

	mutable zkutil::ZooKeeperPtr zookeeper;					/// Клиент для ZooKeeper.

	String interserver_io_host;								/// Имя хоста         по которым это сервер доступен для других серверов.
	int interserver_io_port;								///           и порт,

	String path;											/// Путь к директории с данными, со слешем на конце.
	String tmp_path;										/// Путь ко временным файлам, возникающим при обработке запроса.
	Databases databases;									/// Список БД и таблиц в них.
	TableFunctionFactory table_function_factory;			/// Табличные функции.
	AggregateFunctionFactory aggregate_function_factory; 	/// Агрегатные функции.
	FormatFactory format_factory;							/// Форматы.
	mutable SharedPtr<Dictionaries> dictionaries;			/// Словари Метрики. Инициализируются лениво.
	mutable SharedPtr<ExternalDictionaries> external_dictionaries;
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
	std::unique_ptr<Compiler> compiler;						/// Для динамической компиляции частей запроса, при необходимости.
	std::unique_ptr<QueryLog> query_log;					/// Для логгирования запросов.
	mutable std::unique_ptr<CompressionMethodSelector> compression_method_selector; /// Правила для выбора метода сжатия в зависимости от размера куска.
	std::unique_ptr<MergeTreeSettings> merge_tree_settings;	/// Настройки для движка MergeTree.

	/// Кластеры для distributed таблиц
	/// Создаются при создании Distributed таблиц, так как нужно дождаться пока будут выставлены Settings
	Poco::SharedPtr<Clusters> clusters;

	Poco::UUIDGenerator uuid_generator;

	bool shutdown_called = false;


	~ContextShared()
	{
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


Context::Context()
	: shared(new ContextShared),
	quota(new QuotaForIntervals)
{
}

Context::~Context() = default;


const TableFunctionFactory & Context::getTableFunctionFactory() const			{ return shared->table_function_factory; }
const AggregateFunctionFactory & Context::getAggregateFunctionFactory() const	{ return shared->aggregate_function_factory; }
const FormatFactory & Context::getFormatFactory() const							{ return shared->format_factory; }
InterserverIOHandler & Context::getInterserverIOHandler()						{ return shared->interserver_io_handler; }
Poco::Mutex & Context::getMutex() const 										{ return shared->mutex; }
const Databases & Context::getDatabases() const 								{ return shared->databases; }
Databases & Context::getDatabases() 											{ return shared->databases; }
ProcessList & Context::getProcessList()											{ return shared->process_list; }
const ProcessList & Context::getProcessList() const								{ return shared->process_list; }
MergeList & Context::getMergeList() 											{ return shared->merge_list; }
const MergeList & Context::getMergeList() const 								{ return shared->merge_list; }


String Context::getPath() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return shared->path;
}

String Context::getTemporaryPath() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return shared->tmp_path;
}


void Context::setPath(const String & path)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	shared->path = path;
}

void Context::setTemporaryPath(const String & path)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	shared->tmp_path = path;
}


void Context::setUsersConfig(ConfigurationPtr config)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	shared->users_config = config;
	shared->users.loadFromConfig(*shared->users_config);
	shared->quotas.loadFromConfig(*shared->users_config);
}

ConfigurationPtr Context::getUsersConfig()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return shared->users_config;
}


void Context::setUser(const String & name, const String & password, const Poco::Net::IPAddress & address, const String & quota_key)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	const User & user_props = shared->users.get(name, password, address);
	setSetting("profile", user_props.profile);
	setQuota(user_props.quota, quota_key, name, address);

	user = name;
	ip_address = address;
}


void Context::setQuota(const String & name, const String & quota_key, const String & user_name, const Poco::Net::IPAddress & address)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	quota = shared->quotas.get(name, quota_key, user_name, address);
}


QuotaForIntervals & Context::getQuota()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return *quota;
}

void Context::checkDatabaseAccessRights(const std::string & database_name) const
{
	if (user.empty() || (database_name == "system"))
	{
		/// Безымянный пользователь, т.е. сервер, имеет доступ ко всем БД.
		/// Все пользователи имеют доступ к БД system.
		return;
	}
	if (!shared->users.isAllowedDatabase(user, database_name))
		throw Exception("Access denied to database " + database_name, ErrorCodes::DATABASE_ACCESS_DENIED);
}

void Context::addDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	checkDatabaseAccessRights(from.first);
	checkDatabaseAccessRights(where.first);
	shared->view_dependencies[from].insert(where);
}

void Context::removeDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	checkDatabaseAccessRights(from.first);
	checkDatabaseAccessRights(where.first);
	shared->view_dependencies[from].erase(where);
}

Dependencies Context::getDependencies(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;
	checkDatabaseAccessRights(db);

	ViewDependencies::const_iterator iter = shared->view_dependencies.find(DatabaseAndTableName(db, table_name));
	if (iter == shared->view_dependencies.end())
		return {};

	return Dependencies(iter->second.begin(), iter->second.end());
}

bool Context::isTableExist(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;
	checkDatabaseAccessRights(db);

	Databases::const_iterator it;
	return shared->databases.end() != (it = shared->databases.find(db))
		&& it->second.end() != it->second.find(table_name);
}


bool Context::isDatabaseExist(const String & database_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	String db = database_name.empty() ? current_database : database_name;
	checkDatabaseAccessRights(db);
	return shared->databases.end() != shared->databases.find(db);
}


void Context::assertTableExists(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;
	checkDatabaseAccessRights(db);

	Databases::const_iterator it = shared->databases.find(db);
	if (shared->databases.end() == it)
		throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

	if (it->second.end() == it->second.find(table_name))
		throw Exception("Table " + db + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
}


void Context::assertTableDoesntExist(const String & database_name, const String & table_name, bool check_database_access_rights) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;
	if (check_database_access_rights)
		checkDatabaseAccessRights(db);

	Databases::const_iterator it;
	if (shared->databases.end() != (it = shared->databases.find(db))
		&& it->second.end() != it->second.find(table_name))
		throw Exception("Table " + db + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}


void Context::assertDatabaseExists(const String & database_name, bool check_database_access_rights) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;
	if (check_database_access_rights)
		checkDatabaseAccessRights(db);

	if (shared->databases.end() == shared->databases.find(db))
		throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
}


void Context::assertDatabaseDoesntExist(const String & database_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;
	checkDatabaseAccessRights(db);

	if (shared->databases.end() != shared->databases.find(db))
		throw Exception("Database " + db + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
}


Tables Context::getExternalTables() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	Tables res = external_tables;
	if (session_context && session_context != this)
	{
		Tables buf = session_context->getExternalTables();
		res.insert(buf.begin(), buf.end());
	}
	else if (global_context && global_context != this)
	{
		Tables buf = global_context->getExternalTables();
		res.insert(buf.begin(), buf.end());
	}
	return res;
}


StoragePtr Context::tryGetExternalTable(const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	Tables::const_iterator jt = external_tables.find(table_name);
	if (external_tables.end() == jt)
		return StoragePtr();

	return jt->second;
}


StoragePtr Context::getTable(const String & database_name, const String & table_name) const
{
	Exception exc;
	auto res = getTableImpl(database_name, table_name, &exc);
	if (!res)
		throw exc;
	return res;
}


StoragePtr Context::tryGetTable(const String & database_name, const String & table_name) const
{
	return getTableImpl(database_name, table_name, nullptr);
}


StoragePtr Context::getTableImpl(const String & database_name, const String & table_name, Exception * exception) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	/** Возможность обратиться к временным таблицам другого запроса в виде _query_QUERY_ID.table
	  * NOTE В дальнейшем может потребоваться подумать об изоляции.
	  */
	if (database_name.size() > strlen("_query_")
		&& database_name.compare(0, strlen("_query_"), "_query_") == 0)
	{
		String requested_query_id = database_name.substr(strlen("_query_"));

		auto res = shared->process_list.tryGetTemporaryTable(requested_query_id, table_name);

		if (!res && exception)
			*exception = Exception(
				"Cannot find temporary table with name " + table_name + " for query with id " + requested_query_id, ErrorCodes::UNKNOWN_TABLE);

		return res;
	}

	if (database_name.empty())
	{
		StoragePtr res = tryGetExternalTable(table_name);
		if (res)
			return res;
	}

	String db = database_name.empty() ? current_database : database_name;
	checkDatabaseAccessRights(db);

	Databases::const_iterator it = shared->databases.find(db);
	if (shared->databases.end() == it)
	{
		if (exception)
			*exception = Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
		return {};
	}

	Tables::const_iterator jt = it->second.find(table_name);
	if (it->second.end() == jt)
	{
		if (exception)
			*exception = Exception("Table " + db + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
		return {};
	}

	if (!jt->second)
		throw Exception("Logical error: entry for table " + db + "." + table_name + " exists in Context but it is nullptr.", ErrorCodes::LOGICAL_ERROR);

	return jt->second;
}


void Context::addExternalTable(const String & table_name, StoragePtr storage)
{
	if (external_tables.end() != external_tables.find(table_name))
		throw Exception("Temporary table " + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

	external_tables[table_name] = storage;

	if (process_list_elem)
	{
		Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
		shared->process_list.addTemporaryTable(*process_list_elem, table_name, storage);
	}
}


void Context::addTable(const String & database_name, const String & table_name, StoragePtr table)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;
	checkDatabaseAccessRights(db);

	assertDatabaseExists(db, false);
	assertTableDoesntExist(db, table_name, false);
	shared->databases[db][table_name] = table;
}


void Context::addDatabase(const String & database_name)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	assertDatabaseDoesntExist(db);
	shared->databases[db];
}


StoragePtr Context::detachTable(const String & database_name, const String & table_name)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	assertTableExists(db, table_name);
	Tables::iterator it = shared->databases[db].find(table_name);
	StoragePtr res = it->second;
	shared->databases[db].erase(it);
	return res;
}


void Context::detachDatabase(const String & database_name)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	assertDatabaseExists(db);
	shared->databases.erase(db);
}


ASTPtr Context::getCreateQuery(const String & database_name, const String & table_name) const
{
	StoragePtr table;
	String db;

	{
		Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
		db = database_name.empty() ? current_database : database_name;
		table = getTable(db, table_name);
	}

	auto table_lock = table->lockStructure(false);

	/// Здесь хранится определение таблицы
	String metadata_path = shared->path + "metadata/" + escapeForFileName(db) + "/" + escapeForFileName(table_name) + ".sql";

	if (!Poco::File(metadata_path).exists())
	{
		try
		{
			/// Если файл .sql не предусмотрен (например, для таблиц типа ChunkRef), то движок может сам предоставить запрос CREATE.
			return table->getCustomCreateQuery(*this);
		}
		catch (...)
		{
			throw Exception("Metadata file " + metadata_path + " for table " + db + "." + table_name + " doesn't exist.",
				ErrorCodes::TABLE_METADATA_DOESNT_EXIST);
		}
	}

	StringPtr query = new String();
	{
		ReadBufferFromFile in(metadata_path);
		WriteBufferFromString out(*query);
		copyData(in, out);
	}

	ParserCreateQuery parser;
	ASTPtr ast = parseQuery(parser, query->data(), query->data() + query->size(), "in file " + metadata_path);

	ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = false;
	ast_create_query.database = db;
	ast_create_query.query_string = query;

	return ast;
}


Settings Context::getSettings() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return settings;
}


Limits Context::getLimits() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return settings.limits;
}


void Context::setSettings(const Settings & settings_)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	settings = settings_;
}


void Context::setSetting(const String & name, const Field & value)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	if (name == "profile")
		settings.setProfile(value.safeGet<String>(), *shared->users_config);
	else
		settings.set(name, value);
}


void Context::setSetting(const String & name, const std::string & value)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	if (name == "profile")
		settings.setProfile(value, *shared->users_config);
	else
		settings.set(name, value);
}


String Context::getCurrentDatabase() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return current_database;
}


String Context::getCurrentQueryId() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return current_query_id;
}


void Context::setCurrentDatabase(const String & name)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	assertDatabaseExists(name);
	current_database = name;
}


void Context::setCurrentQueryId(const String & query_id)
{
	if (!current_query_id.empty())
		throw Exception("Logical error: attempt to set query_id twice", ErrorCodes::LOGICAL_ERROR);

	String query_id_to_set = query_id;
	if (query_id_to_set.empty())	/// Если пользователь не передал свой query_id, то генерируем его самостоятельно.
		query_id_to_set = shared->uuid_generator.createRandom().toString();

	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	current_query_id = query_id_to_set;
}


String Context::getDefaultFormat() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return default_format.empty() ? "TabSeparated" : default_format;
}


void Context::setDefaultFormat(const String & name)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	default_format = name;
}

const Macros& Context::getMacros() const
{
	return shared->macros;
}

void Context::setMacros(Macros && macros)
{
	/// Полагаемся, что это присваивание происходит один раз при старте сервера. Если это не так, нужно использовать мьютекс.
	shared->macros = macros;
}


Context & Context::getSessionContext()
{
	if (!session_context)
		throw Exception("There is no session", ErrorCodes::THERE_IS_NO_SESSION);
	return *session_context;
}


Context & Context::getGlobalContext()
{
	if (!global_context)
		throw Exception("Logical error: there is no global context", ErrorCodes::LOGICAL_ERROR);
	return *global_context;
}


const Dictionaries & Context::getDictionaries() const
{
	return getDictionariesImpl(false);
}


const ExternalDictionaries & Context::getExternalDictionaries() const
{
	return getExternalDictionariesImpl(false);
}


const Dictionaries & Context::getDictionariesImpl(const bool throw_on_error) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (!shared->dictionaries)
		shared->dictionaries = new Dictionaries{throw_on_error};

	return *shared->dictionaries;
}


const ExternalDictionaries & Context::getExternalDictionariesImpl(const bool throw_on_error) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->external_dictionaries_mutex);

	if (!shared->external_dictionaries)
	{
		if (!this->global_context)
			throw Exception("Logical error: there is no global context", ErrorCodes::LOGICAL_ERROR);
		shared->external_dictionaries = new ExternalDictionaries{*this->global_context, throw_on_error};
	}

	return *shared->external_dictionaries;
}


void Context::tryCreateDictionaries() const
{
	static_cast<void>(getDictionariesImpl(true));
}


void Context::tryCreateExternalDictionaries() const
{
	static_cast<void>(getExternalDictionariesImpl(true));
}


void Context::setProgressCallback(ProgressCallback callback)
{
	/// Колбек устанавливается на сессию или на запрос. В сессии одновременно обрабатывается только один запрос. Поэтому блокировка не нужна.
	progress_callback = callback;
}

ProgressCallback Context::getProgressCallback() const
{
	return progress_callback;
}


void Context::setProcessListElement(ProcessList::Element * elem)
{
	/// Устанавливается на сессию или на запрос. В сессии одновременно обрабатывается только один запрос. Поэтому блокировка не нужна.
	process_list_elem = elem;
}

ProcessList::Element * Context::getProcessListElement()
{
	return process_list_elem;
}


void Context::setUncompressedCache(size_t max_size_in_bytes)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (shared->uncompressed_cache)
		throw Exception("Uncompressed cache has been already created.", ErrorCodes::LOGICAL_ERROR);

	shared->uncompressed_cache.reset(new UncompressedCache(max_size_in_bytes));
}


UncompressedCachePtr Context::getUncompressedCache() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return shared->uncompressed_cache;
}

void Context::setMarkCache(size_t cache_size_in_bytes)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (shared->mark_cache)
		throw Exception("Uncompressed cache has been already created.", ErrorCodes::LOGICAL_ERROR);

	shared->mark_cache.reset(new MarkCache(cache_size_in_bytes, std::chrono::seconds(settings.mark_cache_min_lifetime)));
}

MarkCachePtr Context::getMarkCache() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return shared->mark_cache;
}

BackgroundProcessingPool & Context::getBackgroundPool()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	if (!shared->background_pool)
		shared->background_pool = new BackgroundProcessingPool(settings.background_pool_size);
	return *shared->background_pool;
}

void Context::resetCaches() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (shared->uncompressed_cache)
		shared->uncompressed_cache->reset();

	if (shared->mark_cache)
		shared->mark_cache->reset();
}

void Context::setZooKeeper(zkutil::ZooKeeperPtr zookeeper)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (shared->zookeeper)
		throw Exception("ZooKeeper client has already been set.", ErrorCodes::LOGICAL_ERROR);

	shared->zookeeper = zookeeper;
}

zkutil::ZooKeeperPtr Context::getZooKeeper() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (shared->zookeeper && shared->zookeeper->expired())
		shared->zookeeper = shared->zookeeper->startNewSession();

	return shared->zookeeper;
}


void Context::setInterserverIOAddress(const String & host, UInt16 port)
{
	shared->interserver_io_host = host;
	shared->interserver_io_port = port;
}


std::pair<String, UInt16> Context::getInterserverIOAddress() const
{
	if (shared->interserver_io_host.empty() || shared->interserver_io_port == 0)
		throw Exception("Parameter 'interserver_http_port' required for replication is not specified in configuration file.",
			ErrorCodes::NO_ELEMENTS_IN_CONFIG);

	return { shared->interserver_io_host, shared->interserver_io_port };
}

UInt16 Context::getTCPPort() const
{
	return Poco::Util::Application::instance().config().getInt("tcp_port");
}


void Context::initClusters()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	if (!shared->clusters)
		shared->clusters = new Clusters(settings);
}

Cluster & Context::getCluster(const std::string & cluster_name)
{
	if (!shared->clusters)
		throw Poco::Exception("Clusters have not been initialized yet.");

	Clusters::Impl::iterator it = shared->clusters->impl.find(cluster_name);
	if (it != shared->clusters->impl.end())
		return it->second;
	else
		throw Poco::Exception("Failed to find cluster with name = " + cluster_name);
}

Poco::SharedPtr<Clusters> Context::getClusters() const
{
	if (!shared->clusters)
		throw Poco::Exception("Clusters have not been initialized yet.");
	return shared->clusters;
}

Compiler & Context::getCompiler()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (!shared->compiler)
		shared->compiler.reset(new Compiler{ shared->path + "build/", 1 });

	return *shared->compiler;
}


QueryLog & Context::getQueryLog()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (!shared->query_log)
	{
		auto & config = Poco::Util::Application::instance().config();

		String database = config.getString("query_log.database", "system");
		String table = config.getString("query_log.table", "query_log");
		size_t flush_interval_milliseconds = parse<size_t>(
			config.getString("query_log.flush_interval_milliseconds", DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS_STR));

		shared->query_log.reset(new QueryLog{ *this, database, table, flush_interval_milliseconds });
	}

	return *shared->query_log;
}


CompressionMethod Context::chooseCompressionMethod(size_t part_size, double part_size_ratio) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (!shared->compression_method_selector)
	{
		constexpr auto config_name = "compression";
		auto & config = Poco::Util::Application::instance().config();

		if (config.has(config_name))
			shared->compression_method_selector.reset(new CompressionMethodSelector{config, "compression"});
		else
			shared->compression_method_selector.reset(new CompressionMethodSelector);
	}

	return shared->compression_method_selector->choose(part_size, part_size_ratio);
}


const MergeTreeSettings & Context::getMergeTreeSettings()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (!shared->merge_tree_settings)
	{
		auto & config = Poco::Util::Application::instance().config();
		shared->merge_tree_settings.reset(new MergeTreeSettings());
		shared->merge_tree_settings->loadFromConfig("merge_tree", config);
	}

	return *shared->merge_tree_settings;
}


void Context::shutdown()
{
	shared->shutdown();
}

}
