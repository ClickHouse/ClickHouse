#include <Poco/File.h>
#include <Poco/Net/NetworkInterface.h>

#include <DB/Common/escapeForFileName.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/copyData.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Interpreters/Context.h>
#include <DB/Client/ConnectionPoolWithFailover.h>

namespace DB
{

String Context::getPath() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return shared->path;
}


void Context::setPath(const String & path)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	shared->path = path;
}


void Context::initUsersFromConfig()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	shared->users.initFromConfig();
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


void Context::initQuotasFromConfig()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	shared->quotas.initFromConfig();
}


void Context::setQuota(const String & name, const String & quota_key, const String & user_name, const Poco::Net::IPAddress & address)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	quota = &shared->quotas.get(name, quota_key, user_name, address);
}


QuotaForIntervals & Context::getQuota()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return *quota;
}

void Context::addDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	shared->view_dependencies[from].insert(where);
}

void Context::removeDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	shared->view_dependencies[from].erase(where);
}

Dependencies Context::getDependencies(const DatabaseAndTableName & from) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	ViewDependencies::const_iterator iter = shared->view_dependencies.find(from);
	if (iter == shared->view_dependencies.end())
		return Dependencies();
	const std::set<DatabaseAndTableName> &buf = iter->second;
	Dependencies res(buf.begin(), buf.end());
	return res;
}

bool Context::isTableExist(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	Databases::const_iterator it;
	return shared->databases.end() != (it = shared->databases.find(db))
		&& it->second.end() != it->second.find(table_name);
}


bool Context::isDatabaseExist(const String & database_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	String db = database_name.empty() ? current_database : database_name;
	return shared->databases.end() != shared->databases.find(db);
}


void Context::assertTableExists(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;
		
	Databases::const_iterator it;
	if (shared->databases.end() == (it = shared->databases.find(db)))
		throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

	if (it->second.end() == it->second.find(table_name))
		throw Exception("Table " + db + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
}


void Context::assertTableDoesntExist(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	Databases::const_iterator it;
	if (shared->databases.end() != (it = shared->databases.find(db))
		&& it->second.end() != it->second.find(table_name))
		throw Exception("Table " + db + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}


void Context::assertDatabaseExists(const String & database_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	if (shared->databases.end() == shared->databases.find(db))
		throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
}


void Context::assertDatabaseDoesntExist(const String & database_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	if (shared->databases.end() != shared->databases.find(db))
		throw Exception("Database " + db + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
}


StoragePtr Context::getTable(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	Databases::const_iterator it;
	if (shared->databases.end() == (it = shared->databases.find(db)))
		throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

	Tables::const_iterator jt;
	if (it->second.end() == (jt = it->second.find(table_name)))
		throw Exception("Table " + db + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

	return jt->second;
}

StoragePtr Context::tryGetTable(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	
	String db = database_name.empty() ? current_database : database_name;
	
	Databases::const_iterator it;
	if (shared->databases.end() == (it = shared->databases.find(db)))
		return StoragePtr();
	
	Tables::const_iterator jt;
	if (it->second.end() == (jt = it->second.find(table_name)))
		return StoragePtr();
	
	return jt->second;
}


void Context::addTable(const String & database_name, const String & table_name, StoragePtr table)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	
	String db = database_name.empty() ? current_database : database_name;

	assertDatabaseExists(db);
	assertTableDoesntExist(db, table_name);
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
	shared->databases.erase(shared->databases.find(db));
	shared->database_droppers.erase(db);
}


ASTPtr Context::getCreateQuery(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	assertDatabaseExists(db);
	assertTableExists(db, table_name);

	/// Здесь хранится определение таблицы
	String metadata_path = shared->path + "metadata/" + escapeForFileName(db) + "/" + escapeForFileName(table_name) + ".sql";
	
	if (!Poco::File(metadata_path).exists())
	{
		try
		{
			/// Если файл .sql не предусмотрен (например, для таблиц типа ChunkRef), то движок может сам предоставить запрос CREATE.
			return getTable(database_name, table_name)->getCustomCreateQuery(*this);
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

	const char * begin = query->data();
	const char * end = begin + query->size();
	const char * pos = begin;

	ParserCreateQuery parser;
	ASTPtr ast;
	String expected;
	bool parse_res = parser.parse(pos, end, ast, expected);

	/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
	if (!parse_res || (pos != end && *pos != ';'))
		throw Exception("Syntax error while parsing query from file " + metadata_path + ": failed at position "
			+ toString(pos - begin) + ": "
			+ std::string(pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - pos))
			+ ", expected " + (parse_res ? "end of query" : expected) + ".",
			DB::ErrorCodes::SYNTAX_ERROR);

	ASTCreateQuery & ast_create_query = dynamic_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = false;
	ast_create_query.database = db;
	ast_create_query.query_string = query;

	return ast;
}


DatabaseDropperPtr Context::getDatabaseDropper(const String & name)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	
	String db = name.empty() ? current_database : name;
	
	if (shared->databases.count(db) == 0)
		throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
	
	if (shared->database_droppers.count(db) == 0)
		shared->database_droppers[db] = DatabaseDropperPtr(new DatabaseDropper(shared->path +  "data/" + escapeForFileName(db)));
	
	return shared->database_droppers[db];
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
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	current_query_id = query_id;
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
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	
	if (!shared->dictionaries)
		shared->dictionaries = new Dictionaries;
	
	return *shared->dictionaries;
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


void Context::setUncompressedCache(size_t cache_size_in_cells)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (shared->uncompressed_cache)
		throw Exception("Uncompressed cache has been already created.", ErrorCodes::LOGICAL_ERROR);

	shared->uncompressed_cache = new UncompressedCache(cache_size_in_cells);
}


UncompressedCachePtr Context::getUncompressedCache() const
{
	/// Исходим из допущения, что функция setUncompressedCache, если вызывалась, то раньше. Иначе поставьте mutex.
	return shared->uncompressed_cache;
}

void Context::setMarkCache(size_t cache_size_in_bytes)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	if (shared->mark_cache)
		throw Exception("Uncompressed cache has been already created.", ErrorCodes::LOGICAL_ERROR);

	shared->mark_cache = new MarkCache(cache_size_in_bytes);
}

MarkCachePtr Context::getMarkCache() const
{
	return shared->mark_cache;
}

void Context::initClusters()
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	if (!shared->clusters)
	{
		shared->clusters = new Clusters(settings, shared->data_type_factory);
	}
}

Cluster & Context::getCluster(const std::string & cluster_name)
{
	if (!shared->clusters)
		throw Poco::Exception("Clusters have not been initialized yet.");

	Clusters::iterator it = shared->clusters->find(cluster_name);
	if (it != shared->clusters->end())
		return it->second;
	else
		throw Poco::Exception("Failed to find cluster with name = " + cluster_name);
}
}
