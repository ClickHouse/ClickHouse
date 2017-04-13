#include <map>
#include <set>
#include <chrono>
#include <random>

#include <Poco/Mutex.h>
#include <Poco/File.h>
#include <Poco/UUID.h>
#include <Poco/Net/IPAddress.h>

#include <common/logger_useful.h>

#include <Common/Macros.h>
#include <Common/escapeForFileName.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <DataStreams/FormatFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/IStorage.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/MergeTree/ReshardingWorker.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/CompressionMethodSelector.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Users.h>
#include <Interpreters/Quota.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Interpreters/Compiler.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/UncompressedCache.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Databases/IDatabase.h>

#include <Common/ConfigProcessor.h>
#include <zkutil/ZooKeeper.h>


namespace ProfileEvents
{
    extern const Event ContextLock;
}

namespace CurrentMetrics
{
    extern const Metric ContextLockWait;
    extern const Metric MemoryTrackingForMerges;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int DATABASE_ACCESS_DENIED;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int TABLE_METADATA_DOESNT_EXIST;
    extern const int THERE_IS_NO_SESSION;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int DDL_GUARD_IS_ACTIVE;
    extern const int TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT;
}

class TableFunctionFactory;


/** Set of known objects (environment), that could be used in query.
  * Shared (global) part. Order of members (especially, order of destruction) is very important.
  */
struct ContextShared
{
    Logger * log = &Logger::get("Context");

    /// For access of most of shared objects. Recursive mutex.
    mutable Poco::Mutex mutex;
    /// Separate mutex for access of dictionaries. Separate mutex to avoid locks when server doing request to itself.
    mutable std::mutex embedded_dictionaries_mutex;
    mutable std::mutex external_dictionaries_mutex;
    /// Separate mutex for re-initialization of zookeer session. This operation could take a long time and must not interfere with another operations.
    mutable std::mutex zookeeper_mutex;

    mutable zkutil::ZooKeeperPtr zookeeper;                 /// Client for ZooKeeper.

    String interserver_io_host;                             /// The host name by which this server is available for other servers.
    int interserver_io_port;                                /// and port,

    String path;                                            /// Path to the data directory, with a slash at the end.
    String tmp_path;                                        /// The path to the temporary files that occur when processing the request.
    String flags_path;                                        ///
    Databases databases;                                    /// List of databases and tables in them.
    TableFunctionFactory table_function_factory;            /// Table functions.
    FormatFactory format_factory;                           /// Formats.
    mutable std::shared_ptr<EmbeddedDictionaries> embedded_dictionaries;    /// Metrica's dictionaeis. Have lazy initialization.
    mutable std::shared_ptr<ExternalDictionaries> external_dictionaries;
    String default_profile_name;                            /// Default profile name used for default values.
    Users users;                                            /// Known users.
    Quotas quotas;                                          /// Known quotas for resource use.
    mutable UncompressedCachePtr uncompressed_cache;        /// The cache of decompressed blocks.
    mutable MarkCachePtr mark_cache;                        /// Cache of marks in compressed files.
    ProcessList process_list;                               /// Executing queries at the moment.
    MergeList merge_list;                                   /// The list of executable merge (for (Replicated)?MergeTree)
    ViewDependencies view_dependencies;                     /// Current dependencies
    ConfigurationPtr users_config;                          /// Config with the users, profiles and quotas sections.
    InterserverIOHandler interserver_io_handler;            /// Handler for interserver communication.
    BackgroundProcessingPoolPtr background_pool;            /// The thread pool for the background work performed by the tables.
    ReshardingWorkerPtr resharding_worker;
    Macros macros;                                            /// Substitutions extracted from config.
    std::unique_ptr<Compiler> compiler;                        /// Used for dynamic compilation of queries' parts if it necessary.
    std::unique_ptr<QueryLog> query_log;                    /// Used to log queries.
    std::shared_ptr<PartLog> part_log;                        /// Used to log operations with parts
    std::shared_ptr<DDLWorker> ddl_worker;                    /// Process ddl commands from zk.
    /// Rules for selecting the compression method, depending on the size of the part.
    mutable std::unique_ptr<CompressionMethodSelector> compression_method_selector;
    std::unique_ptr<MergeTreeSettings> merge_tree_settings;    /// Settings of MergeTree* engines.
    size_t max_table_size_to_drop = 50000000000lu;            /// Protects MergeTree tables from accidental DROP (50GB by default)

    /// Clusters for distributed tables
    /// Initialized on demand (on distributed storages initialization) since Settings should be initialized
    std::unique_ptr<Clusters> clusters;
    ConfigurationPtr clusters_config;                        /// Soteres updated configs
    mutable std::mutex clusters_mutex;                        /// Guards clusters and clusters_config

    bool shutdown_called = false;

    /// Do not allow simultaneous execution of DDL requests on the same table.
    /// database -> table -> exception_message
    /// For the duration of the operation, an element is placed here, and an object is returned, which deletes the element in the destructor.
    /// In case the element already exists, an exception is thrown. See class DDLGuard below.
    using DDLGuards = std::unordered_map<String, DDLGuard::Map>;
    DDLGuards ddl_guards;
    /// If you capture mutex and ddl_guards_mutex, then you need to grab them strictly in this order.
    mutable std::mutex ddl_guards_mutex;

    Stopwatch uptime_watch;

    Context::ApplicationType application_type = Context::ApplicationType::SERVER;

    std::mt19937_64 rng{randomSeed()};


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


    /** Perform a complex job of destroying objects in advance.
      */
    void shutdown()
    {
        if (shutdown_called)
            return;
        shutdown_called = true;

        query_log.reset();
        part_log.reset();

        /** At this point, some tables may have threads that block our mutex.
          * To complete them correctly, we will copy the current list of tables,
          *  and ask them all to finish their work.
          * Then delete all objects with tables.
          */

        Databases current_databases;

        {
            Poco::ScopedLock<Poco::Mutex> lock(mutex);
            current_databases = databases;
        }

        for (auto & database : current_databases)
            database.second->shutdown();

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


const TableFunctionFactory & Context::getTableFunctionFactory() const            { return shared->table_function_factory; }
InterserverIOHandler & Context::getInterserverIOHandler()                        { return shared->interserver_io_handler; }

std::unique_lock<Poco::Mutex> Context::getLock() const
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    return std::unique_lock<Poco::Mutex>(shared->mutex);
}

ProcessList & Context::getProcessList()                                            { return shared->process_list; }
const ProcessList & Context::getProcessList() const                                { return shared->process_list; }
MergeList & Context::getMergeList()                                             { return shared->merge_list; }
const MergeList & Context::getMergeList() const                                 { return shared->merge_list; }


const Databases Context::getDatabases() const
{
    auto lock = getLock();
    return shared->databases;
}

Databases Context::getDatabases()
{
    auto lock = getLock();
    return shared->databases;
}


static String resolveDatabase(const String & database_name, const String & current_database)
{
    String res = database_name.empty() ? current_database : database_name;
    if (res.empty())
        throw Exception("Default database is not selected", ErrorCodes::UNKNOWN_DATABASE);
    return res;
}


const DatabasePtr Context::getDatabase(const String & database_name) const
{
    auto lock = getLock();
    String db = resolveDatabase(database_name, current_database);
    assertDatabaseExists(db);
    return shared->databases[db];
}

DatabasePtr Context::getDatabase(const String & database_name)
{
    auto lock = getLock();
    String db = resolveDatabase(database_name, current_database);
    assertDatabaseExists(db);
    return shared->databases[db];
}

const DatabasePtr Context::tryGetDatabase(const String & database_name) const
{
    auto lock = getLock();
    String db = resolveDatabase(database_name, current_database);
    auto it = shared->databases.find(db);
    if (it == shared->databases.end())
        return {};
    return it->second;
}

DatabasePtr Context::tryGetDatabase(const String & database_name)
{
    auto lock = getLock();
    String db = resolveDatabase(database_name, current_database);
    auto it = shared->databases.find(db);
    if (it == shared->databases.end())
        return {};
    return it->second;
}


String Context::getPath() const
{
    auto lock = getLock();
    return shared->path;
}

String Context::getTemporaryPath() const
{
    auto lock = getLock();
    return shared->tmp_path;
}

String Context::getFlagsPath() const
{
    auto lock = getLock();
    if (!shared->flags_path.empty())
        return shared->flags_path;

    shared->flags_path = shared->path + "flags/";
    Poco::File(shared->flags_path).createDirectories();
    return shared->flags_path;
}


void Context::setPath(const String & path)
{
    auto lock = getLock();
    shared->path = path;
}

void Context::setTemporaryPath(const String & path)
{
    auto lock = getLock();
    shared->tmp_path = path;
}

void Context::setFlagsPath(const String & path)
{
    auto lock = getLock();
    shared->flags_path = path;
}


void Context::setUsersConfig(const ConfigurationPtr & config)
{
    auto lock = getLock();
    shared->users_config = config;
    shared->users.loadFromConfig(*shared->users_config);
    shared->quotas.loadFromConfig(*shared->users_config);
}

ConfigurationPtr Context::getUsersConfig()
{
    auto lock = getLock();
    return shared->users_config;
}


void Context::setUser(const String & name, const String & password, const Poco::Net::SocketAddress & address, const String & quota_key)
{
    auto lock = getLock();

    const User & user_props = shared->users.get(name, password, address.host());

    /// Firstly set default settings from default profile
    auto default_profile_name = getDefaultProfileName();
    if (user_props.profile != default_profile_name)
        settings.setProfile(default_profile_name, *shared->users_config);
    settings.setProfile(user_props.profile, *shared->users_config);
    setQuota(user_props.quota, quota_key, name, address.host());

    client_info.current_user = name;
    client_info.current_address = address;

    if (!quota_key.empty())
        client_info.quota_key = quota_key;
}


void Context::setQuota(const String & name, const String & quota_key, const String & user_name, const Poco::Net::IPAddress & address)
{
    auto lock = getLock();
    quota = shared->quotas.get(name, quota_key, user_name, address);
}


QuotaForIntervals & Context::getQuota()
{
    auto lock = getLock();
    return *quota;
}

void Context::checkDatabaseAccessRights(const std::string & database_name) const
{
    if (client_info.current_user.empty() || (database_name == "system"))
    {
         /// An unnamed user, i.e. server, has access to all databases.
         /// All users have access to the database system.
        return;
    }
    if (!shared->users.isAllowedDatabase(client_info.current_user, database_name))
        throw Exception("Access denied to database " + database_name, ErrorCodes::DATABASE_ACCESS_DENIED);
}

void Context::addDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
    auto lock = getLock();
    checkDatabaseAccessRights(from.first);
    checkDatabaseAccessRights(where.first);
    shared->view_dependencies[from].insert(where);
}

void Context::removeDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
    auto lock = getLock();
    checkDatabaseAccessRights(from.first);
    checkDatabaseAccessRights(where.first);
    shared->view_dependencies[from].erase(where);
}

Dependencies Context::getDependencies(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRights(db);

    ViewDependencies::const_iterator iter = shared->view_dependencies.find(DatabaseAndTableName(db, table_name));
    if (iter == shared->view_dependencies.end())
        return {};

    return Dependencies(iter->second.begin(), iter->second.end());
}

bool Context::isTableExist(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRights(db);

    Databases::const_iterator it = shared->databases.find(db);
    return shared->databases.end() != it
        && it->second->isTableExist(table_name);
}


bool Context::isDatabaseExist(const String & database_name) const
{
    auto lock = getLock();
    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRights(db);
    return shared->databases.end() != shared->databases.find(db);
}


void Context::assertTableExists(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRights(db);

    Databases::const_iterator it = shared->databases.find(db);
    if (shared->databases.end() == it)
        throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

    if (!it->second->isTableExist(table_name))
        throw Exception("Table " + db + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
}


void Context::assertTableDoesntExist(const String & database_name, const String & table_name, bool check_database_access_rights) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    if (check_database_access_rights)
        checkDatabaseAccessRights(db);

    Databases::const_iterator it = shared->databases.find(db);
    if (shared->databases.end() != it && it->second->isTableExist(table_name))
        throw Exception("Table " + db + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}


void Context::assertDatabaseExists(const String & database_name, bool check_database_access_rights) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    if (check_database_access_rights)
        checkDatabaseAccessRights(db);

    if (shared->databases.end() == shared->databases.find(db))
        throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
}


void Context::assertDatabaseDoesntExist(const String & database_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRights(db);

    if (shared->databases.end() != shared->databases.find(db))
        throw Exception("Database " + db + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
}


Tables Context::getExternalTables() const
{
    auto lock = getLock();

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
    auto lock = getLock();

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
    auto lock = getLock();

    /** Ability to access the temporary tables of another query in the form _query_QUERY_ID.table
      * NOTE In the future, you may need to think about isolation.
      */
    if (startsWith(database_name, "_query_"))
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

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRights(db);

    Databases::const_iterator it = shared->databases.find(db);
    if (shared->databases.end() == it)
    {
        if (exception)
            *exception = Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
        return {};
    }

    auto table = it->second->tryGetTable(table_name);
    if (!table)
    {
        if (exception)
            *exception = Exception("Table " + db + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        return {};
    }

    return table;
}


void Context::addExternalTable(const String & table_name, StoragePtr storage)
{
    if (external_tables.end() != external_tables.find(table_name))
        throw Exception("Temporary table " + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    external_tables[table_name] = storage;

    if (process_list_elem)
    {
        auto lock = getLock();
        shared->process_list.addTemporaryTable(*process_list_elem, table_name, storage);
    }
}


DDLGuard::DDLGuard(Map & map_, std::mutex & mutex_, std::unique_lock<std::mutex> && lock, const String & elem, const String & message)
    : map(map_), mutex(mutex_)
{
    bool inserted;
    std::tie(it, inserted) = map.emplace(elem, message);
    if (!inserted)
        throw Exception(it->second, ErrorCodes::DDL_GUARD_IS_ACTIVE);
}

DDLGuard::~DDLGuard()
{
    std::lock_guard<std::mutex> lock(mutex);
    map.erase(it);
}

std::unique_ptr<DDLGuard> Context::getDDLGuard(const String & database, const String & table, const String & message) const
{
    std::unique_lock<std::mutex> lock(shared->ddl_guards_mutex);
    return std::make_unique<DDLGuard>(shared->ddl_guards[database], shared->ddl_guards_mutex, std::move(lock), table, message);
}


std::unique_ptr<DDLGuard> Context::getDDLGuardIfTableDoesntExist(const String & database, const String & table, const String & message) const
{
    auto lock = getLock();

    Databases::const_iterator it = shared->databases.find(database);
    if (shared->databases.end() != it && it->second->isTableExist(table))
        return {};

    return getDDLGuard(database, table, message);
}


void Context::addDatabase(const String & database_name, const DatabasePtr & database)
{
    auto lock = getLock();

    assertDatabaseDoesntExist(database_name);
    shared->databases[database_name] = database;
}


DatabasePtr Context::detachDatabase(const String & database_name)
{
    auto lock = getLock();

    auto res = getDatabase(database_name);
    shared->databases.erase(database_name);
    return res;
}


ASTPtr Context::getCreateQuery(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    assertDatabaseExists(db);

    return shared->databases[db]->getCreateQuery(table_name);
}


Settings Context::getSettings() const
{
    auto lock = getLock();
    return settings;
}


Limits Context::getLimits() const
{
    auto lock = getLock();
    return settings.limits;
}


void Context::setSettings(const Settings & settings_)
{
    auto lock = getLock();
    settings = settings_;
}


void Context::setSetting(const String & name, const Field & value)
{
    auto lock = getLock();
    if (name == "profile")
        settings.setProfile(value.safeGet<String>(), *shared->users_config);
    else
        settings.set(name, value);
}


void Context::setSetting(const String & name, const std::string & value)
{
    auto lock = getLock();
    if (name == "profile")
        settings.setProfile(value, *shared->users_config);
    else
        settings.set(name, value);
}


String Context::getCurrentDatabase() const
{
    auto lock = getLock();
    return current_database;
}


String Context::getCurrentQueryId() const
{
    auto lock = getLock();
    return client_info.current_query_id;
}


void Context::setCurrentDatabase(const String & name)
{
    auto lock = getLock();
    assertDatabaseExists(name);
    current_database = name;
}


void Context::setCurrentQueryId(const String & query_id)
{
    auto lock = getLock();

    if (!client_info.current_query_id.empty())
        throw Exception("Logical error: attempt to set query_id twice", ErrorCodes::LOGICAL_ERROR);

    String query_id_to_set = query_id;

    if (query_id_to_set.empty())    /// If the user did not submit his query_id, then we generate it ourselves.
    {
        /// Generate random UUID, but using lower quality RNG,
        ///  because Poco::UUIDGenerator::generateRandom method is using /dev/random, that is very expensive.
        /// NOTE: Actually we don't need to use UUIDs for query identifiers.
        /// We could use any suitable string instead.

        union
        {
            char bytes[16];
            struct
            {
                UInt64 a;
                UInt64 b;
            };
        } random;

        random.a = shared->rng();
        random.b = shared->rng();

        /// Use protected constructor.
        struct UUID : Poco::UUID
        {
            UUID(const char * bytes, Poco::UUID::Version version)
                : Poco::UUID(bytes, version) {}
        };

        query_id_to_set = UUID(random.bytes, Poco::UUID::UUID_RANDOM).toString();
    }

    client_info.current_query_id = query_id_to_set;
}


String Context::getDefaultFormat() const
{
    auto lock = getLock();
    return default_format.empty() ? "TabSeparated" : default_format;
}


void Context::setDefaultFormat(const String & name)
{
    auto lock = getLock();
    default_format = name;
}

const Macros& Context::getMacros() const
{
    return shared->macros;
}

void Context::setMacros(Macros && macros)
{
    /// We assume that this assignment occurs once when the server starts. If this is not the case, you need to use a mutex.
    shared->macros = macros;
}

const Context & Context::getSessionContext() const
{
    if (!session_context)
        throw Exception("There is no session", ErrorCodes::THERE_IS_NO_SESSION);
    return *session_context;
}

Context & Context::getSessionContext()
{
    if (!session_context)
        throw Exception("There is no session", ErrorCodes::THERE_IS_NO_SESSION);
    return *session_context;
}

const Context & Context::getGlobalContext() const
{
    if (!global_context)
        throw Exception("Logical error: there is no global context", ErrorCodes::LOGICAL_ERROR);
    return *global_context;
}

Context & Context::getGlobalContext()
{
    if (!global_context)
        throw Exception("Logical error: there is no global context", ErrorCodes::LOGICAL_ERROR);
    return *global_context;
}


const EmbeddedDictionaries & Context::getEmbeddedDictionaries() const
{
    return getEmbeddedDictionariesImpl(false);
}


const ExternalDictionaries & Context::getExternalDictionaries() const
{
    return getExternalDictionariesImpl(false);
}


const EmbeddedDictionaries & Context::getEmbeddedDictionariesImpl(const bool throw_on_error) const
{
    std::lock_guard<std::mutex> lock(shared->embedded_dictionaries_mutex);

    if (!shared->embedded_dictionaries)
        shared->embedded_dictionaries = std::make_shared<EmbeddedDictionaries>(throw_on_error);

    return *shared->embedded_dictionaries;
}


const ExternalDictionaries & Context::getExternalDictionariesImpl(const bool throw_on_error) const
{
    std::lock_guard<std::mutex> lock(shared->external_dictionaries_mutex);

    if (!shared->external_dictionaries)
    {
        if (!this->global_context)
            throw Exception("Logical error: there is no global context", ErrorCodes::LOGICAL_ERROR);
        shared->external_dictionaries = std::make_shared<ExternalDictionaries>(*this->global_context, throw_on_error);
    }

    return *shared->external_dictionaries;
}


void Context::tryCreateEmbeddedDictionaries() const
{
    static_cast<void>(getEmbeddedDictionariesImpl(true));
}


void Context::tryCreateExternalDictionaries() const
{
    static_cast<void>(getExternalDictionariesImpl(true));
}


void Context::setProgressCallback(ProgressCallback callback)
{
    /// Callback is set to a session or to a query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    progress_callback = callback;
}

ProgressCallback Context::getProgressCallback() const
{
    return progress_callback;
}


void Context::setProcessListElement(ProcessList::Element * elem)
{
    /// Set to a session or query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    process_list_elem = elem;
}

ProcessList::Element * Context::getProcessListElement()
{
    return process_list_elem;
}


void Context::setUncompressedCache(size_t max_size_in_bytes)
{
    auto lock = getLock();

    if (shared->uncompressed_cache)
        throw Exception("Uncompressed cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->uncompressed_cache = std::make_shared<UncompressedCache>(max_size_in_bytes);
}


UncompressedCachePtr Context::getUncompressedCache() const
{
    auto lock = getLock();
    return shared->uncompressed_cache;
}

void Context::setMarkCache(size_t cache_size_in_bytes)
{
    auto lock = getLock();

    if (shared->mark_cache)
        throw Exception("Uncompressed cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->mark_cache = std::make_shared<MarkCache>(cache_size_in_bytes, std::chrono::seconds(settings.mark_cache_min_lifetime));
}

MarkCachePtr Context::getMarkCache() const
{
    auto lock = getLock();
    return shared->mark_cache;
}

BackgroundProcessingPool & Context::getBackgroundPool()
{
    auto lock = getLock();
    if (!shared->background_pool)
        shared->background_pool = std::make_shared<BackgroundProcessingPool>(settings.background_pool_size);
    return *shared->background_pool;
}

void Context::setReshardingWorker(std::shared_ptr<ReshardingWorker> resharding_worker)
{
    auto lock = getLock();
    if (shared->resharding_worker)
        throw Exception("Resharding background thread has already been initialized.", ErrorCodes::LOGICAL_ERROR);
    shared->resharding_worker = resharding_worker;
}

ReshardingWorker & Context::getReshardingWorker()
{
    auto lock = getLock();
    if (!shared->resharding_worker)
        throw Exception("Resharding background thread not initialized: resharding missing in configuration file.",
            ErrorCodes::LOGICAL_ERROR);
    return *shared->resharding_worker;
}

void Context::setDDLWorker(std::shared_ptr<DDLWorker> ddl_worker)
{
    auto lock = getLock();
    if (shared->ddl_worker)
        throw Exception("DDL background thread has already been initialized.", ErrorCodes::LOGICAL_ERROR);
    shared->ddl_worker = ddl_worker;
}

DDLWorker & Context::getDDLWorker()
{
    auto lock = getLock();
    if (!shared->ddl_worker)
        throw Exception("DDL background thread not initialized.", ErrorCodes::LOGICAL_ERROR);
    return *shared->ddl_worker;
}

void Context::resetCaches() const
{
    auto lock = getLock();

    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();

    if (shared->mark_cache)
        shared->mark_cache->reset();
}

void Context::setZooKeeper(zkutil::ZooKeeperPtr zookeeper)
{
    std::lock_guard<std::mutex> lock(shared->zookeeper_mutex);

    if (shared->zookeeper)
        throw Exception("ZooKeeper client has already been set.", ErrorCodes::LOGICAL_ERROR);

    shared->zookeeper = zookeeper;
}

zkutil::ZooKeeperPtr Context::getZooKeeper() const
{
    std::lock_guard<std::mutex> lock(shared->zookeeper_mutex);

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


std::shared_ptr<Cluster> Context::getCluster(const std::string & cluster_name) const
{
    auto res = getClusters().getCluster(cluster_name);

    if (!res)
        throw Exception("Requested cluster '" + cluster_name + "' not found", ErrorCodes::BAD_GET);

    return res;
}


std::shared_ptr<Cluster> Context::tryGetCluster(const std::string & cluster_name) const
{
    return getClusters().getCluster(cluster_name);
}


Clusters & Context::getClusters() const
{
    {
        std::lock_guard<std::mutex> lock(shared->clusters_mutex);
        if (!shared->clusters)
        {
            auto & config = shared->clusters_config ? *shared->clusters_config : Poco::Util::Application::instance().config();
            shared->clusters = std::make_unique<Clusters>(config, settings);
        }
    }

    return *shared->clusters;
}


/// On repeating calls updates existing clusters and adds new clusters, doesn't delete old clusters
void Context::setClustersConfig(const ConfigurationPtr & config)
{
    std::lock_guard<std::mutex> lock(shared->clusters_mutex);

    shared->clusters_config = config;
    if (shared->clusters)
        shared->clusters->updateClusters(*shared->clusters_config, settings);
}


Compiler & Context::getCompiler()
{
    auto lock = getLock();

    if (!shared->compiler)
        shared->compiler = std::make_unique<Compiler>(shared->path + "build/", 1);

    return *shared->compiler;
}


QueryLog & Context::getQueryLog()
{
    auto lock = getLock();

    if (!shared->query_log)
    {
        if (shared->shutdown_called)
            throw Exception("Will not get query_log because shutdown was called", ErrorCodes::LOGICAL_ERROR);

        if (!global_context)
            throw Exception("Logical error: no global context for query log", ErrorCodes::LOGICAL_ERROR);

        auto & config = Poco::Util::Application::instance().config();

        String database = config.getString("query_log.database", "system");
        String table = config.getString("query_log.table", "query_log");
        size_t flush_interval_milliseconds = config.getUInt64(
                "query_log.flush_interval_milliseconds", DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS);

        shared->query_log = std::make_unique<QueryLog>(
            *global_context, database, table, "MergeTree(event_date, event_time, 1024)", flush_interval_milliseconds);
    }

    return *shared->query_log;
}


std::shared_ptr<PartLog> Context::getPartLog()
{
    auto lock = getLock();

    auto & config = Poco::Util::Application::instance().config();
    if (!config.has("part_log"))
        return nullptr;

    if (!shared->part_log)
    {
        if (shared->shutdown_called)
            throw Exception("Will not get part_log because shutdown was called", ErrorCodes::LOGICAL_ERROR);

        if (!global_context)
            throw Exception("Logical error: no global context for part log", ErrorCodes::LOGICAL_ERROR);

        String database = config.getString("part_log.database", "system");
        String table = config.getString("part_log.table", "part_log");
        size_t flush_interval_milliseconds = config.getUInt64(
                "part_log.flush_interval_milliseconds", DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS);
        shared->part_log = std::make_unique<PartLog>(
            *global_context, database, table, "MergeTree(event_date, event_time, 1024)", flush_interval_milliseconds);
    }

    return shared->part_log;
}


CompressionMethod Context::chooseCompressionMethod(size_t part_size, double part_size_ratio) const
{
    auto lock = getLock();

    if (!shared->compression_method_selector)
    {
        constexpr auto config_name = "compression";
        auto & config = Poco::Util::Application::instance().config();

        if (config.has(config_name))
            shared->compression_method_selector = std::make_unique<CompressionMethodSelector>(config, "compression");
        else
            shared->compression_method_selector = std::make_unique<CompressionMethodSelector>();
    }

    return shared->compression_method_selector->choose(part_size, part_size_ratio);
}


const MergeTreeSettings & Context::getMergeTreeSettings()
{
    auto lock = getLock();

    if (!shared->merge_tree_settings)
    {
        auto & config = Poco::Util::Application::instance().config();
        shared->merge_tree_settings = std::make_unique<MergeTreeSettings>();
        shared->merge_tree_settings->loadFromConfig("merge_tree", config);
    }

    return *shared->merge_tree_settings;
}


void Context::setMaxTableSizeToDrop(size_t max_size)
{
    // Is initialized at server startup
    shared->max_table_size_to_drop = max_size;
}

void Context::checkTableCanBeDropped(const String & database, const String & table, size_t table_size)
{
    size_t max_table_size_to_drop = shared->max_table_size_to_drop;

    if (!max_table_size_to_drop || table_size <= max_table_size_to_drop)
        return;

    Poco::File force_file(getFlagsPath() + "force_drop_table");
    bool force_file_exists = force_file.exists();

    if (force_file_exists)
    {
        try
        {
            force_file.remove();
            return;
        }
        catch (...)
        {
            /// User should recreate force file on each drop, it shouldn't be protected
            tryLogCurrentException("Drop table check", "Can't remove force file to enable table drop");
        }
    }

    String table_size_str = formatReadableSizeWithDecimalSuffix(table_size);
    String max_table_size_to_drop_str = formatReadableSizeWithDecimalSuffix(max_table_size_to_drop);
    std::stringstream ostr;

    ostr << "Table " << backQuoteIfNeed(database) << "." << backQuoteIfNeed(table) << " was not dropped.\n"
         << "Reason:\n"
         << "1. Table size (" << table_size_str << ") is greater than max_table_size_to_drop (" << max_table_size_to_drop_str << ")\n"
         << "2. File '" << force_file.path() << "' intedned to force DROP "
            << (force_file_exists ? "exists but not writeable (could not be removed)" : "doesn't exist") << "\n";

    ostr << "How to fix this:\n"
         << "1. Either increase (or set to zero) max_table_size_to_drop in server config and restart ClickHouse\n"
         << "2. Either create forcing file " << force_file.path() << " and make sure that ClickHouse has write permission for it.\n"
         << "Example:\nsudo touch '" << force_file.path() << "' && sudo chmod 666 '" << force_file.path() << "'";

    throw Exception(ostr.str(), ErrorCodes::TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT);
}


BlockInputStreamPtr Context::getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, size_t max_block_size) const
{
    return shared->format_factory.getInput(name, buf, sample, *this, max_block_size);
}

BlockOutputStreamPtr Context::getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return shared->format_factory.getOutput(name, buf, sample, *this);
}


time_t Context::getUptimeSeconds() const
{
    auto lock = getLock();
    return shared->uptime_watch.elapsedSeconds();
}


void Context::shutdown()
{
    shared->shutdown();
}


Context::ApplicationType Context::getApplicationType() const
{
    return shared->application_type;
}

void Context::setApplicationType(ApplicationType type)
{
    /// Lock isn't required, you should set it at start
    shared->application_type = type;
}


String Context::getDefaultProfileName() const
{
    return shared->default_profile_name;
}

void Context::setDefaultProfileName(const String & name)
{
    shared->default_profile_name = name;
}


}
