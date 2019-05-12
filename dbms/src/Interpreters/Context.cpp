#include <map>
#include <set>
#include <optional>
#include <memory>
#include <Poco/Mutex.h>
#include <Poco/UUID.h>
#include <Poco/Net/IPAddress.h>
#include <pcg_random.hpp>
#include <Common/Macros.h>
#include <Common/escapeForFileName.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Compression/ICompressionCodec.h>
#include <Core/BackgroundSchedulePool.h>
#include <Formats/FormatFactory.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/CompressionCodecSelector.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/ActionLocksManager.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionJIT.h>
#include <Interpreters/RuntimeComponentsFactory.h>
#include <Interpreters/IUsersManager.h>
#include <Interpreters/Quota.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/ExternalModels.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Interpreters/Compiler.h>
#include <Interpreters/SettingsConstraints.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Common/DNSResolver.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/UncompressedCache.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/Config/ConfigProcessor.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ShellCommand.h>
#include <common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event ContextLock;
    extern const Event CompiledCacheSizeBytes;
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
    extern const int THERE_IS_NO_SESSION;
    extern const int THERE_IS_NO_QUERY;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int DDL_GUARD_IS_ACTIVE;
    extern const int TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT;
    extern const int PARTITION_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT;
    extern const int SESSION_NOT_FOUND;
    extern const int SESSION_IS_LOCKED;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
}


/** Set of known objects (environment), that could be used in query.
  * Shared (global) part. Order of members (especially, order of destruction) is very important.
  */
struct ContextShared
{
    Logger * log = &Logger::get("Context");

    std::unique_ptr<IRuntimeComponentsFactory> runtime_components_factory;

    /// For access of most of shared objects. Recursive mutex.
    mutable std::recursive_mutex mutex;
    /// Separate mutex for access of dictionaries. Separate mutex to avoid locks when server doing request to itself.
    mutable std::mutex embedded_dictionaries_mutex;
    mutable std::recursive_mutex external_dictionaries_mutex;
    mutable std::mutex external_models_mutex;
    /// Separate mutex for re-initialization of zookeer session. This operation could take a long time and must not interfere with another operations.
    mutable std::mutex zookeeper_mutex;

    mutable zkutil::ZooKeeperPtr zookeeper;                 /// Client for ZooKeeper.

    String interserver_io_host;                             /// The host name by which this server is available for other servers.
    UInt16 interserver_io_port = 0;                         /// and port.
    String interserver_io_user;
    String interserver_io_password;
    String interserver_scheme;                              /// http or https

    String path;                                            /// Path to the data directory, with a slash at the end.
    String tmp_path;                                        /// The path to the temporary files that occur when processing the request.
    String flags_path;                                      /// Path to the directory with some control flags for server maintenance.
    String user_files_path;                                 /// Path to the directory with user provided files, usable by 'file' table function.
    ConfigurationPtr config;                                /// Global configuration settings.

    Databases databases;                                    /// List of databases and tables in them.
    mutable std::optional<EmbeddedDictionaries> embedded_dictionaries;    /// Metrica's dictionaries. Have lazy initialization.
    mutable std::optional<ExternalDictionaries> external_dictionaries;
    mutable std::optional<ExternalModels> external_models;
    String default_profile_name;                            /// Default profile name used for default values.
    String system_profile_name;                             /// Profile used by system processes
    std::unique_ptr<IUsersManager> users_manager;           /// Known users.
    Quotas quotas;                                          /// Known quotas for resource use.
    mutable UncompressedCachePtr uncompressed_cache;        /// The cache of decompressed blocks.
    mutable MarkCachePtr mark_cache;                        /// Cache of marks in compressed files.
    ProcessList process_list;                               /// Executing queries at the moment.
    MergeList merge_list;                                   /// The list of executable merge (for (Replicated)?MergeTree)
    ViewDependencies view_dependencies;                     /// Current dependencies
    ConfigurationPtr users_config;                          /// Config with the users, profiles and quotas sections.
    InterserverIOHandler interserver_io_handler;            /// Handler for interserver communication.
    std::optional<BackgroundProcessingPool> background_pool; /// The thread pool for the background work performed by the tables.
    std::optional<BackgroundSchedulePool> schedule_pool;    /// A thread pool that can run different jobs in background (used in replicated tables)
    MultiVersion<Macros> macros;                            /// Substitutions extracted from config.
    std::optional<Compiler> compiler;                     /// Used for dynamic compilation of queries' parts if it necessary.
    std::unique_ptr<DDLWorker> ddl_worker;                  /// Process ddl commands from zk.
    /// Rules for selecting the compression settings, depending on the size of the part.
    mutable std::unique_ptr<CompressionCodecSelector> compression_codec_selector;
    std::optional<MergeTreeSettings> merge_tree_settings; /// Settings of MergeTree* engines.
    size_t max_table_size_to_drop = 50000000000lu;          /// Protects MergeTree tables from accidental DROP (50GB by default)
    size_t max_partition_size_to_drop = 50000000000lu;      /// Protects MergeTree partitions from accidental DROP (50GB by default)
    String format_schema_path;                              /// Path to a directory that contains schema files used by input formats.
    ActionLocksManagerPtr action_locks_manager;             /// Set of storages' action lockers
    std::optional<SystemLogs> system_logs;                              /// Used to log queries and operations on parts

    /// Named sessions. The user could specify session identifier to reuse settings and temporary tables in subsequent requests.

    class SessionKeyHash
    {
    public:
        size_t operator()(const Context::SessionKey & key) const
        {
            SipHash hash;
            hash.update(key.first);
            hash.update(key.second);
            return hash.get64();
        }
    };

    using Sessions = std::unordered_map<Context::SessionKey, std::shared_ptr<Context>, SessionKeyHash>;
    using CloseTimes = std::deque<std::vector<Context::SessionKey>>;
    mutable Sessions sessions;
    mutable CloseTimes close_times;
    std::chrono::steady_clock::duration close_interval = std::chrono::seconds(1);
    std::chrono::steady_clock::time_point close_cycle_time = std::chrono::steady_clock::now();
    UInt64 close_cycle = 0;

    /// Clusters for distributed tables
    /// Initialized on demand (on distributed storages initialization) since Settings should be initialized
    std::unique_ptr<Clusters> clusters;
    ConfigurationPtr clusters_config;                        /// Soteres updated configs
    mutable std::mutex clusters_mutex;                        /// Guards clusters and clusters_config

#if USE_EMBEDDED_COMPILER
    std::shared_ptr<CompiledExpressionCache> compiled_expression_cache;
#endif

    bool shutdown_called = false;

    /// Do not allow simultaneous execution of DDL requests on the same table.
    /// database -> table -> (mutex, counter), counter: how many threads are running a query on the table at the same time
    /// For the duration of the operation, an element is placed here, and an object is returned,
    /// which deletes the element in the destructor when counter becomes zero.
    /// In case the element already exists, waits, when query will be executed in other thread. See class DDLGuard below.
    using DDLGuards = std::unordered_map<String, DDLGuard::Map>;
    DDLGuards ddl_guards;
    /// If you capture mutex and ddl_guards_mutex, then you need to grab them strictly in this order.
    mutable std::mutex ddl_guards_mutex;

    Stopwatch uptime_watch;

    Context::ApplicationType application_type = Context::ApplicationType::SERVER;

    pcg64 rng{randomSeed()};

    /// vector of xdbc-bridge commands, they will be killed when Context will be destroyed
    std::vector<std::unique_ptr<ShellCommand>> bridge_commands;

    Context::ConfigReloadCallback config_reload_callback;

    ContextShared(std::unique_ptr<IRuntimeComponentsFactory> runtime_components_factory_)
        : runtime_components_factory(std::move(runtime_components_factory_)), macros(std::make_unique<Macros>())
    {
        /// TODO: make it singleton (?)
        static std::atomic<size_t> num_calls{0};
        if (++num_calls > 1)
        {
            std::cerr << "Attempting to create multiple ContextShared instances. Stack trace:\n" << StackTrace().toString();
            std::cerr.flush();
            std::terminate();
        }

        initialize();
    }


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

        {
            std::lock_guard lock(mutex);

            /** After this point, system logs will shutdown their threads and no longer write any data.
            * It will prevent recreation of system tables at shutdown.
            * Note that part changes at shutdown won't be logged to part log.
            */
            system_logs.reset();
        }

        /** At this point, some tables may have threads that block our mutex.
          * To shutdown them correctly, we will copy the current list of tables,
          *  and ask them all to finish their work.
          * Then delete all objects with tables.
          */

        Databases current_databases;

        {
            std::lock_guard lock(mutex);
            current_databases = databases;
        }

        /// We still hold "databases" in Context (instead of std::move) for Buffer tables to flush data correctly.

        for (auto & database : current_databases)
            database.second->shutdown();

        {
            std::lock_guard lock(mutex);
            databases.clear();
        }

        /// Preemptive destruction is important, because these objects may have a refcount to ContextShared (cyclic reference).
        /// TODO: Get rid of this.

        embedded_dictionaries.reset();
        external_dictionaries.reset();
        external_models.reset();
        background_pool.reset();
        schedule_pool.reset();
        ddl_worker.reset();
    }

private:
    void initialize()
    {
       users_manager = runtime_components_factory->createUsersManager();
    }
};


Context::Context() = default;
Context::Context(const Context &) = default;
Context & Context::operator=(const Context &) = default;


Context Context::createGlobal(std::unique_ptr<IRuntimeComponentsFactory> runtime_components_factory)
{
    Context res;
    res.shared = std::make_shared<ContextShared>(std::move(runtime_components_factory));
    res.quota = std::make_shared<QuotaForIntervals>();
    return res;
}

Context Context::createGlobal()
{
    return createGlobal(std::make_unique<RuntimeComponentsFactory>());
}

Context::~Context() = default;


InterserverIOHandler & Context::getInterserverIOHandler() { return shared->interserver_io_handler; }

std::unique_lock<std::recursive_mutex> Context::getLock() const
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    return std::unique_lock(shared->mutex);
}

ProcessList & Context::getProcessList() { return shared->process_list; }
const ProcessList & Context::getProcessList() const { return shared->process_list; }
MergeList & Context::getMergeList() { return shared->merge_list; }
const MergeList & Context::getMergeList() const { return shared->merge_list; }


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


Context::SessionKey Context::getSessionKey(const String & session_id) const
{
    auto & user_name = client_info.current_user;

    if (user_name.empty())
        throw Exception("Empty user name.", ErrorCodes::LOGICAL_ERROR);

    return SessionKey(user_name, session_id);
}


void Context::scheduleCloseSession(const Context::SessionKey & key, std::chrono::steady_clock::duration timeout)
{
    const UInt64 close_index = timeout / shared->close_interval + 1;
    const auto new_close_cycle = shared->close_cycle + close_index;

    if (session_close_cycle != new_close_cycle)
    {
        session_close_cycle = new_close_cycle;
        if (shared->close_times.size() < close_index + 1)
            shared->close_times.resize(close_index + 1);
        shared->close_times[close_index].emplace_back(key);
    }
}


std::shared_ptr<Context> Context::acquireSession(const String & session_id, std::chrono::steady_clock::duration timeout, bool session_check) const
{
    auto lock = getLock();

    const auto & key = getSessionKey(session_id);
    auto it = shared->sessions.find(key);

    if (it == shared->sessions.end())
    {
        if (session_check)
            throw Exception("Session not found.", ErrorCodes::SESSION_NOT_FOUND);

        auto new_session = std::make_shared<Context>(*this);

        new_session->scheduleCloseSession(key, timeout);

        it = shared->sessions.insert(std::make_pair(key, std::move(new_session))).first;
    }
    else if (it->second->client_info.current_user != client_info.current_user)
    {
        throw Exception("Session belongs to a different user", ErrorCodes::LOGICAL_ERROR);
    }

    const auto & session = it->second;

    if (session->session_is_used)
        throw Exception("Session is locked by a concurrent client.", ErrorCodes::SESSION_IS_LOCKED);
    session->session_is_used = true;

    session->client_info = client_info;

    return session;
}


void Context::releaseSession(const String & session_id, std::chrono::steady_clock::duration timeout)
{
    auto lock = getLock();

    session_is_used = false;
    scheduleCloseSession(getSessionKey(session_id), timeout);
}


std::chrono::steady_clock::duration Context::closeSessions() const
{
    auto lock = getLock();

    const auto now = std::chrono::steady_clock::now();

    if (now < shared->close_cycle_time)
        return shared->close_cycle_time - now;

    const auto current_cycle = shared->close_cycle;

    ++shared->close_cycle;
    shared->close_cycle_time = now + shared->close_interval;

    if (shared->close_times.empty())
        return shared->close_interval;

    auto & sessions_to_close = shared->close_times.front();

    for (const auto & key : sessions_to_close)
    {
        const auto session = shared->sessions.find(key);

        if (session != shared->sessions.end() && session->second->session_close_cycle <= current_cycle)
        {
            if (session->second->session_is_used)
                session->second->scheduleCloseSession(key, std::chrono::seconds(0));
            else
                shared->sessions.erase(session);
        }
    }

    shared->close_times.pop_front();

    return shared->close_interval;
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
    return shared->flags_path;
}

String Context::getUserFilesPath() const
{
    auto lock = getLock();
    return shared->user_files_path;
}

void Context::setPath(const String & path)
{
    auto lock = getLock();

    shared->path = path;

    if (shared->tmp_path.empty())
        shared->tmp_path = shared->path + "tmp/";

    if (shared->flags_path.empty())
        shared->flags_path = shared->path + "flags/";

    if (shared->user_files_path.empty())
        shared->user_files_path = shared->path + "user_files/";
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

void Context::setUserFilesPath(const String & path)
{
    auto lock = getLock();
    shared->user_files_path = path;
}

void Context::setConfig(const ConfigurationPtr & config)
{
    auto lock = getLock();
    shared->config = config;
}

const Poco::Util::AbstractConfiguration & Context::getConfigRef() const
{
    auto lock = getLock();
    return shared->config ? *shared->config : Poco::Util::Application::instance().config();
}

void Context::setUsersConfig(const ConfigurationPtr & config)
{
    auto lock = getLock();
    shared->users_config = config;
    shared->users_manager->loadFromConfig(*shared->users_config);
    shared->quotas.loadFromConfig(*shared->users_config);
}

ConfigurationPtr Context::getUsersConfig()
{
    auto lock = getLock();
    return shared->users_config;
}

bool Context::hasUserProperty(const String & database, const String & table, const String & name) const
{
    auto lock = getLock();

    // No user - no properties.
    if (client_info.current_user.empty())
        return false;

    const auto & props = shared->users_manager->getUser(client_info.current_user)->table_props;

    auto db = props.find(database);
    if (db == props.end())
        return false;

    auto table_props = db->second.find(table);
    if (table_props == db->second.end())
        return false;

    return !!table_props->second.count(name);
}

const String & Context::getUserProperty(const String & database, const String & table, const String & name) const
{
    auto lock = getLock();
    const auto & props = shared->users_manager->getUser(client_info.current_user)->table_props;
    return props.at(database).at(table).at(name);
}

void Context::calculateUserSettings()
{
    auto lock = getLock();

    String profile = shared->users_manager->getUser(client_info.current_user)->profile;

    /// 1) Set default settings (hardcoded values)
    /// NOTE: we ignore global_context settings (from which it is usually copied)
    /// NOTE: global_context settings are immutable and not auto updated
    settings = Settings();
    settings_constraints = nullptr;

    /// 2) Apply settings from default profile
    auto default_profile_name = getDefaultProfileName();
    if (profile != default_profile_name)
        setProfile(default_profile_name);

    /// 3) Apply settings from current user
    setProfile(profile);
}


void Context::setProfile(const String & profile)
{
    settings.setProfile(profile, *shared->users_config);

    auto new_constraints
        = settings_constraints ? std::make_shared<SettingsConstraints>(*settings_constraints) : std::make_shared<SettingsConstraints>();
    new_constraints->setProfile(profile, *shared->users_config);
    settings_constraints = std::move(new_constraints);
}


void Context::setUser(const String & name, const String & password, const Poco::Net::SocketAddress & address, const String & quota_key)
{
    auto lock = getLock();

    auto user_props = shared->users_manager->authorizeAndGetUser(name, password, address.host());

    client_info.current_user = name;
    client_info.current_address = address;
    client_info.current_password = password;

    if (!quota_key.empty())
        client_info.quota_key = quota_key;

    calculateUserSettings();

    setQuota(user_props->quota, quota_key, name, address.host());
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
    auto lock = getLock();
    checkDatabaseAccessRightsImpl(database_name);
}

bool Context::hasDatabaseAccessRights(const String & database_name) const
{
    auto lock = getLock();
    return client_info.current_user.empty() || (database_name == "system") ||
        shared->users_manager->hasAccessToDatabase(client_info.current_user, database_name);
}

void Context::checkDatabaseAccessRightsImpl(const std::string & database_name) const
{
    if (client_info.current_user.empty() || (database_name == "system"))
    {
         /// An unnamed user, i.e. server, has access to all databases.
         /// All users have access to the database system.
        return;
    }
    if (!shared->users_manager->hasAccessToDatabase(client_info.current_user, database_name))
        throw Exception("Access denied to database " + database_name + " for user " + client_info.current_user , ErrorCodes::DATABASE_ACCESS_DENIED);
}

void Context::addDependencyUnsafe(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
    checkDatabaseAccessRightsImpl(from.first);
    checkDatabaseAccessRightsImpl(where.first);
    shared->view_dependencies[from].insert(where);

    // Notify table of dependencies change
    auto table = tryGetTable(from.first, from.second);
    if (table != nullptr)
        table->updateDependencies();
}

void Context::addDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
    auto lock = getLock();
    addDependencyUnsafe(from, where);
}

void Context::removeDependencyUnsafe(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
    checkDatabaseAccessRightsImpl(from.first);
    checkDatabaseAccessRightsImpl(where.first);
    shared->view_dependencies[from].erase(where);

    // Notify table of dependencies change
    auto table = tryGetTable(from.first, from.second);
    if (table != nullptr)
        table->updateDependencies();
}

void Context::removeDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
    auto lock = getLock();
    removeDependencyUnsafe(from, where);
}

Dependencies Context::getDependencies(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);

    if (database_name.empty() && tryGetExternalTable(table_name))
    {
        /// Table is temporary. Access granted.
    }
    else
    {
        checkDatabaseAccessRightsImpl(db);
    }

    ViewDependencies::const_iterator iter = shared->view_dependencies.find(DatabaseAndTableName(db, table_name));
    if (iter == shared->view_dependencies.end())
        return {};

    return Dependencies(iter->second.begin(), iter->second.end());
}

bool Context::isTableExist(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRightsImpl(db);

    Databases::const_iterator it = shared->databases.find(db);
    return shared->databases.end() != it
        && it->second->isTableExist(*this, table_name);
}


bool Context::isDatabaseExist(const String & database_name) const
{
    auto lock = getLock();
    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRightsImpl(db);
    return shared->databases.end() != shared->databases.find(db);
}

bool Context::isExternalTableExist(const String & table_name) const
{
    return external_tables.end() != external_tables.find(table_name);
}


void Context::assertTableExists(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRightsImpl(db);

    Databases::const_iterator it = shared->databases.find(db);
    if (shared->databases.end() == it)
        throw Exception("Database " + backQuoteIfNeed(db) + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

    if (!it->second->isTableExist(*this, table_name))
        throw Exception("Table " + backQuoteIfNeed(db) + "." + backQuoteIfNeed(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
}


void Context::assertTableDoesntExist(const String & database_name, const String & table_name, bool check_database_access_rights) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    if (check_database_access_rights)
        checkDatabaseAccessRightsImpl(db);

    Databases::const_iterator it = shared->databases.find(db);
    if (shared->databases.end() != it && it->second->isTableExist(*this, table_name))
        throw Exception("Table " + backQuoteIfNeed(db) + "." + backQuoteIfNeed(table_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}


void Context::assertDatabaseExists(const String & database_name, bool check_database_access_rights) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    if (check_database_access_rights)
        checkDatabaseAccessRightsImpl(db);

    if (shared->databases.end() == shared->databases.find(db))
        throw Exception("Database " + backQuoteIfNeed(db) + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
}


void Context::assertDatabaseDoesntExist(const String & database_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRightsImpl(db);

    if (shared->databases.end() != shared->databases.find(db))
        throw Exception("Database " + backQuoteIfNeed(db) + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
}


Tables Context::getExternalTables() const
{
    auto lock = getLock();

    Tables res;
    for (auto & table : external_tables)
        res[table.first] = table.second.first;

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
    TableAndCreateASTs::const_iterator jt = external_tables.find(table_name);
    if (external_tables.end() == jt)
        return StoragePtr();

    return jt->second.first;
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

    if (database_name.empty())
    {
        StoragePtr res = tryGetExternalTable(table_name);
        if (res)
            return res;
    }

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRightsImpl(db);

    Databases::const_iterator it = shared->databases.find(db);
    if (shared->databases.end() == it)
    {
        if (exception)
            *exception = Exception("Database " + backQuoteIfNeed(db) + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
        return {};
    }

    auto table = it->second->tryGetTable(*this, table_name);
    if (!table)
    {
        if (exception)
            *exception = Exception("Table " + backQuoteIfNeed(db) + "." + backQuoteIfNeed(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        return {};
    }

    return table;
}


void Context::addExternalTable(const String & table_name, const StoragePtr & storage, const ASTPtr & ast)
{
    if (external_tables.end() != external_tables.find(table_name))
        throw Exception("Temporary table " + backQuoteIfNeed(table_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    external_tables[table_name] = std::pair(storage, ast);
}

StoragePtr Context::tryRemoveExternalTable(const String & table_name)
{
    TableAndCreateASTs::const_iterator it = external_tables.find(table_name);

    if (external_tables.end() == it)
        return StoragePtr();

    auto storage = it->second.first;
    external_tables.erase(it);
    return storage;
}


StoragePtr Context::executeTableFunction(const ASTPtr & table_expression)
{
    /// Slightly suboptimal.
    auto hash = table_expression->getTreeHash();
    String key = toString(hash.first) + '_' + toString(hash.second);

    StoragePtr & res = table_function_results[key];

    if (!res)
    {
        TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_expression->as<ASTFunction>()->name, *this);

        /// Run it and remember the result
        res = table_function_ptr->execute(table_expression, *this);
    }

    return res;
}


DDLGuard::DDLGuard(Map & map_, std::unique_lock<std::mutex> guards_lock_, const String & elem)
    : map(map_), guards_lock(std::move(guards_lock_))
{
    it = map.emplace(elem, Entry{std::make_unique<std::mutex>(), 0}).first;
    ++it->second.counter;
    guards_lock.unlock();
    table_lock = std::unique_lock(*it->second.mutex);
}

DDLGuard::~DDLGuard()
{
    guards_lock.lock();
    --it->second.counter;
    if (!it->second.counter)
    {
        table_lock.unlock();
        map.erase(it);
    }
}

std::unique_ptr<DDLGuard> Context::getDDLGuard(const String & database, const String & table) const
{
    std::unique_lock lock(shared->ddl_guards_mutex);
    return std::make_unique<DDLGuard>(shared->ddl_guards[database], std::move(lock), table);
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


ASTPtr Context::getCreateTableQuery(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    assertDatabaseExists(db);

    return shared->databases[db]->getCreateTableQuery(*this, table_name);
}

ASTPtr Context::getCreateExternalTableQuery(const String & table_name) const
{
    TableAndCreateASTs::const_iterator jt = external_tables.find(table_name);
    if (external_tables.end() == jt)
        throw Exception("Temporary table " + backQuoteIfNeed(table_name) + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);

    return jt->second.second;
}

ASTPtr Context::getCreateDatabaseQuery(const String & database_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    assertDatabaseExists(db);

    return shared->databases[db]->getCreateDatabaseQuery(*this);
}

Settings Context::getSettings() const
{
    return settings;
}


void Context::setSettings(const Settings & settings_)
{
    settings = settings_;
}


void Context::setSetting(const String & name, const String & value)
{
    auto lock = getLock();
    if (name == "profile")
    {
        setProfile(value);
        return;
    }
    settings.set(name, value);
}


void Context::setSetting(const String & name, const Field & value)
{
    auto lock = getLock();
    if (name == "profile")
    {
        setProfile(value.safeGet<String>());
        return;
    }
    settings.set(name, value);
}


void Context::applySettingChange(const SettingChange & change)
{
    setSetting(change.name, change.value);
}


void Context::applySettingsChanges(const SettingsChanges & changes)
{
    auto lock = getLock();
    for (const SettingChange & change : changes)
        applySettingChange(change);
}


void Context::checkSettingsConstraints(const SettingChange & change)
{
    if (settings_constraints)
        settings_constraints->check(settings, change);
}


void Context::checkSettingsConstraints(const SettingsChanges & changes)
{
    if (settings_constraints)
        settings_constraints->check(settings, changes);
}


String Context::getCurrentDatabase() const
{
    return current_database;
}


String Context::getCurrentQueryId() const
{
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
            } words;
        } random;

        {
            auto lock = getLock();

            random.words.a = shared->rng();
            random.words.b = shared->rng();
        }

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

void Context::killCurrentQuery()
{
    if (process_list_elem)
    {
        process_list_elem->cancelQuery(true);
    }
};

String Context::getDefaultFormat() const
{
    return default_format.empty() ? "TabSeparated" : default_format;
}


void Context::setDefaultFormat(const String & name)
{
    default_format = name;
}

MultiVersion<Macros>::Version Context::getMacros() const
{
    return shared->macros.get();
}

void Context::setMacros(std::unique_ptr<Macros> && macros)
{
    shared->macros.set(std::move(macros));
}

const Context & Context::getQueryContext() const
{
    if (!query_context)
        throw Exception("There is no query", ErrorCodes::THERE_IS_NO_QUERY);
    return *query_context;
}

Context & Context::getQueryContext()
{
    if (!query_context)
        throw Exception("There is no query", ErrorCodes::THERE_IS_NO_QUERY);
    return *query_context;
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

EmbeddedDictionaries & Context::getEmbeddedDictionaries()
{
    return getEmbeddedDictionariesImpl(false);
}


const ExternalDictionaries & Context::getExternalDictionaries() const
{
    return getExternalDictionariesImpl(false);
}

ExternalDictionaries & Context::getExternalDictionaries()
{
    return getExternalDictionariesImpl(false);
}


const ExternalModels & Context::getExternalModels() const
{
    return getExternalModelsImpl(false);
}

ExternalModels & Context::getExternalModels()
{
    return getExternalModelsImpl(false);
}


EmbeddedDictionaries & Context::getEmbeddedDictionariesImpl(const bool throw_on_error) const
{
    std::lock_guard lock(shared->embedded_dictionaries_mutex);

    if (!shared->embedded_dictionaries)
    {
        auto geo_dictionaries_loader = shared->runtime_components_factory->createGeoDictionariesLoader();

        shared->embedded_dictionaries.emplace(
            std::move(geo_dictionaries_loader),
            *this->global_context,
            throw_on_error);
    }

    return *shared->embedded_dictionaries;
}


ExternalDictionaries & Context::getExternalDictionariesImpl(const bool throw_on_error) const
{
    {
        std::lock_guard lock(shared->external_dictionaries_mutex);
        if (shared->external_dictionaries)
            return *shared->external_dictionaries;
    }

    const auto & config = getConfigRef();
    std::lock_guard lock(shared->external_dictionaries_mutex);
    if (!shared->external_dictionaries)
    {
        if (!this->global_context)
            throw Exception("Logical error: there is no global context", ErrorCodes::LOGICAL_ERROR);

        auto config_repository = shared->runtime_components_factory->createExternalDictionariesConfigRepository();
        shared->external_dictionaries.emplace(std::move(config_repository), config, *this->global_context);
        shared->external_dictionaries->init(throw_on_error);
    }
    return *shared->external_dictionaries;
}

ExternalModels & Context::getExternalModelsImpl(bool throw_on_error) const
{
    std::lock_guard lock(shared->external_models_mutex);
    if (!shared->external_models)
    {
        if (!this->global_context)
            throw Exception("Logical error: there is no global context", ErrorCodes::LOGICAL_ERROR);

        auto config_repository = shared->runtime_components_factory->createExternalModelsConfigRepository();
        shared->external_models.emplace(std::move(config_repository), *this->global_context);
        shared->external_models->init(throw_on_error);
    }
    return *shared->external_models;
}

void Context::tryCreateEmbeddedDictionaries() const
{
    static_cast<void>(getEmbeddedDictionariesImpl(true));
}


void Context::tryCreateExternalDictionaries() const
{
    static_cast<void>(getExternalDictionariesImpl(true));
}


void Context::tryCreateExternalModels() const
{
    static_cast<void>(getExternalModelsImpl(true));
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

ProcessList::Element * Context::getProcessListElement() const
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


void Context::dropUncompressedCache() const
{
    auto lock = getLock();
    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();
}


void Context::setMarkCache(size_t cache_size_in_bytes)
{
    auto lock = getLock();

    if (shared->mark_cache)
        throw Exception("Mark cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->mark_cache = std::make_shared<MarkCache>(cache_size_in_bytes, std::chrono::seconds(settings.mark_cache_min_lifetime));
}


MarkCachePtr Context::getMarkCache() const
{
    auto lock = getLock();
    return shared->mark_cache;
}


void Context::dropMarkCache() const
{
    auto lock = getLock();
    if (shared->mark_cache)
        shared->mark_cache->reset();
}


void Context::dropCaches() const
{
    auto lock = getLock();

    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();

    if (shared->mark_cache)
        shared->mark_cache->reset();
}

BackgroundProcessingPool & Context::getBackgroundPool()
{
    auto lock = getLock();
    if (!shared->background_pool)
        shared->background_pool.emplace(settings.background_pool_size);
    return *shared->background_pool;
}

BackgroundSchedulePool & Context::getSchedulePool()
{
    auto lock = getLock();
    if (!shared->schedule_pool)
        shared->schedule_pool.emplace(settings.background_schedule_pool_size);
    return *shared->schedule_pool;
}

void Context::setDDLWorker(std::unique_ptr<DDLWorker> ddl_worker)
{
    auto lock = getLock();
    if (shared->ddl_worker)
        throw Exception("DDL background thread has already been initialized.", ErrorCodes::LOGICAL_ERROR);
    shared->ddl_worker = std::move(ddl_worker);
}

DDLWorker & Context::getDDLWorker() const
{
    auto lock = getLock();
    if (!shared->ddl_worker)
        throw Exception("DDL background thread is not initialized.", ErrorCodes::LOGICAL_ERROR);
    return *shared->ddl_worker;
}

zkutil::ZooKeeperPtr Context::getZooKeeper() const
{
    std::lock_guard lock(shared->zookeeper_mutex);

    if (!shared->zookeeper)
        shared->zookeeper = std::make_shared<zkutil::ZooKeeper>(getConfigRef(), "zookeeper");
    else if (shared->zookeeper->expired())
        shared->zookeeper = shared->zookeeper->startNewSession();

    return shared->zookeeper;
}

bool Context::hasZooKeeper() const
{
    return getConfigRef().has("zookeeper");
}


void Context::setInterserverIOAddress(const String & host, UInt16 port)
{
    shared->interserver_io_host = host;
    shared->interserver_io_port = port;
}

std::pair<String, UInt16> Context::getInterserverIOAddress() const
{
    if (shared->interserver_io_host.empty() || shared->interserver_io_port == 0)
        throw Exception("Parameter 'interserver_http(s)_port' required for replication is not specified in configuration file.",
                        ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    return { shared->interserver_io_host, shared->interserver_io_port };
}

void Context::setInterserverCredentials(const String & user, const String & password)
{
    shared->interserver_io_user = user;
    shared->interserver_io_password = password;
}

std::pair<String, String> Context::getInterserverCredentials() const
{
    return { shared->interserver_io_user, shared->interserver_io_password };
}

void Context::setInterserverScheme(const String & scheme)
{
    shared->interserver_scheme = scheme;
}

String Context::getInterserverScheme() const
{
    return shared->interserver_scheme;
}

UInt16 Context::getTCPPort() const
{
    auto lock = getLock();

    auto & config = getConfigRef();
    return config.getInt("tcp_port");
}

std::optional<UInt16> Context::getTCPPortSecure() const
{
    auto lock = getLock();

    auto & config = getConfigRef();
    if (config.has("tcp_port_secure"))
        return config.getInt("tcp_port_secure");
    return {};
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


void Context::reloadClusterConfig()
{
    while (true)
    {
        ConfigurationPtr cluster_config;
        {
            std::lock_guard lock(shared->clusters_mutex);
            cluster_config = shared->clusters_config;
        }

        auto & config = cluster_config ? *cluster_config : getConfigRef();
        auto new_clusters = std::make_unique<Clusters>(config, settings);

        {
            std::lock_guard lock(shared->clusters_mutex);
            if (shared->clusters_config.get() == cluster_config.get())
            {
                shared->clusters = std::move(new_clusters);
                return;
            }

            /// Clusters config has been suddenly changed, recompute clusters
        }
    }
}


Clusters & Context::getClusters() const
{
    std::lock_guard lock(shared->clusters_mutex);
    if (!shared->clusters)
    {
        auto & config = shared->clusters_config ? *shared->clusters_config : getConfigRef();
        shared->clusters = std::make_unique<Clusters>(config, settings);
    }

    return *shared->clusters;
}


/// On repeating calls updates existing clusters and adds new clusters, doesn't delete old clusters
void Context::setClustersConfig(const ConfigurationPtr & config, const String & config_name)
{
    std::lock_guard lock(shared->clusters_mutex);

    shared->clusters_config = config;

    if (!shared->clusters)
        shared->clusters = std::make_unique<Clusters>(*shared->clusters_config, settings, config_name);
    else
        shared->clusters->updateClusters(*shared->clusters_config, settings, config_name);
}


void Context::setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster)
{
    std::lock_guard lock(shared->clusters_mutex);

    if (!shared->clusters)
        throw Exception("Clusters are not set", ErrorCodes::LOGICAL_ERROR);

    shared->clusters->setCluster(cluster_name, cluster);
}


Compiler & Context::getCompiler()
{
    auto lock = getLock();

    if (!shared->compiler)
        shared->compiler.emplace(shared->path + "build/", 1);

    return *shared->compiler;
}


void Context::initializeSystemLogs()
{
    auto lock = getLock();
    shared->system_logs.emplace(*global_context, getConfigRef());
}


std::shared_ptr<QueryLog> Context::getQueryLog()
{
    auto lock = getLock();

    if (!shared->system_logs || !shared->system_logs->query_log)
        return {};

    return shared->system_logs->query_log;
}


std::shared_ptr<QueryThreadLog> Context::getQueryThreadLog()
{
    auto lock = getLock();

    if (!shared->system_logs || !shared->system_logs->query_thread_log)
        return {};

    return shared->system_logs->query_thread_log;
}


std::shared_ptr<PartLog> Context::getPartLog(const String & part_database)
{
    auto lock = getLock();

    /// No part log or system logs are shutting down.
    if (!shared->system_logs || !shared->system_logs->part_log)
        return {};

    /// Will not log operations on system tables (including part_log itself).
    /// It doesn't make sense and not allow to destruct PartLog correctly due to infinite logging and flushing,
    /// and also make troubles on startup.
    if (part_database == shared->system_logs->part_log_database)
        return {};

    return shared->system_logs->part_log;
}


CompressionCodecPtr Context::chooseCompressionCodec(size_t part_size, double part_size_ratio) const
{
    auto lock = getLock();

    if (!shared->compression_codec_selector)
    {
        constexpr auto config_name = "compression";
        auto & config = getConfigRef();

        if (config.has(config_name))
            shared->compression_codec_selector = std::make_unique<CompressionCodecSelector>(config, "compression");
        else
            shared->compression_codec_selector = std::make_unique<CompressionCodecSelector>();
    }

    return shared->compression_codec_selector->choose(part_size, part_size_ratio);
}


const MergeTreeSettings & Context::getMergeTreeSettings() const
{
    auto lock = getLock();

    if (!shared->merge_tree_settings)
    {
        auto & config = getConfigRef();
        shared->merge_tree_settings.emplace();
        shared->merge_tree_settings->loadFromConfig("merge_tree", config);
    }

    return *shared->merge_tree_settings;
}


void Context::checkCanBeDropped(const String & database, const String & table, const size_t & size, const size_t & max_size_to_drop) const
{
    if (!max_size_to_drop || size <= max_size_to_drop)
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
            tryLogCurrentException("Drop table check", "Can't remove force file to enable table or partition drop");
        }
    }

    String size_str = formatReadableSizeWithDecimalSuffix(size);
    String max_size_to_drop_str = formatReadableSizeWithDecimalSuffix(max_size_to_drop);
    std::stringstream ostr;

    ostr << "Table or Partition in " << backQuoteIfNeed(database) << "." << backQuoteIfNeed(table) << " was not dropped.\n"
         << "Reason:\n"
         << "1. Size (" << size_str << ") is greater than max_[table/partition]_size_to_drop (" << max_size_to_drop_str << ")\n"
         << "2. File '" << force_file.path() << "' intended to force DROP "
         << (force_file_exists ? "exists but not writeable (could not be removed)" : "doesn't exist") << "\n";

    ostr << "How to fix this:\n"
         << "1. Either increase (or set to zero) max_[table/partition]_size_to_drop in server config and restart ClickHouse\n"
         << "2. Either create forcing file " << force_file.path() << " and make sure that ClickHouse has write permission for it.\n"
         << "Example:\nsudo touch '" << force_file.path() << "' && sudo chmod 666 '" << force_file.path() << "'";

    throw Exception(ostr.str(), ErrorCodes::TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT);
}


void Context::setMaxTableSizeToDrop(size_t max_size)
{
    // Is initialized at server startup
    shared->max_table_size_to_drop = max_size;
}


void Context::checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const
{
    size_t max_table_size_to_drop = shared->max_table_size_to_drop;

    checkCanBeDropped(database, table, table_size, max_table_size_to_drop);
}


void Context::setMaxPartitionSizeToDrop(size_t max_size)
{
    // Is initialized at server startup
    shared->max_partition_size_to_drop = max_size;
}


void Context::checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const
{
    size_t max_partition_size_to_drop = shared->max_partition_size_to_drop;

    checkCanBeDropped(database, table, partition_size, max_partition_size_to_drop);
}


BlockInputStreamPtr Context::getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size) const
{
    return FormatFactory::instance().getInput(name, buf, sample, *this, max_block_size);
}

BlockOutputStreamPtr Context::getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutput(name, buf, sample, *this);
}


time_t Context::getUptimeSeconds() const
{
    auto lock = getLock();
    return shared->uptime_watch.elapsedSeconds();
}


void Context::setConfigReloadCallback(ConfigReloadCallback && callback)
{
    /// Is initialized at server startup, so lock isn't required. Otherwise use mutex.
    shared->config_reload_callback = std::move(callback);
}

void Context::reloadConfig() const
{
    /// Use mutex if callback may be changed after startup.
    if (!shared->config_reload_callback)
        throw Exception("Can't reload config beacuse config_reload_callback is not set.", ErrorCodes::LOGICAL_ERROR);

    shared->config_reload_callback();
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

void Context::setDefaultProfiles(const Poco::Util::AbstractConfiguration & config)
{
    shared->default_profile_name = config.getString("default_profile", "default");
    shared->system_profile_name = config.getString("system_profile", shared->default_profile_name);
    setSetting("profile", shared->system_profile_name);
}

String Context::getDefaultProfileName() const
{
    return shared->default_profile_name;
}

String Context::getSystemProfileName() const
{
    return shared->system_profile_name;
}

String Context::getFormatSchemaPath() const
{
    return shared->format_schema_path;
}

void Context::setFormatSchemaPath(const String & path)
{
    shared->format_schema_path = path;
}

Context::SampleBlockCache & Context::getSampleBlockCache() const
{
    return getQueryContext().sample_block_cache;
}


#if USE_EMBEDDED_COMPILER

std::shared_ptr<CompiledExpressionCache> Context::getCompiledExpressionCache() const
{
    auto lock = getLock();
    return shared->compiled_expression_cache;
}

void Context::setCompiledExpressionCache(size_t cache_size)
{

    auto lock = getLock();

    if (shared->compiled_expression_cache)
        throw Exception("Compiled expressions cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->compiled_expression_cache = std::make_shared<CompiledExpressionCache>(cache_size);
}

void Context::dropCompiledExpressionCache() const
{
    auto lock = getLock();
    if (shared->compiled_expression_cache)
        shared->compiled_expression_cache->reset();
}

#endif


void Context::addXDBCBridgeCommand(std::unique_ptr<ShellCommand> cmd)
{
    auto lock = getLock();
    shared->bridge_commands.emplace_back(std::move(cmd));
}


IHostContextPtr & Context::getHostContext()
{
    return host_context;
}


const IHostContextPtr & Context::getHostContext() const
{
    return host_context;
}


std::shared_ptr<ActionLocksManager> Context::getActionLocksManager()
{
    auto lock = getLock();

    if (!shared->action_locks_manager)
        shared->action_locks_manager = std::make_shared<ActionLocksManager>(getGlobalContext());

    return shared->action_locks_manager;
}


void Context::setExternalTablesInitializer(ExternalTablesInitializer && initializer)
{
    if (external_tables_initializer_callback)
        throw Exception("External tables initializer is already set", ErrorCodes::LOGICAL_ERROR);

    external_tables_initializer_callback = std::move(initializer);
}

void Context::initializeExternalTablesIfSet()
{
    if (external_tables_initializer_callback)
    {
        external_tables_initializer_callback(*this);
        /// Reset callback
        external_tables_initializer_callback = {};
    }
}


SessionCleaner::~SessionCleaner()
{
    try
    {
        {
            std::lock_guard lock{mutex};
            quit = true;
        }

        cond.notify_one();

        thread.join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void SessionCleaner::run()
{
    setThreadName("SessionCleaner");

    std::unique_lock lock{mutex};

    while (true)
    {
        auto interval = context.closeSessions();

        if (cond.wait_for(lock, interval, [this]() -> bool { return quit; }))
            break;
    }
}


}
