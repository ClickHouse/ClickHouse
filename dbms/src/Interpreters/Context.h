#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <Core/Types.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Users.h>
#include <Parsers/IAST_fwd.h>
#include <Common/LRUCache.h>
#include <Common/MultiVersion.h>
#include <Common/ThreadPool.h>
#include "config_core.h"
#include <Storages/IStorage_fwd.h>
#include <Disks/DiskSpaceMonitor.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <Common/RemoteHostFilter.h>


namespace Poco
{
    namespace Net
    {
        class IPAddress;
    }
}

namespace zkutil
{
    class ZooKeeper;
}


namespace DB
{

struct ContextShared;
class Context;
class QuotaContext;
class EmbeddedDictionaries;
class ExternalDictionariesLoader;
class ExternalModelsLoader;
class InterserverIOHandler;
class BackgroundProcessingPool;
class BackgroundSchedulePool;
class MergeList;
class Cluster;
class Compiler;
class MarkCache;
class UncompressedCache;
class ProcessList;
class QueryStatus;
class Macros;
struct Progress;
class Clusters;
class QueryLog;
class QueryThreadLog;
class PartLog;
class TextLog;
class TraceLog;
class MetricLog;
struct MergeTreeSettings;
class IDatabase;
class DDLGuard;
class DDLWorker;
class ITableFunction;
class Block;
class ActionLocksManager;
using ActionLocksManagerPtr = std::shared_ptr<ActionLocksManager>;
class ShellCommand;
class ICompressionCodec;
class AccessControlManager;
class SettingsConstraints;
class RemoteHostFilter;

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

#if USE_EMBEDDED_COMPILER

class CompiledExpressionCache;

#endif

/// (database name, table name)
using DatabaseAndTableName = std::pair<String, String>;

/// Table -> set of table-views that make SELECT from it.
using ViewDependencies = std::map<DatabaseAndTableName, std::set<DatabaseAndTableName>>;
using Dependencies = std::vector<DatabaseAndTableName>;

using TableAndCreateAST = std::pair<StoragePtr, ASTPtr>;
using TableAndCreateASTs = std::map<String, TableAndCreateAST>;

/// Callback for external tables initializer
using ExternalTablesInitializer = std::function<void(Context &)>;

/// Callback for initialize input()
using InputInitializer = std::function<void(Context &, const StoragePtr &)>;
/// Callback for reading blocks of data from client for function input()
using InputBlocksReader = std::function<Block(Context &)>;

/// Scalar results of sub queries
using Scalars = std::map<String, Block>;

/// An empty interface for an arbitrary object that may be attached by a shared pointer
/// to query context, when using ClickHouse as a library.
struct IHostContext
{
    virtual ~IHostContext() = default;
};

using IHostContextPtr = std::shared_ptr<IHostContext>;

/** A set of known objects that can be used in the query.
  * Consists of a shared part (always common to all sessions and queries)
  *  and copied part (which can be its own for each session or query).
  *
  * Everything is encapsulated for all sorts of checks and locks.
  */
class Context
{
private:
    using Shared = std::shared_ptr<ContextShared>;
    Shared shared;

    ClientInfo client_info;
    ExternalTablesInitializer external_tables_initializer_callback;

    InputInitializer input_initializer_callback;
    InputBlocksReader input_blocks_reader;

    std::shared_ptr<QuotaContext> quota;           /// Current quota. By default - empty quota, that have no limits.
    bool is_quota_management_allowed = false;      /// Whether the current user is allowed to manage quotas via SQL commands.
    String current_database;
    Settings settings;                                  /// Setting for query execution.
    std::shared_ptr<const SettingsConstraints> settings_constraints;
    using ProgressCallback = std::function<void(const Progress & progress)>;
    ProgressCallback progress_callback;                 /// Callback for tracking progress of query execution.
    QueryStatus * process_list_elem = nullptr;   /// For tracking total resource usage for query.
    std::pair<String, String> insertion_table;  /// Saved insertion table in query context

    String default_format;  /// Format, used when server formats data by itself and if query does not have FORMAT specification.
                            /// Thus, used in HTTP interface. If not specified - then some globally default format is used.
    TableAndCreateASTs external_tables;     /// Temporary tables.
    Scalars scalars;
    StoragePtr view_source;                 /// Temporary StorageValues used to generate alias columns for materialized views
    Tables table_function_results;          /// Temporary tables obtained by execution of table functions. Keyed by AST tree id.
    Context * query_context = nullptr;
    Context * session_context = nullptr;    /// Session context or nullptr. Could be equal to this.
    Context * global_context = nullptr;     /// Global context. Could be equal to this.

    UInt64 session_close_cycle = 0;
    bool session_is_used = false;

    using SampleBlockCache = std::unordered_map<std::string, Block>;
    mutable SampleBlockCache sample_block_cache;

    using DatabasePtr = std::shared_ptr<IDatabase>;
    using Databases = std::map<String, std::shared_ptr<IDatabase>>;

    NameToNameMap query_parameters;   /// Dictionary with query parameters for prepared statements.
                                                     /// (key=name, value)

    IHostContextPtr host_context;  /// Arbitrary object that may used to attach some host specific information to query context,
                                   /// when using ClickHouse as a library in some project. For example, it may contain host
                                   /// logger, some query identification information, profiling guards, etc. This field is
                                   /// to be customized in HTTP and TCP servers by overloading the customizeContext(DB::Context&)
                                   /// methods.

    /// Use copy constructor or createGlobal() instead
    Context();

public:
    /// Create initial Context with ContextShared and etc.
    static Context createGlobal();

    Context(const Context &);
    Context & operator=(const Context &);
    ~Context();

    String getPath() const;
    String getTemporaryPath() const;
    String getFlagsPath() const;
    String getUserFilesPath() const;

    void setPath(const String & path);
    void setTemporaryPath(const String & path);
    void setFlagsPath(const String & path);
    void setUserFilesPath(const String & path);

    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    /// Global application configuration settings.
    void setConfig(const ConfigurationPtr & config);
    const Poco::Util::AbstractConfiguration & getConfigRef() const;

    AccessControlManager & getAccessControlManager();
    const AccessControlManager & getAccessControlManager() const;
    std::shared_ptr<QuotaContext> getQuota() const { return quota; }
    void checkQuotaManagementIsAllowed();

    /** Take the list of users, quotas and configuration profiles from this config.
      * The list of users is completely replaced.
      * The accumulated quota values are not reset if the quota is not deleted.
      */
    void setUsersConfig(const ConfigurationPtr & config);
    ConfigurationPtr getUsersConfig();

    // User property is a key-value pair from the configuration entry: users.<username>.databases.<db_name>.<table_name>.<key_name>
    bool hasUserProperty(const String & database, const String & table, const String & name) const;
    const String & getUserProperty(const String & database, const String & table, const String & name) const;

    /// Must be called before getClientInfo.
    void setUser(const String & name, const String & password, const Poco::Net::SocketAddress & address, const String & quota_key);

    /// Used by MySQL Secure Password Authentication plugin.
    std::shared_ptr<const User> getUser(const String & user_name);

    /// Compute and set actual user settings, client_info.current_user should be set
    void calculateUserSettings();

    /// We have to copy external tables inside executeQuery() to track limits. Therefore, set callback for it. Must set once.
    void setExternalTablesInitializer(ExternalTablesInitializer && initializer);
    /// This method is called in executeQuery() and will call the external tables initializer.
    void initializeExternalTablesIfSet();

    /// When input() is present we have to send columns structure to client
    void setInputInitializer(InputInitializer && initializer);
    /// This method is called in StorageInput::read while executing query
    void initializeInput(const StoragePtr & input_storage);

    /// Callback for read data blocks from client one by one for function input()
    void setInputBlocksReaderCallback(InputBlocksReader && reader);
    /// Get callback for reading data for input()
    InputBlocksReader getInputBlocksReaderCallback() const;
    void resetInputCallbacks();

    ClientInfo & getClientInfo() { return client_info; }
    const ClientInfo & getClientInfo() const { return client_info; }

    void addDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where);
    void removeDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where);
    Dependencies getDependencies(const String & database_name, const String & table_name) const;

    /// Functions where we can lock the context manually
    void addDependencyUnsafe(const DatabaseAndTableName & from, const DatabaseAndTableName & where);
    void removeDependencyUnsafe(const DatabaseAndTableName & from, const DatabaseAndTableName & where);

    /// Checking the existence of the table/database. Database can be empty - in this case the current database is used.
    bool isTableExist(const String & database_name, const String & table_name) const;
    bool isDatabaseExist(const String & database_name) const;
    bool isDictionaryExists(const String & database_name, const String & dictionary_name) const;
    bool isExternalTableExist(const String & table_name) const;
    bool hasDatabaseAccessRights(const String & database_name) const;

    bool hasDictionaryAccessRights(const String & dictionary_name) const;

    /** The parameter check_database_access_rights exists to not check the permissions of the database again,
      * when assertTableDoesntExist or assertDatabaseExists is called inside another function that already
      * made this check.
      */
    void assertTableDoesntExist(const String & database_name, const String & table_name, bool check_database_acccess_rights = true) const;
    void assertDatabaseExists(const String & database_name, bool check_database_acccess_rights = true) const;

    void assertDatabaseDoesntExist(const String & database_name) const;
    void checkDatabaseAccessRights(const std::string & database_name) const;

    const Scalars & getScalars() const;
    const Block & getScalar(const String & name) const;
    Tables getExternalTables() const;
    StoragePtr tryGetExternalTable(const String & table_name) const;
    StoragePtr getTable(const String & database_name, const String & table_name) const;
    StoragePtr tryGetTable(const String & database_name, const String & table_name) const;
    void addExternalTable(const String & table_name, const StoragePtr & storage, const ASTPtr & ast = {});
    void addScalar(const String & name, const Block & block);
    bool hasScalar(const String & name) const;
    StoragePtr tryRemoveExternalTable(const String & table_name);

    StoragePtr executeTableFunction(const ASTPtr & table_expression);

    void addViewSource(const StoragePtr & storage);
    StoragePtr getViewSource();

    void addDatabase(const String & database_name, const DatabasePtr & database);
    DatabasePtr detachDatabase(const String & database_name);

    /// Get an object that protects the table from concurrently executing multiple DDL operations.
    std::unique_ptr<DDLGuard> getDDLGuard(const String & database, const String & table) const;

    String getCurrentDatabase() const;
    String getCurrentQueryId() const;

    /// Id of initiating query for distributed queries; or current query id if it's not a distributed query.
    String getInitialQueryId() const;

    void setCurrentDatabase(const String & name);
    void setCurrentQueryId(const String & query_id);

    void killCurrentQuery();

    void setInsertionTable(std::pair<String, String> && db_and_table) { insertion_table = db_and_table; }
    const std::pair<String, String> & getInsertionTable() const { return insertion_table; }

    String getDefaultFormat() const;    /// If default_format is not specified, some global default format is returned.
    void setDefaultFormat(const String & name);

    MultiVersion<Macros>::Version getMacros() const;
    void setMacros(std::unique_ptr<Macros> && macros);

    Settings getSettings() const;
    void setSettings(const Settings & settings_);

    /// Set settings by name.
    void setSetting(const String & name, const String & value);
    void setSetting(const String & name, const Field & value);
    void applySettingChange(const SettingChange & change);
    void applySettingsChanges(const SettingsChanges & changes);

    /// Checks the constraints.
    void checkSettingsConstraints(const SettingChange & change);
    void checkSettingsConstraints(const SettingsChanges & changes);

    /// Returns the current constraints (can return null).
    std::shared_ptr<const SettingsConstraints> getSettingsConstraints() const { return settings_constraints; }

    const EmbeddedDictionaries & getEmbeddedDictionaries() const;
    const ExternalDictionariesLoader & getExternalDictionariesLoader() const;
    const ExternalModelsLoader & getExternalModelsLoader() const;
    EmbeddedDictionaries & getEmbeddedDictionaries();
    ExternalDictionariesLoader & getExternalDictionariesLoader();
    ExternalModelsLoader & getExternalModelsLoader();
    void tryCreateEmbeddedDictionaries() const;

    /// I/O formats.
    BlockInputStreamPtr getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size) const;
    BlockOutputStreamPtr getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample) const;

    OutputFormatPtr getOutputFormatProcessor(const String & name, WriteBuffer & buf, const Block & sample) const;

    InterserverIOHandler & getInterserverIOHandler();

    /// How other servers can access this for downloading replicated data.
    void setInterserverIOAddress(const String & host, UInt16 port);
    std::pair<String, UInt16> getInterserverIOAddress() const;

    /// Credentials which server will use to communicate with others
    void setInterserverCredentials(const String & user, const String & password);
    std::pair<String, String> getInterserverCredentials() const;

    /// Interserver requests scheme (http or https)
    void setInterserverScheme(const String & scheme);
    String getInterserverScheme() const;

    /// Storage of allowed hosts from config.xml
    void setRemoteHostFilter(const Poco::Util::AbstractConfiguration & config);
    const RemoteHostFilter & getRemoteHostFilter() const;

    /// The port that the server listens for executing SQL queries.
    UInt16 getTCPPort() const;

    std::optional<UInt16> getTCPPortSecure() const;

    /// Get query for the CREATE table.
    ASTPtr getCreateTableQuery(const String & database_name, const String & table_name) const;
    ASTPtr getCreateExternalTableQuery(const String & table_name) const;
    ASTPtr getCreateDatabaseQuery(const String & database_name) const;
    ASTPtr getCreateDictionaryQuery(const String & database_name, const String & dictionary_name) const;

    const DatabasePtr getDatabase(const String & database_name) const;
    DatabasePtr getDatabase(const String & database_name);
    const DatabasePtr tryGetDatabase(const String & database_name) const;
    DatabasePtr tryGetDatabase(const String & database_name);

    const Databases getDatabases() const;
    Databases getDatabases();

    std::shared_ptr<Context> acquireSession(const String & session_id, std::chrono::steady_clock::duration timeout, bool session_check) const;
    void releaseSession(const String & session_id, std::chrono::steady_clock::duration timeout);

    /// Close sessions, that has been expired. Returns how long to wait for next session to be expired, if no new sessions will be added.
    std::chrono::steady_clock::duration closeSessions() const;

    /// For methods below you may need to acquire a lock by yourself.
    std::unique_lock<std::recursive_mutex> getLock() const;

    const Context & getQueryContext() const;
    Context & getQueryContext();
    bool hasQueryContext() const { return query_context != nullptr; }

    const Context & getSessionContext() const;
    Context & getSessionContext();
    bool hasSessionContext() const { return session_context != nullptr; }

    const Context & getGlobalContext() const;
    Context & getGlobalContext();
    bool hasGlobalContext() const { return global_context != nullptr; }

    void setQueryContext(Context & context_) { query_context = &context_; }
    void setSessionContext(Context & context_) { session_context = &context_; }

    void makeQueryContext() { query_context = this; }
    void makeSessionContext() { session_context = this; }
    void makeGlobalContext() { global_context = this; }

    const Settings & getSettingsRef() const { return settings; }
    Settings & getSettingsRef() { return settings; }

    void setProgressCallback(ProgressCallback callback);
    /// Used in InterpreterSelectQuery to pass it to the IBlockInputStream.
    ProgressCallback getProgressCallback() const;

    /** Set in executeQuery and InterpreterSelectQuery. Then it is used in IBlockInputStream,
      *  to update and monitor information about the total number of resources spent for the query.
      */
    void setProcessListElement(QueryStatus * elem);
    /// Can return nullptr if the query was not inserted into the ProcessList.
    QueryStatus * getProcessListElement() const;

    /// List all queries.
    ProcessList & getProcessList();
    const ProcessList & getProcessList() const;

    MergeList & getMergeList();
    const MergeList & getMergeList() const;

    /// If the current session is expired at the time of the call, synchronously creates and returns a new session with the startNewSession() call.
    /// If no ZooKeeper configured, throws an exception.
    std::shared_ptr<zkutil::ZooKeeper> getZooKeeper() const;
    /// Has ready or expired ZooKeeper
    bool hasZooKeeper() const;
    /// Reset current zookeeper session. Do not create a new one.
    void resetZooKeeper() const;

    /// Create a cache of uncompressed blocks of specified size. This can be done only once.
    void setUncompressedCache(size_t max_size_in_bytes);
    std::shared_ptr<UncompressedCache> getUncompressedCache() const;
    void dropUncompressedCache() const;

    /// Create a cache of marks of specified size. This can be done only once.
    void setMarkCache(size_t cache_size_in_bytes);
    std::shared_ptr<MarkCache> getMarkCache() const;
    void dropMarkCache() const;

    /** Clear the caches of the uncompressed blocks and marks.
      * This is usually done when renaming tables, changing the type of columns, deleting a table.
      *  - since caches are linked to file names, and become incorrect.
      *  (when deleting a table - it is necessary, since in its place another can appear)
      * const - because the change in the cache is not considered significant.
      */
    void dropCaches() const;

    BackgroundProcessingPool & getBackgroundPool();
    BackgroundProcessingPool & getBackgroundMovePool();
    BackgroundSchedulePool & getSchedulePool();

    void setDDLWorker(std::unique_ptr<DDLWorker> ddl_worker);
    DDLWorker & getDDLWorker() const;

    Clusters & getClusters() const;
    std::shared_ptr<Cluster> getCluster(const std::string & cluster_name) const;
    std::shared_ptr<Cluster> tryGetCluster(const std::string & cluster_name) const;
    void setClustersConfig(const ConfigurationPtr & config, const String & config_name = "remote_servers");
    /// Sets custom cluster, but doesn't update configuration
    void setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster);
    void reloadClusterConfig();

    Compiler & getCompiler();

    /// Call after initialization before using system logs. Call for global context.
    void initializeSystemLogs();

    void initializeTraceCollector();
    bool hasTraceCollector();

    /// Nullptr if the query log is not ready for this moment.
    std::shared_ptr<QueryLog> getQueryLog();
    std::shared_ptr<QueryThreadLog> getQueryThreadLog();
    std::shared_ptr<TraceLog> getTraceLog();
    std::shared_ptr<TextLog> getTextLog();
    std::shared_ptr<MetricLog> getMetricLog();

    /// Returns an object used to log opertaions with parts if it possible.
    /// Provide table name to make required cheks.
    std::shared_ptr<PartLog> getPartLog(const String & part_database);

    const MergeTreeSettings & getMergeTreeSettings() const;

    /// Prevents DROP TABLE if its size is greater than max_size (50GB by default, max_size=0 turn off this check)
    void setMaxTableSizeToDrop(size_t max_size);
    void checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const;

    /// Prevents DROP PARTITION if its size is greater than max_size (50GB by default, max_size=0 turn off this check)
    void setMaxPartitionSizeToDrop(size_t max_size);
    void checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const;

    /// Lets you select the compression codec according to the conditions described in the configuration file.
    std::shared_ptr<ICompressionCodec> chooseCompressionCodec(size_t part_size, double part_size_ratio) const;

    DiskSelector & getDiskSelector() const;

    /// Provides storage disks
    const DiskPtr & getDisk(const String & name) const;
    const DiskPtr & getDefaultDisk() const { return getDisk("default"); }

    StoragePolicySelector & getStoragePolicySelector() const;

    /// Provides storage politics schemes
    const StoragePolicyPtr & getStoragePolicy(const String &name) const;

    /// Get the server uptime in seconds.
    time_t getUptimeSeconds() const;

    using ConfigReloadCallback = std::function<void()>;
    void setConfigReloadCallback(ConfigReloadCallback && callback);
    void reloadConfig() const;

    void shutdown();

    ActionLocksManagerPtr getActionLocksManager();

    enum class ApplicationType
    {
        SERVER,         /// The program is run as clickhouse-server daemon (default behavior)
        CLIENT,         /// clickhouse-client
        LOCAL           /// clickhouse-local
    };

    ApplicationType getApplicationType() const;
    void setApplicationType(ApplicationType type);

    /// Sets default_profile and system_profile, must be called once during the initialization
    void setDefaultProfiles(const Poco::Util::AbstractConfiguration & config);
    String getDefaultProfileName() const;
    String getSystemProfileName() const;

    /// Base path for format schemas
    String getFormatSchemaPath() const;
    void setFormatSchemaPath(const String & path);

    /// User name and session identifier. Named sessions are local to users.
    using SessionKey = std::pair<String, String>;

    SampleBlockCache & getSampleBlockCache() const;

    /// Query parameters for prepared statements.
    bool hasQueryParameters() const;
    const NameToNameMap & getQueryParameters() const;
    void setQueryParameter(const String & name, const String & value);
    void setQueryParameters(const NameToNameMap & parameters) { query_parameters = parameters; }

#if USE_EMBEDDED_COMPILER
    std::shared_ptr<CompiledExpressionCache> getCompiledExpressionCache() const;
    void setCompiledExpressionCache(size_t cache_size);
    void dropCompiledExpressionCache() const;
#endif

    /// Add started bridge command. It will be killed after context destruction
    void addXDBCBridgeCommand(std::unique_ptr<ShellCommand> cmd) const;

    IHostContextPtr & getHostContext();
    const IHostContextPtr & getHostContext() const;

    struct MySQLWireContext
    {
        uint8_t sequence_id = 0;
        uint32_t client_capabilities = 0;
        size_t max_packet_size = 0;
    };

    MySQLWireContext mysql;
private:
    /** Check if the current client has access to the specified database.
      * If access is denied, throw an exception.
      * NOTE: This method should always be called when the `shared->mutex` mutex is acquired.
      */
    void checkDatabaseAccessRightsImpl(const std::string & database_name) const;

    void setProfile(const String & profile);

    EmbeddedDictionaries & getEmbeddedDictionariesImpl(bool throw_on_error) const;

    StoragePtr getTableImpl(const String & database_name, const String & table_name, Exception * exception) const;

    SessionKey getSessionKey(const String & session_id) const;

    /// Session will be closed after specified timeout.
    void scheduleCloseSession(const SessionKey & key, std::chrono::steady_clock::duration timeout);

    void checkCanBeDropped(const String & database, const String & table, const size_t & size, const size_t & max_size_to_drop) const;
};


/// Allows executing DDL query only in one thread.
/// Puts an element into the map, locks tables's mutex, counts how much threads run parallel query on the table,
/// when counter is 0 erases element in the destructor.
/// If the element already exists in the map, waits, when ddl query will be finished in other thread.
class DDLGuard
{
public:
    struct Entry
    {
        std::unique_ptr<std::mutex> mutex;
        UInt32 counter;
    };

    /// Element name -> (mutex, counter).
    /// NOTE: using std::map here (and not std::unordered_map) to avoid iterator invalidation on insertion.
    using Map = std::map<String, Entry>;

    DDLGuard(Map & map_, std::unique_lock<std::mutex> guards_lock_, const String & elem);
    ~DDLGuard();

private:
    Map & map;
    Map::iterator it;
    std::unique_lock<std::mutex> guards_lock;
    std::unique_lock<std::mutex> table_lock;
};


class SessionCleaner
{
public:
    SessionCleaner(Context & context_)
        : context{context_}
    {
    }
    ~SessionCleaner();

private:
    void run();

    Context & context;

    std::mutex mutex;
    std::condition_variable cond;
    std::atomic<bool> quit{false};
    ThreadFromGlobalPool thread{&SessionCleaner::run, this};
};

}
