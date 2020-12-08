#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <common/types.h>
#include <Core/UUID.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/IAST_fwd.h>
#include <Access/RowPolicy.h>
#include <Common/MultiVersion.h>
#include <Common/ThreadPool.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Storages/IStorage_fwd.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <Common/RemoteHostFilter.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif


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
class ContextAccess;
struct User;
using UserPtr = std::shared_ptr<const User>;
struct EnabledRolesInfo;
class EnabledRowPolicies;
class EnabledQuota;
struct QuotaUsage;
class AccessFlags;
struct AccessRightsElement;
class AccessRightsElements;
class EmbeddedDictionaries;
class ExternalDictionariesLoader;
class ExternalModelsLoader;
class InterserverIOHandler;
class BackgroundSchedulePool;
class MergeList;
class ReplicatedFetchList;
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
class AsynchronousMetricLog;
class OpenTelemetrySpanLog;
struct MergeTreeSettings;
class StorageS3Settings;
class IDatabase;
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
struct StorageID;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class DiskSelector;
using DiskSelectorPtr = std::shared_ptr<const DiskSelector>;
using DisksMap = std::map<String, DiskPtr>;
class StoragePolicy;
using StoragePolicyPtr = std::shared_ptr<const StoragePolicy>;
using StoragePoliciesMap = std::map<String, StoragePolicyPtr>;
class StoragePolicySelector;
using StoragePolicySelectorPtr = std::shared_ptr<const StoragePolicySelector>;

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;
class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;
struct NamedSession;
struct BackgroundTaskSchedulingSettings;


#if USE_EMBEDDED_COMPILER
class CompiledExpressionCache;
#endif

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

/// A small class which owns ContextShared.
/// We don't use something like unique_ptr directly to allow ContextShared type to be incomplete.
struct SharedContextHolder
{
    ~SharedContextHolder();
    SharedContextHolder();
    SharedContextHolder(std::unique_ptr<ContextShared> shared_context);
    SharedContextHolder(SharedContextHolder &&) noexcept;

    SharedContextHolder & operator=(SharedContextHolder &&);

    ContextShared * get() const { return shared.get(); }
    void reset();
private:
    std::unique_ptr<ContextShared> shared;
};

/** A set of known objects that can be used in the query.
  * Consists of a shared part (always common to all sessions and queries)
  *  and copied part (which can be its own for each session or query).
  *
  * Everything is encapsulated for all sorts of checks and locks.
  */
class Context
{
private:
    ContextShared * shared;

    ClientInfo client_info;
    ExternalTablesInitializer external_tables_initializer_callback;

    InputInitializer input_initializer_callback;
    InputBlocksReader input_blocks_reader;

    std::optional<UUID> user_id;
    boost::container::flat_set<UUID> current_roles;
    bool use_default_roles = false;
    std::shared_ptr<const ContextAccess> access;
    std::shared_ptr<const EnabledRowPolicies> initial_row_policy;
    String current_database;
    Settings settings;                                  /// Setting for query execution.
    using ProgressCallback = std::function<void(const Progress & progress)>;
    ProgressCallback progress_callback;                 /// Callback for tracking progress of query execution.
    QueryStatus * process_list_elem = nullptr;   /// For tracking total resource usage for query.
    StorageID insertion_table = StorageID::createEmpty();  /// Saved insertion table in query context

    String default_format;  /// Format, used when server formats data by itself and if query does not have FORMAT specification.
                            /// Thus, used in HTTP interface. If not specified - then some globally default format is used.
    TemporaryTablesMapping external_tables_mapping;
    Scalars scalars;

    //TODO maybe replace with temporary tables?
    StoragePtr view_source;                 /// Temporary StorageValues used to generate alias columns for materialized views
    Tables table_function_results;          /// Temporary tables obtained by execution of table functions. Keyed by AST tree id.

    Context * query_context = nullptr;
    Context * session_context = nullptr;    /// Session context or nullptr. Could be equal to this.
    Context * global_context = nullptr;     /// Global context. Could be equal to this.

public:
    // Top-level OpenTelemetry trace context for the query. Makes sense only for
    // a query context.
    OpenTelemetryTraceContext query_trace_context;

private:
    friend class NamedSessions;

    using SampleBlockCache = std::unordered_map<std::string, Block>;
    mutable SampleBlockCache sample_block_cache;

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
    static Context createGlobal(ContextShared * shared);
    static SharedContextHolder createShared();

    Context(const Context &);
    Context & operator=(const Context &);
    ~Context();

    String getPath() const;
    String getFlagsPath() const;
    String getUserFilesPath() const;
    String getDictionariesLibPath() const;

    VolumePtr getTemporaryVolume() const;

    void setPath(const String & path);
    void setFlagsPath(const String & path);
    void setUserFilesPath(const String & path);
    void setDictionariesLibPath(const String & path);

    VolumePtr setTemporaryStorage(const String & path, const String & policy_name = "");

    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    /// Global application configuration settings.
    void setConfig(const ConfigurationPtr & config);
    const Poco::Util::AbstractConfiguration & getConfigRef() const;

    AccessControlManager & getAccessControlManager();
    const AccessControlManager & getAccessControlManager() const;

    /// Sets external authenticators config (LDAP).
    void setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config);

    /** Take the list of users, quotas and configuration profiles from this config.
      * The list of users is completely replaced.
      * The accumulated quota values are not reset if the quota is not deleted.
      */
    void setUsersConfig(const ConfigurationPtr & config);
    ConfigurationPtr getUsersConfig();

    /// Sets the current user, checks the password and that the specified host is allowed.
    /// Must be called before getClientInfo.
    void setUser(const String & name, const String & password, const Poco::Net::SocketAddress & address);

    /// Sets the current user, *do not checks the password and that the specified host is allowed*.
    /// Must be called before getClientInfo.
    ///
    /// (Used only internally in cluster, if the secret matches)
    void setUserWithoutCheckingPassword(const String & name, const Poco::Net::SocketAddress & address);

    void setQuotaKey(String quota_key_);

    UserPtr getUser() const;
    String getUserName() const;
    std::optional<UUID> getUserID() const;

    void setCurrentRoles(const boost::container::flat_set<UUID> & current_roles_);
    void setCurrentRolesDefault();
    boost::container::flat_set<UUID> getCurrentRoles() const;
    boost::container::flat_set<UUID> getEnabledRoles() const;
    std::shared_ptr<const EnabledRolesInfo> getRolesInfo() const;

    /// Checks access rights.
    /// Empty database means the current database.
    void checkAccess(const AccessFlags & flags) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::string_view & column) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::vector<std::string_view> & columns) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id, const Strings & columns) const;
    void checkAccess(const AccessRightsElement & element) const;
    void checkAccess(const AccessRightsElements & elements) const;

    std::shared_ptr<const ContextAccess> getAccess() const;

    ASTPtr getRowPolicyCondition(const String & database, const String & table_name, RowPolicy::ConditionType type) const;

    /// Sets an extra row policy based on `client_info.initial_user`, if it exists.
    /// TODO: we need a better solution here. It seems we should pass the initial row policy
    /// because a shard is allowed to don't have the initial user or it may be another user with the same name.
    void setInitialRowPolicy();

    std::shared_ptr<const EnabledQuota> getQuota() const;
    std::optional<QuotaUsage> getQuotaUsage() const;

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

    enum StorageNamespace
    {
         ResolveGlobal = 1u,                                           /// Database name must be specified
         ResolveCurrentDatabase = 2u,                                  /// Use current database
         ResolveOrdinary = ResolveGlobal | ResolveCurrentDatabase,     /// If database name is not specified, use current database
         ResolveExternal = 4u,                                         /// Try get external table
         ResolveAll = ResolveExternal | ResolveOrdinary                /// If database name is not specified, try get external table,
                                                                       ///    if external table not found use current database.
    };

    String resolveDatabase(const String & database_name) const;
    StorageID resolveStorageID(StorageID storage_id, StorageNamespace where = StorageNamespace::ResolveAll) const;
    StorageID tryResolveStorageID(StorageID storage_id, StorageNamespace where = StorageNamespace::ResolveAll) const;
    StorageID resolveStorageIDImpl(StorageID storage_id, StorageNamespace where, std::optional<Exception> * exception) const;

    Tables getExternalTables() const;
    void addExternalTable(const String & table_name, TemporaryTableHolder && temporary_table);
    std::shared_ptr<TemporaryTableHolder> removeExternalTable(const String & table_name);

    const Scalars & getScalars() const;
    const Block & getScalar(const String & name) const;
    void addScalar(const String & name, const Block & block);
    bool hasScalar(const String & name) const;

    StoragePtr executeTableFunction(const ASTPtr & table_expression);

    void addViewSource(const StoragePtr & storage);
    StoragePtr getViewSource();

    String getCurrentDatabase() const;
    String getCurrentQueryId() const;

    /// Id of initiating query for distributed queries; or current query id if it's not a distributed query.
    String getInitialQueryId() const;

    void setCurrentDatabase(const String & name);
    /// Set current_database for global context. We don't validate that database
    /// exists because it should be set before databases loading.
    void setCurrentDatabaseNameInGlobalContext(const String & name);
    void setCurrentQueryId(const String & query_id);

    void killCurrentQuery();

    void setInsertionTable(StorageID db_and_table) { insertion_table = std::move(db_and_table); }
    const StorageID & getInsertionTable() const { return insertion_table; }

    String getDefaultFormat() const;    /// If default_format is not specified, some global default format is returned.
    void setDefaultFormat(const String & name);

    MultiVersion<Macros>::Version getMacros() const;
    void setMacros(std::unique_ptr<Macros> && macros);

    Settings getSettings() const;
    void setSettings(const Settings & settings_);

    /// Set settings by name.
    void setSetting(const StringRef & name, const String & value);
    void setSetting(const StringRef & name, const Field & value);
    void applySettingChange(const SettingChange & change);
    void applySettingsChanges(const SettingsChanges & changes);

    /// Checks the constraints.
    void checkSettingsConstraints(const SettingChange & change) const;
    void checkSettingsConstraints(const SettingsChanges & changes) const;
    void checkSettingsConstraints(SettingsChanges & changes) const;
    void clampToSettingsConstraints(SettingsChanges & changes) const;

    /// Returns the current constraints (can return null).
    std::shared_ptr<const SettingsConstraints> getSettingsConstraints() const;

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

    /// Allow to use named sessions. The thread will be run to cleanup sessions after timeout has expired.
    /// The method must be called at the server startup.
    void enableNamedSessions();

    std::shared_ptr<NamedSession> acquireNamedSession(const String & session_id, std::chrono::steady_clock::duration timeout, bool session_check);

    /// For methods below you may need to acquire the context lock by yourself.

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
    void makeGlobalContext() { initGlobal(); global_context = this; }

    const Settings & getSettingsRef() const { return settings; }

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

    ReplicatedFetchList & getReplicatedFetchList();
    const ReplicatedFetchList & getReplicatedFetchList() const;

    /// If the current session is expired at the time of the call, synchronously creates and returns a new session with the startNewSession() call.
    /// If no ZooKeeper configured, throws an exception.
    std::shared_ptr<zkutil::ZooKeeper> getZooKeeper() const;
    /// Same as above but return a zookeeper connection from auxiliary_zookeepers configuration entry.
    std::shared_ptr<zkutil::ZooKeeper> getAuxiliaryZooKeeper(const String & name) const;

    /// Set auxiliary zookeepers configuration at server starting or configuration reloading.
    void reloadAuxiliaryZooKeepersConfigIfChanged(const ConfigurationPtr & config);
    /// Has ready or expired ZooKeeper
    bool hasZooKeeper() const;
    /// Has ready or expired auxiliary ZooKeeper
    bool hasAuxiliaryZooKeeper(const String & name) const;
    /// Reset current zookeeper session. Do not create a new one.
    void resetZooKeeper() const;
    // Reload Zookeeper
    void reloadZooKeeperIfChanged(const ConfigurationPtr & config) const;

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

    /// Settings for MergeTree background tasks stored in config.xml
    BackgroundTaskSchedulingSettings getBackgroundProcessingTaskSchedulingSettings() const;
    BackgroundTaskSchedulingSettings getBackgroundMoveTaskSchedulingSettings() const;

    BackgroundSchedulePool & getBufferFlushSchedulePool() const;
    BackgroundSchedulePool & getSchedulePool() const;
    BackgroundSchedulePool & getDistributedSchedulePool() const;

    /// Has distributed_ddl configuration or not.
    bool hasDistributedDDL() const;
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

    /// Call after initialization before using trace collector.
    void initializeTraceCollector();

    bool hasTraceCollector() const;

    /// Nullptr if the query log is not ready for this moment.
    std::shared_ptr<QueryLog> getQueryLog();
    std::shared_ptr<QueryThreadLog> getQueryThreadLog();
    std::shared_ptr<TraceLog> getTraceLog();
    std::shared_ptr<TextLog> getTextLog();
    std::shared_ptr<MetricLog> getMetricLog();
    std::shared_ptr<AsynchronousMetricLog> getAsynchronousMetricLog();
    std::shared_ptr<OpenTelemetrySpanLog> getOpenTelemetrySpanLog();

    /// Returns an object used to log operations with parts if it possible.
    /// Provide table name to make required checks.
    std::shared_ptr<PartLog> getPartLog(const String & part_database);

    const MergeTreeSettings & getMergeTreeSettings() const;
    const MergeTreeSettings & getReplicatedMergeTreeSettings() const;
    const StorageS3Settings & getStorageS3Settings() const;

    /// Prevents DROP TABLE if its size is greater than max_size (50GB by default, max_size=0 turn off this check)
    void setMaxTableSizeToDrop(size_t max_size);
    void checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const;

    /// Prevents DROP PARTITION if its size is greater than max_size (50GB by default, max_size=0 turn off this check)
    void setMaxPartitionSizeToDrop(size_t max_size);
    void checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const;

    /// Lets you select the compression codec according to the conditions described in the configuration file.
    std::shared_ptr<ICompressionCodec> chooseCompressionCodec(size_t part_size, double part_size_ratio) const;


    /// Provides storage disks
    DiskPtr getDisk(const String & name) const;

    StoragePoliciesMap getPoliciesMap() const;
    DisksMap getDisksMap() const;
    void updateStorageConfiguration(const Poco::Util::AbstractConfiguration & config);

    /// Provides storage politics schemes
    StoragePolicyPtr getStoragePolicy(const String & name) const;

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
    std::unique_lock<std::recursive_mutex> getLock() const;

    void initGlobal();

    /// Compute and set actual user settings, client_info.current_user should be set
    void calculateAccessRights();

    template <typename... Args>
    void checkAccessImpl(const Args &... args) const;

    void setProfile(const String & profile);

    EmbeddedDictionaries & getEmbeddedDictionariesImpl(bool throw_on_error) const;

    void checkCanBeDropped(const String & database, const String & table, const size_t & size, const size_t & max_size_to_drop) const;

    StoragePolicySelectorPtr getStoragePolicySelector(std::lock_guard<std::mutex> & lock) const;

    DiskSelectorPtr getDiskSelector(std::lock_guard<std::mutex> & /* lock */) const;

    /// If the password is not set, the password will not be checked
    void setUserImpl(const String & name, const std::optional<String> & password, const Poco::Net::SocketAddress & address);
};


class NamedSessions;

/// User name and session identifier. Named sessions are local to users.
using NamedSessionKey = std::pair<String, String>;

/// Named sessions. The user could specify session identifier to reuse settings and temporary tables in subsequent requests.
struct NamedSession
{
    NamedSessionKey key;
    UInt64 close_cycle = 0;
    Context context;
    std::chrono::steady_clock::duration timeout;
    NamedSessions & parent;

    NamedSession(NamedSessionKey key_, Context & context_, std::chrono::steady_clock::duration timeout_, NamedSessions & parent_)
        : key(key_), context(context_), timeout(timeout_), parent(parent_)
    {
    }

    void release();
};

}
