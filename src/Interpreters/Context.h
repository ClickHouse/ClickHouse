#pragma once

#include <base/types.h>
#include <Common/MultiVersion.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/IThrottler.h>
#include <Common/SettingSource.h>
#include <Common/SharedMutex.h>
#include <Common/SharedMutexHelper.h>
#include <Common/StopToken.h>
#include <Core/UUID.h>
#include <Core/ParallelReplicasMode.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <Disks/IO/getThreadPoolReader.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Parsers/IAST_fwd.h>
#include <Server/HTTP/HTTPContext.h>
#include <Storages/IStorage_fwd.h>
#include <Backups/BackupsInMemoryHolder.h>

#include <Poco/AutoPtr.h>

#include "config.h"

#include <functional>
#include <memory>
#include <mutex>
#include <optional>


namespace Poco::Net
{
class IPAddress;
class SocketAddress;
}
namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}
namespace Coordination
{
    struct Request;
    using RequestPtr = std::shared_ptr<Request>;
    using Requests = std::vector<RequestPtr>;
}

struct OvercommitTracker;

namespace DB
{

class ASTSelectQuery;

class SystemLogs;

struct ContextSharedPart;
class ContextAccess;
class ContextAccessWrapper;
class Field;
struct User;
using UserPtr = std::shared_ptr<const User>;
struct SettingsProfilesInfo;
struct EnabledRolesInfo;
struct RowPolicyFilter;
using RowPolicyFilterPtr = std::shared_ptr<const RowPolicyFilter>;
class EnabledQuota;
struct QuotaUsage;
class AccessFlags;
struct AccessRightsElement;
class AccessRightsElements;
enum class RowPolicyFilterType : uint8_t;
struct RolesOrUsersSet;
class EmbeddedDictionaries;
class ExternalDictionariesLoader;
class ExternalUserDefinedExecutableFunctionsLoader;
class IUserDefinedSQLObjectsStorage;
class IWorkloadEntityStorage;
class InterserverCredentials;
using InterserverCredentialsPtr = std::shared_ptr<const InterserverCredentials>;
class InterserverIOHandler;
class AsynchronousMetrics;
class BackgroundSchedulePool;
class MergeList;
class MovesList;
class ReplicatedFetchList;
class RefreshSet;
class Cluster;
class Compiler;
class MarkCache;
class PrimaryIndexCache;
class PageCache;
class MMappedFileCache;
class UncompressedCache;
class IcebergMetadataFilesCache;
class VectorSimilarityIndexCache;
class ProcessList;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class Macros;
struct Progress;
struct FileProgress;
class Clusters;
class QueryResultCache;
class QueryConditionCache;
class ISystemLog;
class QueryLog;
class QueryMetricLog;
class QueryThreadLog;
class QueryViewsLog;
class PartLog;
class TextLog;
class TraceLog;
class MetricLog;
class TransposedMetricLog;
class LatencyLog;
class AsynchronousMetricLog;
class OpenTelemetrySpanLog;
class ZooKeeperLog;
class SessionLog;
class BackupsWorker;
class TransactionsInfoLog;
class ProcessorsProfileLog;
class FilesystemCacheLog;
class FilesystemReadPrefetchesLog;
class ObjectStorageQueueLog;
class AsynchronousInsertLog;
class BackupLog;
class BlobStorageLog;
class IAsynchronousReader;
class IOUringReader;
struct MergeTreeSettings;
struct DistributedSettings;
struct InitialAllRangesAnnouncement;
struct ParallelReadRequest;
struct ParallelReadResponse;
class S3SettingsByEndpoint;
class AzureSettingsByEndpoint;
class IDatabase;
class DDLWorker;
class ITableFunction;
using TableFunctionPtr = std::shared_ptr<ITableFunction>;
class Block;
class ActionLocksManager;
using ActionLocksManagerPtr = std::shared_ptr<ActionLocksManager>;
class ShellCommand;
class ICompressionCodec;
class AccessControl;
class GSSAcceptorContext;
struct Settings;
struct SettingChange;
class SettingsChanges;
struct SettingsConstraintsAndProfileIDs;
struct AlterSettingsProfileElements;
class RemoteHostFilter;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class DiskSelector;
using DiskSelectorPtr = std::shared_ptr<const DiskSelector>;
using DisksMap = std::map<String, DiskPtr>;
class IStoragePolicy;
using StoragePolicyPtr = std::shared_ptr<const IStoragePolicy>;
using StoragePoliciesMap = std::map<String, StoragePolicyPtr>;
class StoragePolicySelector;
using StoragePolicySelectorPtr = std::shared_ptr<const StoragePolicySelector>;
class ServerType;
template <class Queue>
class MergeTreeBackgroundExecutor;
class AsyncLoader;
class HTTPHeaderFilter;
struct AsyncReadCounters;
struct ICgroupsReader;

struct TemporaryTableHolder;
using TemporaryTablesMapping = std::map<String, std::shared_ptr<TemporaryTableHolder>>;

using ClusterPtr = std::shared_ptr<Cluster>;

class LoadTask;
using LoadTaskPtr = std::shared_ptr<LoadTask>;
using LoadTaskPtrs = std::vector<LoadTaskPtr>;

class IClassifier;
using ClassifierPtr = std::shared_ptr<IClassifier>;
class IResourceManager;
using ResourceManagerPtr = std::shared_ptr<IResourceManager>;

/// Scheduling policy can be changed using `background_merges_mutations_scheduling_policy` config option.
/// By default concurrent merges are scheduled using "round_robin" to ensure fair and starvation-free operation.
/// Previously in heavily overloaded shards big merges could possibly be starved by smaller
/// merges due to the use of strict priority scheduling "shortest_task_first".
class DynamicRuntimeQueue;
using MergeMutateBackgroundExecutor = MergeTreeBackgroundExecutor<DynamicRuntimeQueue>;
using MergeMutateBackgroundExecutorPtr = std::shared_ptr<MergeMutateBackgroundExecutor>;

class RoundRobinRuntimeQueue;
using OrdinaryBackgroundExecutor = MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>;
using OrdinaryBackgroundExecutorPtr = std::shared_ptr<OrdinaryBackgroundExecutor>;
struct PartUUIDs;
using PartUUIDsPtr = std::shared_ptr<PartUUIDs>;
class KeeperDispatcher;
struct WriteSettings;

class IInputFormat;
class IOutputFormat;
using InputFormatPtr = std::shared_ptr<IInputFormat>;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;
class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;
struct NamedSession;
struct BackgroundTaskSchedulingSettings;

#if USE_NLP
    class SynonymsExtensions;
    class Lemmatizers;
#endif

class ZooKeeperMetadataTransaction;
using ZooKeeperMetadataTransactionPtr = std::shared_ptr<ZooKeeperMetadataTransaction>;

class AsynchronousInsertQueue;

/// Callback for external tables initializer
using ExternalTablesInitializer = std::function<void(ContextPtr)>;

/// Callback for initialize input()
using InputInitializer = std::function<void(ContextPtr, const StoragePtr &)>;
/// Callback for reading blocks of data from client for function input()
using InputBlocksReader = std::function<Block(ContextPtr)>;

/// Used in distributed task processing
struct ClusterFunctionReadTaskResponse;
using ClusterFunctionReadTaskResponsePtr = std::shared_ptr<ClusterFunctionReadTaskResponse>;
using ClusterFunctionReadTaskCallback = std::function<ClusterFunctionReadTaskResponsePtr()>;

using MergeTreeAllRangesCallback = std::function<void(InitialAllRangesAnnouncement)>;
using MergeTreeReadTaskCallback = std::function<std::optional<ParallelReadResponse>(ParallelReadRequest)>;

using BlockMarshallingCallback = std::function<Block(const Block & block)>;

struct QueryPlanAndSets;
using QueryPlanDeserializationCallback = std::function<std::shared_ptr<QueryPlanAndSets>()>;

class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;

class PreparedSetsCache;
using PreparedSetsCachePtr = std::shared_ptr<PreparedSetsCache>;

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;
using PartitionIdToMaxBlockPtr = std::shared_ptr<const PartitionIdToMaxBlock>;

class SessionTracker;

struct ServerSettings;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

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
    explicit SharedContextHolder(std::unique_ptr<ContextSharedPart> shared_context);
    SharedContextHolder(SharedContextHolder &&) noexcept;

    SharedContextHolder & operator=(SharedContextHolder &&) noexcept;

    ContextSharedPart * get() const { return shared.get(); }
    void reset();

private:
    std::unique_ptr<ContextSharedPart> shared;
};

class ContextSharedMutex : public SharedMutexHelper<ContextSharedMutex>
{
private:
    using Base = SharedMutexHelper<ContextSharedMutex, SharedMutex>;
    friend class SharedMutexHelper<ContextSharedMutex, SharedMutex>;

    void lockImpl();

    void lockSharedImpl();
};

class ContextData
{
protected:
    ContextSharedPart * shared;

    ClientInfo client_info;
    ExternalTablesInitializer external_tables_initializer_callback;
    QueryPlanDeserializationCallback query_plan_deserialization_callback;

    InputInitializer input_initializer_callback;
    InputBlocksReader input_blocks_reader;

    std::optional<UUID> user_id;
    std::shared_ptr<std::vector<UUID>> current_roles;
    std::shared_ptr<std::vector<UUID>> external_roles;
    std::shared_ptr<const SettingsConstraintsAndProfileIDs> settings_constraints_and_current_profiles;
    mutable std::shared_ptr<const ContextAccess> access;
    mutable bool need_recalculate_access = true;
    String current_database;
    std::unique_ptr<Settings> settings{};  /// Setting for query execution.

    using ProgressCallback = std::function<void(const Progress & progress)>;
    ProgressCallback progress_callback;  /// Callback for tracking progress of query execution.

    using FileProgressCallback = std::function<void(const FileProgress & progress)>;
    FileProgressCallback file_progress_callback; /// Callback for tracking progress of file loading.

    std::weak_ptr<QueryStatus> process_list_elem;  /// For tracking total resource usage for query.
    bool has_process_list_elem = false;     /// It's impossible to check if weak_ptr was initialized or not
    struct InsertionTableInfo
    {
        StorageID table = StorageID::createEmpty();
        std::optional<Names> column_names;
    };

    InsertionTableInfo insertion_table_info;  /// Saved information about insertion table in query context
    bool is_distributed = false;  /// Whether the current context it used for distributed query

    String default_format;  /// Format, used when server formats data by itself and if query does not have FORMAT specification.
                            /// Thus, used in HTTP interface. If not specified - then some globally default format is used.

    String insert_format; /// Format, used in insert query.

    TemporaryTablesMapping external_tables_mapping;
    Scalars scalars;
    /// Used to store constant values which are different on each instance during distributed plan, such as _shard_num.
    Scalars special_scalars;

    /// Used in s3Cluster table function. With this callback, a worker node could ask an initiator
    /// about next file to read from s3.
    std::optional<ClusterFunctionReadTaskCallback> next_task_callback;
    /// Used in parallel reading from replicas. A replica tells about its intentions to read
    /// some ranges from some part and initiator will tell the replica about whether it is accepted or denied.
    std::optional<MergeTreeReadTaskCallback> merge_tree_read_task_callback;
    std::optional<MergeTreeAllRangesCallback> merge_tree_all_ranges_callback;
    UUID parallel_replicas_group_uuid{UUIDHelpers::Nil};

    BlockMarshallingCallback block_marshalling_callback;

    bool is_under_restore = false;

    /// This parameter can be set by the HTTP client to tune the behavior of output formats for compatibility.
    UInt64 client_protocol_version = 0;

    /// Max block numbers in partitions to read from MergeTree tables.
    PartitionIdToMaxBlockPtr partition_id_to_max_block;

public:
    /// Record entities accessed by current query, and store this information in system.query_log.
    struct QueryAccessInfo
    {
        QueryAccessInfo() = default;

        QueryAccessInfo(const QueryAccessInfo & rhs)
        {
            std::lock_guard<std::mutex> lock(rhs.mutex);
            databases = rhs.databases;
            tables = rhs.tables;
            columns = rhs.columns;
            partitions = rhs.partitions;
            projections = rhs.projections;
            views = rhs.views;
        }

        QueryAccessInfo(QueryAccessInfo && rhs) = delete;

        QueryAccessInfo & operator=(QueryAccessInfo rhs)
        {
            swap(rhs);
            return *this;
        }

        void swap(QueryAccessInfo & rhs) noexcept TSA_NO_THREAD_SAFETY_ANALYSIS
        {
            /// TSA_NO_THREAD_SAFETY_ANALYSIS because it doesn't support scoped_lock
            std::scoped_lock lck{mutex, rhs.mutex};
            std::swap(databases, rhs.databases);
            std::swap(tables, rhs.tables);
            std::swap(columns, rhs.columns);
            std::swap(partitions, rhs.partitions);
            std::swap(projections, rhs.projections);
            std::swap(views, rhs.views);
        }

        /// To prevent a race between copy-constructor and other uses of this structure.
        mutable std::mutex mutex{};
        std::set<std::string> databases TSA_GUARDED_BY(mutex){};
        std::set<std::string> tables TSA_GUARDED_BY(mutex){};
        std::set<std::string> columns TSA_GUARDED_BY(mutex){};
        std::set<std::string> partitions TSA_GUARDED_BY(mutex){};
        std::set<std::string> projections TSA_GUARDED_BY(mutex){};
        std::set<std::string> views TSA_GUARDED_BY(mutex){};
    };
    using QueryAccessInfoPtr = std::shared_ptr<QueryAccessInfo>;

protected:
    /// In some situations, we want to be able to transfer the access info from children back to parents (e.g. definers context).
    /// Therefore, query_access_info must be a pointer.
    QueryAccessInfoPtr query_access_info;

public:
    /// Record names of created objects of factories (for testing, etc)
    struct QueryFactoriesInfo
    {
        QueryFactoriesInfo() = default;

        QueryFactoriesInfo(const QueryFactoriesInfo & rhs)
        {
            std::lock_guard<std::mutex> lock(rhs.mutex);
            aggregate_functions = rhs.aggregate_functions;
            aggregate_function_combinators = rhs.aggregate_function_combinators;
            database_engines = rhs.database_engines;
            data_type_families = rhs.data_type_families;
            dictionaries = rhs.dictionaries;
            formats = rhs.formats;
            functions = rhs.functions;
            storages = rhs.storages;
            table_functions = rhs.table_functions;
            executable_user_defined_functions = rhs.executable_user_defined_functions;
            sql_user_defined_functions = rhs.sql_user_defined_functions;
        }

        QueryFactoriesInfo(QueryFactoriesInfo && rhs) = delete;

        std::unordered_set<std::string> aggregate_functions TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> aggregate_function_combinators TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> database_engines TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> data_type_families TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> dictionaries TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> formats TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> functions TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> storages TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> table_functions TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> executable_user_defined_functions TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> sql_user_defined_functions TSA_GUARDED_BY(mutex);

        mutable std::mutex mutex;
    };

    struct QueryPrivilegesInfo
    {
        QueryPrivilegesInfo() = default;

        QueryPrivilegesInfo(const QueryPrivilegesInfo & rhs)
        {
            std::lock_guard<std::mutex> lock(rhs.mutex);
            used_privileges = rhs.used_privileges;
            missing_privileges = rhs.missing_privileges;
        }

        QueryPrivilegesInfo(QueryPrivilegesInfo && rhs) = delete;

        std::unordered_set<std::string> used_privileges TSA_GUARDED_BY(mutex);
        std::unordered_set<std::string> missing_privileges TSA_GUARDED_BY(mutex);

        mutable std::mutex mutex;
    };

    using QueryPrivilegesInfoPtr = std::shared_ptr<QueryPrivilegesInfo>;

protected:
    /// Needs to be changed while having const context in factories methods
    mutable QueryFactoriesInfo query_factories_info;
    QueryPrivilegesInfoPtr query_privileges_info;
    /// Query metrics for reading data asynchronously with IAsynchronousReader.
    mutable std::shared_ptr<AsyncReadCounters> async_read_counters;

    /// TODO: maybe replace with temporary tables?
    StoragePtr view_source;                 /// Temporary StorageValues used to generate alias columns for materialized views
    Tables table_function_results;          /// Temporary tables obtained by execution of table functions. Keyed by AST tree id.

    ContextWeakMutablePtr query_context;
    ContextWeakMutablePtr session_context;  /// Session context or nullptr. Could be equal to this.
    ContextWeakMutablePtr global_context;   /// Global context. Could be equal to this.

    /// XXX: move this stuff to shared part instead.
    ContextMutablePtr buffer_context;  /// Buffer context. Could be equal to this.

    /// A flag, used to distinguish between user query and internal query to a database engine (MaterializedPostgreSQL).
    bool is_internal_query = false;

    inline static ContextPtr global_context_instance;

    /// Temporary data for query execution accounting.
    TemporaryDataOnDiskScopePtr temp_data_on_disk;

    /// Resource classifier for a query, holds smart pointers required for ResourceLink
    /// NOTE: all resource links became invalid after `classifier` destruction
    mutable ClassifierPtr classifier;

    /// Prepared sets that can be shared between different queries. One use case is when is to share prepared sets between
    /// mutation tasks of one mutation executed against different parts of the same table.
    PreparedSetsCachePtr prepared_sets_cache;

    /// this is a mode of parallel replicas where we set parallel_replicas_count and parallel_replicas_offset
    /// and generate specific filters on the replicas (e.g. when using parallel replicas with sample key)
    /// if we already use a different mode of parallel replicas we want to disable this mode
    bool offset_parallel_replicas_enabled = true;

public:
    /// Some counters for current query execution.
    /// Most of them are workarounds and should be removed in the future.
    struct KitchenSink
    {
        std::atomic<size_t> analyze_counter = 0;

        KitchenSink() = default;

        KitchenSink(const KitchenSink & rhs)
            : analyze_counter(rhs.analyze_counter.load())
        {}

        KitchenSink & operator=(const KitchenSink & rhs)
        {
            if (&rhs == this)
                return *this;
            analyze_counter = rhs.analyze_counter.load();
            return *this;
        }
    };

    KitchenSink kitchen_sink;

    void resetSharedContext();

protected:
    using SampleBlockCache = std::unordered_map<std::string, Block>;
    mutable SampleBlockCache sample_block_cache;
    mutable std::mutex sample_block_cache_mutex;

    using StorageMetadataCache = std::unordered_map<const IStorage *, StorageMetadataPtr>;
    mutable StorageMetadataCache storage_metadata_cache;
    mutable std::mutex storage_metadata_cache_mutex;

    using StorageSnapshotCache = std::unordered_map<const IStorage *, StorageSnapshotPtr>;
    mutable StorageSnapshotCache storage_snapshot_cache;
    mutable std::mutex storage_snapshot_cache_mutex;

    PartUUIDsPtr part_uuids; /// set of parts' uuids, is used for query parts deduplication
    PartUUIDsPtr ignored_part_uuids; /// set of parts' uuids are meant to be excluded from query processing

    NameToNameMap query_parameters;   /// Dictionary with query parameters for prepared statements.
                                                     /// (key=name, value)

    IHostContextPtr host_context;  /// Arbitrary object that may used to attach some host specific information to query context,
                                   /// when using ClickHouse as a library in some project. For example, it may contain host
                                   /// logger, some query identification information, profiling guards, etc. This field is
                                   /// to be customized in HTTP and TCP servers by overloading the customizeContext(DB::ContextPtr)
                                   /// methods.

    ZooKeeperMetadataTransactionPtr metadata_transaction;    /// Distributed DDL context. I'm not sure if it's a suitable place for this,
                                                    /// but it's the easiest way to pass this through the whole stack from executeQuery(...)
                                                    /// to DatabaseOnDisk::commitCreateTable(...) or IStorage::alter(...) without changing
                                                    /// thousands of signatures.
                                                    /// And I hope it will be replaced with more common Transaction sometime.
    std::optional<UUID> parent_table_uuid; /// See comment on setParentTable().
    StopToken ddl_query_cancellation; // See comment on setDDLQueryCancellation().
    Coordination::Requests ddl_additional_checks_on_enqueue; // See comment on setDDLAdditionalChecksOnEnqueue().

    MergeTreeTransactionPtr merge_tree_transaction;     /// Current transaction context. Can be inside session or query context.
                                                        /// It's shared with all children contexts.
    MergeTreeTransactionHolder merge_tree_transaction_holder;   /// It will rollback or commit transaction on Context destruction.

    BackupsInMemoryHolder backups_in_memory; /// Backups stored in memory (see "BACKUP ... TO Memory()" statement)

    /// Use copy constructor or createGlobal() instead
    ContextData();
    ContextData(const ContextData &);

    mutable ThrottlerPtr remote_read_query_throttler;       /// A query-wide throttler for remote IO reads
    mutable ThrottlerPtr remote_write_query_throttler;      /// A query-wide throttler for remote IO writes

    mutable ThrottlerPtr local_read_query_throttler;        /// A query-wide throttler for local IO reads
    mutable ThrottlerPtr local_write_query_throttler;       /// A query-wide throttler for local IO writes

    mutable ThrottlerPtr backups_query_throttler;           /// A query-wide throttler for BACKUPs

    mutable std::mutex mutex_shared_context;    /// mutex to avoid accessing destroyed shared context pointer
                                                /// some Context methods can be called after the shared context is destroyed
                                                /// example, Context::handleCrash() method - called from signal handler
};

/** A set of known objects that can be used in the query.
  * Consists of a shared part (always common to all sessions and queries)
  *  and copied part (which can be its own for each session or query).
  *
  * Everything is encapsulated for all sorts of checks and locks.
  */
class Context: public ContextData, public std::enable_shared_from_this<Context>
{
private:
    /// ContextData mutex
    mutable ContextSharedMutex mutex;

    Context();
    Context(const Context &);

public:
    /// Create initial Context with ContextShared and etc.
    static ContextMutablePtr createGlobal(ContextSharedPart * shared_part);
    static ContextMutablePtr createCopy(const ContextWeakPtr & other);
    static ContextMutablePtr createCopy(const ContextMutablePtr & other);
    static ContextMutablePtr createCopy(const ContextPtr & other);
    static SharedContextHolder createShared();

    ~Context();

    String getPath() const;
    String getFlagsPath() const;
    String getUserFilesPath() const;
    String getDictionariesLibPath() const;
    String getUserScriptsPath() const;
    String getFilesystemCachesPath() const;
    String getFilesystemCacheUser() const;

    // Get the disk used by databases to store metadata files.
    std::shared_ptr<IDisk> getDatabaseDisk() const;

    /// Different kinds of warnings available for use with the `system.warnings` table.
    /// More can be added as necessary. These are used to track if a warning is already
    /// present to be able to add, remove or update warnings from the table
    enum class WarningType
    {
        AVAILABLE_DISK_SPACE_TOO_LOW_FOR_DATA,
        AVAILABLE_DISK_SPACE_TOO_LOW_FOR_LOGS,
        AVAILABLE_MEMORY_TOO_LOW,
        DB_ORDINARY_DEPRECATED,
        DELAY_ACCOUNTING_DISABLED,
        LINUX_FAST_CLOCK_SOURCE_NOT_USED,
        LINUX_MAX_PID_TOO_LOW,
        LINUX_MAX_THREADS_COUNT_TOO_LOW,
        LINUX_MEMORY_OVERCOMMIT_DISABLED,
        LINUX_TRANSPARENT_HUGEPAGES_SET_TO_ALWAYS,
        MAX_ACTIVE_PARTS,
        MAX_ATTACHED_DATABASES,
        MAX_ATTACHED_DICTIONARIES,
        MAX_ATTACHED_TABLES,
        MAX_ATTACHED_VIEWS,
        MAX_NUM_THREADS_LOWER_THAN_LIMIT,
        MAX_PENDING_MUTATIONS_EXCEEDS_LIMIT,
        MAX_PENDING_MUTATIONS_OVER_THRESHOLD,
        MAYBE_BROKEN_TABLES,
        OBSOLETE_MONGO_TABLE_DEFINITION,
        OBSOLETE_SETTINGS,
        PROCESS_USER_MATCHES_DATA_OWNER,
        RABBITMQ_UNSUPPORTED_COLUMNS,
        REPLICATED_DB_WITH_ALL_GROUPS_CLUSTER_PREFIX,
        ROTATIONAL_DISK_WITH_DISABLED_READHEAD,
        SERVER_BUILT_IN_DEBUG_MODE,
        SERVER_BUILT_WITH_COVERAGE,
        SERVER_BUILT_WITH_SANITIZERS,
        SERVER_CPU_OVERLOAD,
        SERVER_LOGGING_LEVEL_TEST,
        SERVER_MEMORY_OVERLOAD,
        SERVER_RUN_UNDER_DEBUGGER,
        SETTING_ZERO_COPY_REPLICATION_ENABLED,
        SKIPPING_CONDITION_QUERY,
        THREAD_FUZZER_IS_ENABLED,
    };

    std::unordered_map<WarningType, PreformattedMessage> getWarnings() const;
    void addOrUpdateWarningMessage(WarningType warning, const PreformattedMessage & message) const;
    void addWarningMessageAboutDatabaseOrdinary(const String & database_name) const;
    void removeWarningMessage(WarningType warning) const;
    void removeAllWarnings() const;

    VolumePtr getGlobalTemporaryVolume() const; /// TODO: remove, use `getTempDataOnDisk`

    TemporaryDataOnDiskScopePtr getTempDataOnDisk() const;
    TemporaryDataOnDiskScopePtr getSharedTempDataOnDisk() const;
    void setTempDataOnDisk(TemporaryDataOnDiskScopePtr temp_data_on_disk_);

    void setFilesystemCachesPath(const String & path);
    void setFilesystemCacheUser(const String & user);

    void setPath(const String & path);
    void setFlagsPath(const String & path);
    void setUserFilesPath(const String & path);
    void setDictionariesLibPath(const String & path);
    void setUserScriptsPath(const String & path);

    void setTemporaryStorageInCache(const String & cache_disk_name, size_t max_size);
    void setTemporaryStoragePolicy(const String & policy_name, size_t max_size);
    void setTemporaryStoragePath(const String & path, size_t max_size);

    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    /// Global application configuration settings.
    void setConfig(const ConfigurationPtr & config);
    const Poco::Util::AbstractConfiguration & getConfigRef() const;

    AccessControl & getAccessControl();
    const AccessControl & getAccessControl() const;

    /// Sets external authenticators config (LDAP, Kerberos).
    void setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config);

    /// Creates GSSAcceptorContext instance based on external authenticator params.
    std::unique_ptr<GSSAcceptorContext> makeGSSAcceptorContext() const;

    /** Take the list of users, quotas and configuration profiles from this config.
      * The list of users is completely replaced.
      * The accumulated quota values are not reset if the quota is not deleted.
      */
    void setUsersConfig(const ConfigurationPtr & config);
    ConfigurationPtr getUsersConfig();

    /// Sets the current user assuming that he/she is already authenticated.
    /// WARNING: This function doesn't check password!
    void setUser(const UUID & user_id_, const std::vector<UUID> & external_roles_ = {});
    UserPtr getUser() const;

    std::optional<UUID> getUserID() const;
    String getUserName() const;

    void setCurrentRoles(const Strings & new_current_roles, bool check_grants = true);
    void setCurrentRoles(const std::vector<UUID> & new_current_roles, bool check_grants = true);
    void setCurrentRoles(const RolesOrUsersSet & new_current_roles, bool check_grants = true);
    void setCurrentRolesDefault();
    std::vector<UUID> getCurrentRoles() const;
    std::vector<UUID> getEnabledRoles() const;
    std::shared_ptr<const EnabledRolesInfo> getRolesInfo() const;

    void setCurrentProfile(const String & profile_name, bool check_constraints = true);
    void setCurrentProfile(const UUID & profile_id, bool check_constraints = true);
    void setCurrentProfiles(const SettingsProfilesInfo & profiles_info, bool check_constraints = true);
    std::vector<UUID> getCurrentProfiles() const;
    std::vector<UUID> getEnabledProfiles() const;

    /// Checks access rights.
    /// Empty database means the current database.
    void checkAccess(const AccessFlags & flags) const;
    void checkAccess(const AccessFlags & flags, std::string_view database) const;
    void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table) const;
    void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id, std::string_view column) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::vector<std::string_view> & columns) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id, const Strings & columns) const;
    void checkAccess(const AccessRightsElement & element) const;
    void checkAccess(const AccessRightsElements & elements) const;

    std::shared_ptr<const ContextAccessWrapper> getAccess() const;

    RowPolicyFilterPtr getRowPolicyFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const;

    std::shared_ptr<const EnabledQuota> getQuota() const;
    std::optional<QuotaUsage> getQuotaUsage() const;

    /// Resource management related
    ResourceManagerPtr getResourceManager() const;
    ClassifierPtr getWorkloadClassifier() const;
    String getMergeWorkload() const;
    void setMergeWorkload(const String & value);
    String getMutationWorkload() const;
    void setMutationWorkload(const String & value);
    bool getThrowOnUnknownWorkload() const;
    void setThrowOnUnknownWorkload(bool value);
    UInt64 getConcurrentThreadsSoftLimitNum() const;
    UInt64 getConcurrentThreadsSoftLimitRatioToCores() const;
    String getConcurrentThreadsScheduler() const;
    std::pair<UInt64, String> setConcurrentThreadsSoftLimit(UInt64 num, UInt64 ratio_to_cores, const String & scheduler);

    /// We have to copy external tables inside executeQuery() to track limits. Therefore, set callback for it. Must set once.
    void setExternalTablesInitializer(ExternalTablesInitializer && initializer);
    /// This method is called in executeQuery() and will call the external tables initializer.
    void initializeExternalTablesIfSet();

    /// This is a callback which returns deserialized QueryPlan if the packet with QueryPlan was received.
    void setQueryPlanDeserializationCallback(QueryPlanDeserializationCallback && callback);
    /// This method is called in executeQuery() and will call the query plan deserialization callback.
    std::shared_ptr<QueryPlanAndSets> getDeserializedQueryPlan();

    /// When input() is present we have to send columns structure to client
    void setInputInitializer(InputInitializer && initializer);
    /// This method is called in StorageInput::read while executing query
    void initializeInput(const StoragePtr & input_storage);

    /// Callback for read data blocks from client one by one for function input()
    void setInputBlocksReaderCallback(InputBlocksReader && reader);
    /// Get callback for reading data for input()
    InputBlocksReader getInputBlocksReaderCallback() const;
    void resetInputCallbacks();

    /// Returns information about the client executing a query.
    const ClientInfo & getClientInfo() const { return client_info; }

    /// Modify stored in the context information about the client executing a query.
    void setClientInfo(const ClientInfo & client_info_);
    void setClientName(const String & client_name);
    void setClientInterface(ClientInfo::Interface interface);
    void setClientVersion(UInt64 client_version_major, UInt64 client_version_minor, UInt64 client_version_patch, unsigned client_tcp_protocol_version);
    void setClientConnectionId(uint32_t connection_id);
    void setScriptQueryAndLineNumber(uint32_t query_number, uint32_t line_number);
    void setHTTPClientInfo(const Poco::Net::HTTPRequest & request);
    void setForwardedFor(const String & forwarded_for);
    void setQueryKind(ClientInfo::QueryKind query_kind);
    void setQueryKindInitial();
    void setQueryKindReplicatedDatabaseInternal();
    void setCurrentUserName(const String & current_user_name);
    void setCurrentAddress(const Poco::Net::SocketAddress & current_address);
    void setInitialUserName(const String & initial_user_name);
    void setInitialAddress(const Poco::Net::SocketAddress & initial_address);
    void setInitialQueryId(const String & initial_query_id);
    void setInitialQueryStartTime(std::chrono::time_point<std::chrono::system_clock> initial_query_start_time);
    void setQuotaClientKey(const String & quota_key);
    void setConnectionClientVersion(UInt64 client_version_major, UInt64 client_version_minor, UInt64 client_version_patch, unsigned client_tcp_protocol_version);
    void increaseDistributedDepth();
    const OpenTelemetry::TracingContext & getClientTraceContext() const { return client_info.client_trace_context; }
    OpenTelemetry::TracingContext & getClientTraceContext() { return client_info.client_trace_context; }

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
    void updateExternalTable(const String & table_name, TemporaryTableHolder && temporary_table);
    void addOrUpdateExternalTable(const String & table_name, TemporaryTableHolder && temporary_table);
    void addExternalTable(const String & table_name, std::shared_ptr<TemporaryTableHolder> temporary_table);
    void updateExternalTable(const String & table_name, std::shared_ptr<TemporaryTableHolder> temporary_table);
    void addOrUpdateExternalTable(const String & table_name, std::shared_ptr<TemporaryTableHolder> temporary_table);
    std::shared_ptr<TemporaryTableHolder> findExternalTable(const String & table_name) const;
    std::shared_ptr<TemporaryTableHolder> removeExternalTable(const String & table_name);

    Scalars getScalars() const;
    Block getScalar(const String & name) const;
    void addScalar(const String & name, const Block & block);
    bool hasScalar(const String & name) const;

    std::optional<Block> tryGetSpecialScalar(const String & name) const;
    void addSpecialScalar(const String & name, const Block & block);

    const QueryAccessInfo & getQueryAccessInfo() const { return *getQueryAccessInfoPtr(); }
    QueryAccessInfoPtr getQueryAccessInfoPtr() const { return query_access_info; }
    void setQueryAccessInfo(QueryAccessInfoPtr other) { query_access_info = other; }

    void addQueryAccessInfo(
        const StorageID & table_id,
        const Names & column_names);

    void addQueryAccessInfo(
        const String & quoted_database_name,
        const String & full_quoted_table_name,
        const Names & column_names);

    void addQueryAccessInfo(const Names & partition_names);
    void addViewAccessInfo(const String & view_name);

    struct QualifiedProjectionName
    {
        StorageID storage_id = StorageID::createEmpty();
        String projection_name;
        explicit operator bool() const { return !projection_name.empty(); }
    };

    void addQueryAccessInfo(const QualifiedProjectionName & qualified_projection_name);

    /// Supported factories for records in query_log
    enum class QueryLogFactories : uint8_t
    {
        AggregateFunction,
        AggregateFunctionCombinator,
        Database,
        DataType,
        Dictionary,
        Format,
        Function,
        Storage,
        TableFunction,
        ExecutableUserDefinedFunction,
        SQLUserDefinedFunction
    };

    QueryFactoriesInfo getQueryFactoriesInfo() const;
    void addQueryFactoriesInfo(QueryLogFactories factory_type, const String & created_object) const;

    const QueryPrivilegesInfo & getQueryPrivilegesInfo() const { return *getQueryPrivilegesInfoPtr(); }
    QueryPrivilegesInfoPtr getQueryPrivilegesInfoPtr() const { return query_privileges_info; }
    void addQueryPrivilegesInfo(const String & privilege, bool granted) const;

    /// For table functions s3/file/url/hdfs/input we can use structure from
    /// insertion table depending on select expression.
    StoragePtr executeTableFunction(const ASTPtr & table_expression, const ASTSelectQuery * select_query_hint = nullptr);
    /// Overload for the new analyzer. Structure inference is performed in QueryAnalysisPass.
    StoragePtr executeTableFunction(const ASTPtr & table_expression, const TableFunctionPtr & table_function_ptr);

    StoragePtr buildParameterizedViewStorage(const String & database_name, const String & table_name, const NameToNameMap & param_values);

    void addViewSource(const StoragePtr & storage);
    StoragePtr getViewSource() const;

    String getCurrentDatabase() const;
    String getCurrentQueryId() const { return client_info.current_query_id; }

    /// Id of initiating query for distributed queries; or current query id if it's not a distributed query.
    String getInitialQueryId() const;

    void setCurrentDatabase(const String & name);
    /// Set current_database for global context. We don't validate that database
    /// exists because it should be set before databases loading.
    void setCurrentDatabaseNameInGlobalContext(const String & name);
    void setCurrentQueryId(const String & query_id);

    /// FIXME: for background operations (like Merge and Mutation) we also use the same Context object and even setup
    /// query_id for it (table_uuid::result_part_name). We can distinguish queries from background operation in some way like
    /// bool is_background = query_id.contains("::"), but it's much worse than just enum check with more clear purpose
    void setBackgroundOperationTypeForContext(ClientInfo::BackgroundOperationType setBackgroundOperationTypeForContextbackground_operation);
    bool isBackgroundOperationContext() const;

    void killCurrentQuery() const;
    bool isCurrentQueryKilled() const;

    bool hasInsertionTable() const { return !insertion_table_info.table.empty(); }
    bool hasInsertionTableColumnNames() const { return insertion_table_info.column_names.has_value(); }
    void setInsertionTable(StorageID db_and_table, std::optional<Names> column_names = std::nullopt) { insertion_table_info = {std::move(db_and_table), std::move(column_names)}; }
    const StorageID & getInsertionTable() const { return insertion_table_info.table; }
    const std::optional<Names> & getInsertionTableColumnNames() const{ return insertion_table_info.column_names; }

    void setDistributed(bool is_distributed_) { is_distributed = is_distributed_; }
    bool isDistributed() const { return is_distributed; }

    bool isUnderRestore() const { return is_under_restore; }
    void setUnderRestore(bool under_restore) { is_under_restore = under_restore; }

    String getDefaultFormat() const;    /// If default_format is not specified, some global default format is returned.
    void setDefaultFormat(const String & name);

    String getInsertFormat() const;
    void setInsertFormat(const String & name);

    MultiVersion<Macros>::Version getMacros() const;
    void setMacros(std::unique_ptr<Macros> && macros);

    bool displaySecretsInShowAndSelect() const;
    Settings getSettingsCopy() const;
    const Settings & getSettingsRef() const { return *settings; }
    void setSettings(const Settings & settings_);

    /// Set settings by name.
    void setSetting(std::string_view name, const String & value);
    void setSetting(std::string_view name, const Field & value);
    void setServerSetting(std::string_view name, const Field & value);
    void applySettingChange(const SettingChange & change);
    void applySettingsChanges(const SettingsChanges & changes);

    /// Checks the constraints.
    void checkSettingsConstraints(const AlterSettingsProfileElements & profile_elements, SettingSource source);
    void checkSettingsConstraints(const SettingChange & change, SettingSource source);
    void checkSettingsConstraints(const SettingsChanges & changes, SettingSource source);
    void checkSettingsConstraints(SettingsChanges & changes, SettingSource source);
    void clampToSettingsConstraints(SettingsChanges & changes, SettingSource source);
    void checkMergeTreeSettingsConstraints(const MergeTreeSettings & merge_tree_settings, const SettingsChanges & changes) const;

    /// Reset settings to default value
    void resetSettingsToDefaultValue(const std::vector<String> & names);

    /// Returns the current constraints (can return null).
    std::shared_ptr<const SettingsConstraintsAndProfileIDs> getSettingsConstraintsAndCurrentProfiles() const;

    AsyncLoader & getAsyncLoader() const;

    const ExternalDictionariesLoader & getExternalDictionariesLoader() const;
    ExternalDictionariesLoader & getExternalDictionariesLoader();
    const EmbeddedDictionaries & getEmbeddedDictionaries() const;
    EmbeddedDictionaries & getEmbeddedDictionaries();
    void tryCreateEmbeddedDictionaries() const;
    void loadOrReloadDictionaries(const Poco::Util::AbstractConfiguration & config);
    void waitForDictionariesLoad() const;

    const ExternalUserDefinedExecutableFunctionsLoader & getExternalUserDefinedExecutableFunctionsLoader() const;
    ExternalUserDefinedExecutableFunctionsLoader & getExternalUserDefinedExecutableFunctionsLoader();
    const IUserDefinedSQLObjectsStorage & getUserDefinedSQLObjectsStorage() const;
    IUserDefinedSQLObjectsStorage & getUserDefinedSQLObjectsStorage();
    void setUserDefinedSQLObjectsStorage(std::unique_ptr<IUserDefinedSQLObjectsStorage> storage);
    void loadOrReloadUserDefinedExecutableFunctions(const Poco::Util::AbstractConfiguration & config);

    IWorkloadEntityStorage & getWorkloadEntityStorage() const;

#if USE_NLP
    SynonymsExtensions & getSynonymsExtensions() const;
    Lemmatizers & getLemmatizers() const;
#endif

    BackupsWorker & getBackupsWorker() const;
    void waitAllBackupsAndRestores() const;
    BackupsInMemoryHolder & getBackupsInMemory();
    const BackupsInMemoryHolder & getBackupsInMemory() const;

    /// I/O formats.
    InputFormatPtr getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size,
                                  const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    OutputFormatPtr getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample, const std::optional<FormatSettings> & format_settings = std::nullopt) const;
    OutputFormatPtr getOutputFormatParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample, const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    InterserverIOHandler & getInterserverIOHandler();
    const InterserverIOHandler & getInterserverIOHandler() const;

    /// How other servers can access this for downloading replicated data.
    void setInterserverIOAddress(const String & host, UInt16 port);
    std::pair<String, UInt16> getInterserverIOAddress() const;

    /// Credentials which server will use to communicate with others
    void updateInterserverCredentials(const Poco::Util::AbstractConfiguration & config);
    InterserverCredentialsPtr getInterserverCredentials() const;

    /// Interserver requests scheme (http or https)
    void setInterserverScheme(const String & scheme);
    String getInterserverScheme() const;

    /// Storage of allowed hosts from config.xml
    void setRemoteHostFilter(const Poco::Util::AbstractConfiguration & config);
    const RemoteHostFilter & getRemoteHostFilter() const;

    /// Storage of forbidden HTTP headers from config.xml
    void setHTTPHeaderFilter(const Poco::Util::AbstractConfiguration & config);
    const HTTPHeaderFilter & getHTTPHeaderFilter() const;

    size_t getMaxTableNumToWarn() const;
    size_t getMaxViewNumToWarn() const;
    size_t getMaxDictionaryNumToWarn() const;
    size_t getMaxDatabaseNumToWarn() const;
    size_t getMaxPartNumToWarn() const;
    size_t getMaxPendingMutationsToWarn() const;
    size_t getMaxPendingMutationsExecutionTimeToWarn() const;

    void setMaxTableNumToWarn(size_t max_table_to_warn);
    void setMaxViewNumToWarn(size_t max_view_to_warn);
    void setMaxDictionaryNumToWarn(size_t max_dictionary_to_warn);
    void setMaxDatabaseNumToWarn(size_t max_database_to_warn);
    void setMaxPartNumToWarn(size_t max_part_to_warn);
    // Based on asynchronous metrics
    void setMaxPendingMutationsToWarn(size_t max_pending_mutations_to_warn);
    void setMaxPendingMutationsExecutionTimeToWarn(size_t max_pending_mutations_execution_time_to_warn);

    double getMinOSCPUWaitTimeRatioToDropConnection() const;
    double getMaxOSCPUWaitTimeRatioToDropConnection() const;
    void setOSCPUOverloadSettings(double min_os_cpu_wait_time_ratio_to_drop_connection, double max_os_cpu_wait_time_ratio_to_drop_connection);

    /// The port that the server listens for executing SQL queries.
    UInt16 getTCPPort() const;

    std::optional<UInt16> getTCPPortSecure() const;

    /// Register server ports during server starting up. No lock is held.
    void registerServerPort(String port_name, UInt16 port);

    UInt16 getServerPort(const String & port_name) const;

    /// For methods below you may need to acquire the context lock by yourself.

    ContextMutablePtr getQueryContext() const;
    bool hasQueryContext() const { return !query_context.expired(); }
    bool isInternalSubquery() const;

    ContextMutablePtr getSessionContext() const;
    bool hasSessionContext() const { return !session_context.expired(); }

    ContextMutablePtr getGlobalContext() const;

    static ContextPtr getGlobalContextInstance() { return global_context_instance; }

    bool hasGlobalContext() const { return !global_context.expired(); }
    bool isGlobalContext() const
    {
        auto ptr = global_context.lock();
        return ptr && ptr.get() == this;
    }

    ContextMutablePtr getBufferContext() const;

    void setQueryContext(ContextMutablePtr context_) { query_context = context_; }
    void setSessionContext(ContextMutablePtr context_) { session_context = context_; }

    void makeQueryContext();
    void makeQueryContextForMerge(const MergeTreeSettings & merge_tree_settings);
    void makeQueryContextForMutate(const MergeTreeSettings & merge_tree_settings);
    void makeSessionContext();
    void makeGlobalContext();

    void setProgressCallback(ProgressCallback callback);
    /// Used in executeQuery() to pass it to the QueryPipeline.
    ProgressCallback getProgressCallback() const;

    void setFileProgressCallback(FileProgressCallback && callback) { file_progress_callback = callback; }
    FileProgressCallback getFileProgressCallback() const { return file_progress_callback; }

    /** Set in executeQuery and InterpreterSelectQuery. Then it is used in QueryPipeline,
      *  to update and monitor information about the total number of resources spent for the query.
      */
    void setProcessListElement(QueryStatusPtr elem);
    /// Can return nullptr if the query was not inserted into the ProcessList.
    QueryStatusPtr getProcessListElement() const;
    QueryStatusPtr getProcessListElementSafe() const;

    /// List all queries.
    ProcessList & getProcessList();
    const ProcessList & getProcessList() const;

    OvercommitTracker * getGlobalOvercommitTracker() const;

    SessionTracker & getSessionTracker();

    MergeList & getMergeList();
    const MergeList & getMergeList() const;

    MovesList & getMovesList();
    const MovesList & getMovesList() const;

    ReplicatedFetchList & getReplicatedFetchList();
    const ReplicatedFetchList & getReplicatedFetchList() const;

    RefreshSet & getRefreshSet();
    const RefreshSet & getRefreshSet() const;

    /// If the current session is expired at the time of the call, synchronously creates and returns a new session with the startNewSession() call.
    /// If no ZooKeeper configured, throws an exception.
    std::shared_ptr<zkutil::ZooKeeper> getZooKeeper() const;
    /// Same as above but return a zookeeper connection from auxiliary_zookeepers configuration entry.
    std::shared_ptr<zkutil::ZooKeeper> getAuxiliaryZooKeeper(const String & name) const;
    /// If name == "default", same as getZooKeeper(), otherwise same as getAuxiliaryZooKeeper().
    std::shared_ptr<zkutil::ZooKeeper> getDefaultOrAuxiliaryZooKeeper(const String & name) const;
    /// return Auxiliary Zookeeper map
    std::map<String, zkutil::ZooKeeperPtr> getAuxiliaryZooKeepers() const;

    /// Try to connect to Keeper using get(Auxiliary)ZooKeeper. Useful for
    /// internal Keeper start (check connection to some other node). Return true
    /// if connected successfully (without exception) or our zookeeper client
    /// connection configured for some other cluster without our node.
    bool tryCheckClientConnectionToMyKeeperCluster() const;

    UInt32 getZooKeeperSessionUptime() const;
    UInt64 getClientProtocolVersion() const;
    void setClientProtocolVersion(UInt64 version);

#if USE_NURAFT
    std::shared_ptr<KeeperDispatcher> getKeeperDispatcher() const;
    std::shared_ptr<KeeperDispatcher> tryGetKeeperDispatcher() const;
#endif
    void initializeKeeperDispatcher(bool start_async) const;
    void shutdownKeeperDispatcher() const;
    void updateKeeperConfiguration(const Poco::Util::AbstractConfiguration & config);

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

    void reloadQueryMaskingRulesIfChanged(const ConfigurationPtr & config) const;

    void setSystemZooKeeperLogAfterInitializationIfNeeded();

    /// --- Caches ------------------------------------------------------------------------------------------

    void setUncompressedCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);
    void updateUncompressedCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<UncompressedCache> getUncompressedCache() const;
    void clearUncompressedCache() const;

    void setPageCache(
        std::chrono::milliseconds history_window, const String & cache_policy, double size_ratio,
        size_t min_size_in_bytes, size_t max_size_in_bytes, double free_memory_ratio,
        size_t num_shards);
    std::shared_ptr<PageCache> getPageCache() const;
    void clearPageCache() const;

    void setMarkCache(const String & cache_policy, size_t max_cache_size_in_bytes, double size_ratio);
    void updateMarkCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<MarkCache> getMarkCache() const;
    void clearMarkCache() const;
    ThreadPool & getLoadMarksThreadpool() const;

    void setPrimaryIndexCache(const String & cache_policy, size_t max_cache_size_in_bytes, double size_ratio);
    void updatePrimaryIndexCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<PrimaryIndexCache> getPrimaryIndexCache() const;
    void clearPrimaryIndexCache() const;

    void setIndexUncompressedCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);
    void updateIndexUncompressedCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<UncompressedCache> getIndexUncompressedCache() const;
    void clearIndexUncompressedCache() const;

    void setIndexMarkCache(const String & cache_policy, size_t max_cache_size_in_bytes, double size_ratio);
    void updateIndexMarkCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<MarkCache> getIndexMarkCache() const;
    void clearIndexMarkCache() const;

    void setVectorSimilarityIndexCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_entries, double size_ratio);
    void updateVectorSimilarityIndexCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<VectorSimilarityIndexCache> getVectorSimilarityIndexCache() const;
    void clearVectorSimilarityIndexCache() const;

    void setMMappedFileCache(size_t max_cache_size_in_num_entries);
    void updateMMappedFileCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<MMappedFileCache> getMMappedFileCache() const;
    void clearMMappedFileCache() const;

    void setQueryResultCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes, size_t max_entry_size_in_rows);
    void updateQueryResultCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<QueryResultCache> getQueryResultCache() const;
    void clearQueryResultCache(const std::optional<String> & tag) const;

#if USE_AVRO
    void setIcebergMetadataFilesCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_entries, double size_ratio);
    void updateIcebergMetadataFilesCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<IcebergMetadataFilesCache> getIcebergMetadataFilesCache() const;
    void clearIcebergMetadataFilesCache() const;
#endif

    void setQueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);
    void updateQueryConditionCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<QueryConditionCache> getQueryConditionCache() const;
    void clearQueryConditionCache() const;

    /** Clear the caches of the uncompressed blocks and marks.
      * This is usually done when renaming tables, changing the type of columns, deleting a table.
      *  - since caches are linked to file names, and become incorrect.
      *  (when deleting a table - it is necessary, since in its place another can appear)
      * const - because the change in the cache is not considered significant.
      */
    void clearCaches() const;

    /// -----------------------------------------------------------------------------------------------------

    void setAsynchronousMetrics(AsynchronousMetrics * asynchronous_metrics_);
    AsynchronousMetrics * getAsynchronousMetrics() const;

    ThreadPool & getPrefetchThreadpool() const;

    /// Note: prefetchThreadpool is different from threadpoolReader
    /// in the way that its tasks are - wait for marks to be loaded
    /// and make a prefetch by putting a read task to threadpoolReader.
    size_t getPrefetchThreadpoolSize() const;

    ThreadPool & getBuildVectorSimilarityIndexThreadPool() const;
    ThreadPool & getIcebergCatalogThreadpool() const;

    /// Settings for MergeTree background tasks stored in config.xml
    BackgroundTaskSchedulingSettings getBackgroundProcessingTaskSchedulingSettings() const;
    BackgroundTaskSchedulingSettings getBackgroundMoveTaskSchedulingSettings() const;

    BackgroundSchedulePool & getBufferFlushSchedulePool() const;
    BackgroundSchedulePool & getSchedulePool() const;
    BackgroundSchedulePool & getMessageBrokerSchedulePool() const;
    BackgroundSchedulePool & getDistributedSchedulePool() const;

    /// Has distributed_ddl configuration or not.
    bool hasDistributedDDL() const;
    void setDDLWorker(std::unique_ptr<DDLWorker> ddl_worker, const LoadTaskPtrs & startup_after);
    DDLWorker & getDDLWorker() const;

    std::map<String, std::shared_ptr<Cluster>> getClusters() const;
    std::shared_ptr<Cluster> getCluster(const std::string & cluster_name) const;
    std::shared_ptr<Cluster> tryGetCluster(const std::string & cluster_name) const;
    void setClustersConfig(const ConfigurationPtr & config, bool enable_discovery = false, const String & config_name = "remote_servers");
    size_t getClustersVersion() const;

    void startClusterDiscovery();

    /// Sets custom cluster, but doesn't update configuration
    void setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster);
    void reloadClusterConfig() const;

    Compiler & getCompiler();

    /// Call after initialization before using system logs. Call for global context.
    void initializeSystemLogs();

    /// Call after initialization before using trace collector.
    void createTraceCollector();

    void initializeTraceCollector();

    /// Call after unexpected crash happen.
    void handleCrash() const;

    bool hasTraceCollector() const;

    /// Nullptr if the query log is not ready for this moment.
    std::shared_ptr<QueryLog> getQueryLog() const;
    std::shared_ptr<QueryThreadLog> getQueryThreadLog() const;
    std::shared_ptr<QueryViewsLog> getQueryViewsLog() const;
    std::shared_ptr<TraceLog> getTraceLog() const;
    std::shared_ptr<TextLog> getTextLog() const;
    std::shared_ptr<MetricLog> getMetricLog() const;
    std::shared_ptr<TransposedMetricLog> getTransposedMetricLog() const;
    std::shared_ptr<LatencyLog> getLatencyLog() const;
    std::shared_ptr<AsynchronousMetricLog> getAsynchronousMetricLog() const;
    std::shared_ptr<OpenTelemetrySpanLog> getOpenTelemetrySpanLog() const;
    std::shared_ptr<ZooKeeperLog> getZooKeeperLog() const;
    std::shared_ptr<SessionLog> getSessionLog() const;
    std::shared_ptr<TransactionsInfoLog> getTransactionsInfoLog() const;
    std::shared_ptr<ProcessorsProfileLog> getProcessorsProfileLog() const;
    std::shared_ptr<FilesystemCacheLog> getFilesystemCacheLog() const;
    std::shared_ptr<ObjectStorageQueueLog> getS3QueueLog() const;
    std::shared_ptr<ObjectStorageQueueLog> getAzureQueueLog() const;
    std::shared_ptr<FilesystemReadPrefetchesLog> getFilesystemReadPrefetchesLog() const;
    std::shared_ptr<AsynchronousInsertLog> getAsynchronousInsertLog() const;
    std::shared_ptr<BackupLog> getBackupLog() const;
    std::shared_ptr<BlobStorageLog> getBlobStorageLog() const;
    std::shared_ptr<QueryMetricLog> getQueryMetricLog() const;

    SystemLogs getSystemLogs() const;

    using Dashboards = std::vector<std::map<String, String>>;
    std::optional<Dashboards> getDashboards() const;
    void setDashboardsConfig(const ConfigurationPtr & config);

    /// Returns an object used to log operations with parts if it possible.
    /// Provide table name to make required checks.
    std::shared_ptr<PartLog> getPartLog(const String & part_database) const;

    const MergeTreeSettings & getMergeTreeSettings() const;
    const MergeTreeSettings & getReplicatedMergeTreeSettings() const;
    const DistributedSettings & getDistributedSettings() const;
    const S3SettingsByEndpoint & getStorageS3Settings() const;
    const AzureSettingsByEndpoint & getStorageAzureSettings() const;

    /// Prevents DROP TABLE if its size is greater than max_size (50GB by default, max_size=0 turn off this check)
    void setMaxTableSizeToDrop(size_t max_size);
    size_t getMaxTableSizeToDrop() const;
    void checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const;
    void checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size, const size_t & max_table_size_to_drop) const;

    /// Prevents DROP PARTITION if its size is greater than max_size (50GB by default, max_size=0 turn off this check)
    void setMaxPartitionSizeToDrop(size_t max_size);
    size_t getMaxPartitionSizeToDrop() const;
    void checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const;
    void checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size, const size_t & max_partition_size_to_drop) const;
    /// Only for system.server_settings, actual value is stored in ConfigReloader
    void setConfigReloaderInterval(size_t value_ms);
    size_t getConfigReloaderInterval() const;

    /// Lets you select the compression codec according to the conditions described in the configuration file.
    std::shared_ptr<ICompressionCodec> chooseCompressionCodec(size_t part_size, double part_size_ratio) const;


    /// Provides storage disks
    DiskPtr getDisk(const String & name) const;
    using DiskCreator = std::function<DiskPtr(const DisksMap & disks_map)>;
    DiskPtr getOrCreateDisk(const String & name, DiskCreator creator) const;

    StoragePoliciesMap getPoliciesMap() const;
    DisksMap getDisksMap() const;
    void updateStorageConfiguration(const Poco::Util::AbstractConfiguration & config);

    /// Provides storage politics schemes
    StoragePolicyPtr getStoragePolicy(const String & name) const;

    StoragePolicyPtr getStoragePolicyFromDisk(const String & disk_name) const;

    using StoragePolicyCreator = std::function<StoragePolicyPtr(const StoragePoliciesMap & storage_policies_map)>;
    StoragePolicyPtr getOrCreateStoragePolicy(const String & name, StoragePolicyCreator creator) const;

    /// Get the server uptime in seconds.
    double getUptimeSeconds() const;

    using ConfigReloadCallback = std::function<void()>;
    void setConfigReloadCallback(ConfigReloadCallback && callback);
    void reloadConfig() const;

    using StartStopServersCallback = std::function<void(const ServerType &)>;
    void setStartServersCallback(StartStopServersCallback && callback);
    void setStopServersCallback(StartStopServersCallback && callback);

    void startServers(const ServerType & server_type) const;
    void stopServers(const ServerType & server_type) const;

    void shutdown();

    bool isInternalQuery() const { return is_internal_query; }
    void setInternalQuery(bool internal) { is_internal_query = internal; }

    ActionLocksManagerPtr getActionLocksManager() const;

    enum class ApplicationType : uint8_t
    {
        SERVER,         /// The program is run as clickhouse-server daemon (default behavior)
        CLIENT,         /// clickhouse-client
        LOCAL,          /// clickhouse-local
        KEEPER,         /// clickhouse-keeper (also daemon)
        DISKS,          /// clickhouse-disks
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

    /// Path to the folder containing the proto files for the well-known Protobuf types
    String getGoogleProtosPath() const;
    void setGoogleProtosPath(const String & path);

    std::pair<Context::SampleBlockCache *, std::unique_lock<std::mutex>> getSampleBlockCache() const;
    std::pair<Context::StorageMetadataCache *, std::unique_lock<std::mutex>> getStorageMetadataCache() const;
    std::pair<Context::StorageSnapshotCache *, std::unique_lock<std::mutex>> getStorageSnapshotCache() const;

    /// Query parameters for prepared statements.
    bool hasQueryParameters() const;
    const NameToNameMap & getQueryParameters() const;

    /// Throws if parameter with the given name already set.
    void setQueryParameter(const String & name, const String & value);
    void setQueryParameters(const NameToNameMap & parameters) { query_parameters = parameters; }

    /// Overrides values of existing parameters.
    void addQueryParameters(const NameToNameMap & parameters);


    IHostContextPtr & getHostContext();
    const IHostContextPtr & getHostContext() const;

    /// Initialize context of distributed DDL query with Replicated database.
    void initZooKeeperMetadataTransaction(ZooKeeperMetadataTransactionPtr txn, bool attach_existing = false);
    /// Returns context of current distributed DDL query or nullptr.
    ZooKeeperMetadataTransactionPtr getZooKeeperMetadataTransaction() const;
    /// Removes context of current distributed DDL.
    void resetZooKeeperMetadataTransaction();

    /// Tells DatabaseReplicated to make this query conditional: it'll only succeed if table with the given UUID exists.
    /// Used by refreshable materialized views to prevent creating inner tables after the MV is dropped.
    /// Doesn't do anything if not in DatabaseReplicated.
    void setParentTable(UUID uuid);
    std::optional<UUID> getParentTable() const;
    /// Allows cancelling DDL query in DatabaseReplicated. Usage:
    ///  1. Call this.
    ///  2. Do a query that goes through DatabaseReplicated's DDL queue (e.g. CREATE TABLE).
    ///  3. The query will wait to complete all previous queries in DDL queue before running this one.
    ///     You can interrupt this wait (and cancel the query from step 2) by cancelling the StopToken.
    ///     (In particular, such cancellation can be done from DDL worker thread itself.
    ///      We do it when dropping refreshable materialized views.)
    ///  4. If the query was interrupted, it'll throw a QUERY_WAS_CANCELLED and will have no effect.
    ///     If the query already started execution, interruption won't happen, and the query will complete normally.
    void setDDLQueryCancellation(StopToken cancel);
    StopToken getDDLQueryCancellation() const;
    /// Allows adding extra zookeeper operations to the transaction that enqueues a DDL query in DatabaseReplicated.
    void setDDLAdditionalChecksOnEnqueue(Coordination::Requests requests);
    Coordination::Requests getDDLAdditionalChecksOnEnqueue() const;

    void checkTransactionsAreAllowed(bool explicit_tcl_query = false) const;
    void initCurrentTransaction(MergeTreeTransactionPtr txn);
    void setCurrentTransaction(MergeTreeTransactionPtr txn);
    MergeTreeTransactionPtr getCurrentTransaction() const;

    bool isServerCompletelyStarted() const;
    void setServerCompletelyStarted();

    PartUUIDsPtr getPartUUIDs() const;
    PartUUIDsPtr getIgnoredPartUUIDs() const;

    AsynchronousInsertQueue * tryGetAsynchronousInsertQueue() const;
    void setAsynchronousInsertQueue(const std::shared_ptr<AsynchronousInsertQueue> & ptr);

    ClusterFunctionReadTaskCallback getClusterFunctionReadTaskCallback() const;
    void setClusterFunctionReadTaskCallback(ClusterFunctionReadTaskCallback && callback);

    MergeTreeReadTaskCallback getMergeTreeReadTaskCallback() const;
    void setMergeTreeReadTaskCallback(MergeTreeReadTaskCallback && callback);

    MergeTreeAllRangesCallback getMergeTreeAllRangesCallback() const;
    void setMergeTreeAllRangesCallback(MergeTreeAllRangesCallback && callback);

    BlockMarshallingCallback getBlockMarshallingCallback() const;
    void setBlockMarshallingCallback(BlockMarshallingCallback && callback);

    UUID getParallelReplicasGroupUUID() const;
    void setParallelReplicasGroupUUID(UUID uuid);

    /// Background executors related methods
    void initializeBackgroundExecutorsIfNeeded();
    bool areBackgroundExecutorsInitialized() const;

    MergeMutateBackgroundExecutorPtr getMergeMutateExecutor() const;
    OrdinaryBackgroundExecutorPtr getMovesExecutor() const;
    OrdinaryBackgroundExecutorPtr getFetchesExecutor() const;
    OrdinaryBackgroundExecutorPtr getCommonExecutor() const;

    IAsynchronousReader & getThreadPoolReader(FilesystemReaderType type) const;
#if USE_LIBURING
    IOUringReader & getIOUringReader() const;
#endif

    std::shared_ptr<AsyncReadCounters> getAsyncReadCounters() const;

    ThreadPool & getThreadPoolWriter() const;

    /** Get settings for reading from filesystem. */
    ReadSettings getReadSettings() const;

    /** Get settings for writing to filesystem. */
    WriteSettings getWriteSettings() const;

    /** There are multiple conditions that have to be met to be able to use parallel replicas */
    bool canUseTaskBasedParallelReplicas() const;
    bool canUseParallelReplicasOnInitiator() const;
    bool canUseParallelReplicasOnFollower() const;
    bool canUseParallelReplicasCustomKey() const;
    bool canUseParallelReplicasCustomKeyForCluster(const Cluster & cluster) const;
    bool canUseOffsetParallelReplicas() const;

    void disableOffsetParallelReplicas();

    ClusterPtr getClusterForParallelReplicas() const;

    void setPreparedSetsCache(const PreparedSetsCachePtr & cache);
    PreparedSetsCachePtr getPreparedSetsCache() const;

    void setPartitionIdToMaxBlock(PartitionIdToMaxBlockPtr partitions);
    PartitionIdToMaxBlockPtr getPartitionIdToMaxBlock() const;

    const ServerSettings & getServerSettings() const;

private:
    std::shared_ptr<const SettingsConstraintsAndProfileIDs> getSettingsConstraintsAndCurrentProfilesWithLock() const;

    void setCurrentProfileWithLock(const String & profile_name, bool check_constraints, const std::lock_guard<ContextSharedMutex> & lock);

    void setCurrentProfileWithLock(const UUID & profile_id, bool check_constraints, const std::lock_guard<ContextSharedMutex> & lock);

    void setCurrentProfilesWithLock(const SettingsProfilesInfo & profiles_info, bool check_constraints, const std::lock_guard<ContextSharedMutex> & lock);

    void setCurrentRolesWithLock(const std::vector<UUID> & new_current_roles, const std::lock_guard<ContextSharedMutex> & lock);

    void setExternalRolesWithLock(const std::vector<UUID> & new_external_roles, const std::lock_guard<ContextSharedMutex> & lock);

    void setSettingWithLock(std::string_view name, const String & value, const std::lock_guard<ContextSharedMutex> & lock);

    void setSettingWithLock(std::string_view name, const Field & value, const std::lock_guard<ContextSharedMutex> & lock);

    void applySettingChangeWithLock(const SettingChange & change, const std::lock_guard<ContextSharedMutex> & lock);

    void applySettingsChangesWithLock(const SettingsChanges & changes, const std::lock_guard<ContextSharedMutex> & lock);

    void setUserIDWithLock(const UUID & user_id_, const std::lock_guard<ContextSharedMutex> & lock);

    void setCurrentDatabaseWithLock(const String & name, const std::lock_guard<ContextSharedMutex> & lock);

    void checkSettingsConstraintsWithLock(const AlterSettingsProfileElements & profile_elements, SettingSource source);

    void checkSettingsConstraintsWithLock(const SettingChange & change, SettingSource source);

    void checkSettingsConstraintsWithLock(const SettingsChanges & changes, SettingSource source);

    void checkSettingsConstraintsWithLock(SettingsChanges & changes, SettingSource source);

    void clampToSettingsConstraintsWithLock(SettingsChanges & changes, SettingSource source);
    void checkSettingsConstraintsWithLock(const AlterSettingsProfileElements & profile_elements, SettingSource source) const;

    void clampToSettingsConstraintsWithLock(SettingsChanges & changes, SettingSource source) const;

    void checkMergeTreeSettingsConstraintsWithLock(const MergeTreeSettings & merge_tree_settings, const SettingsChanges & changes) const;

    ExternalDictionariesLoader & getExternalDictionariesLoaderWithLock(const std::lock_guard<std::mutex> & lock);

    ExternalUserDefinedExecutableFunctionsLoader & getExternalUserDefinedExecutableFunctionsLoaderWithLock(const std::lock_guard<std::mutex> & lock);

    void setUserID(const UUID & user_id_);
    void setCurrentRolesImpl(const std::vector<UUID> & new_current_roles, bool throw_if_not_granted, bool skip_if_not_granted, const std::shared_ptr<const User> & user);

    template <typename... Args>
    void checkAccessImpl(const Args &... args) const;

    EmbeddedDictionaries & getEmbeddedDictionariesImpl(bool throw_on_error) const;

    void checkCanBeDropped(const String & database, const String & table, const size_t & size, const size_t & max_size_to_drop) const;

    StoragePolicySelectorPtr getStoragePolicySelector(std::lock_guard<std::mutex> & lock) const;

    DiskSelectorPtr getDiskSelector(std::lock_guard<std::mutex> & lock) const;

    DisksMap getDisksMap(std::lock_guard<std::mutex> & lock) const;

    /// Expect lock for shared->clusters_mutex
    std::shared_ptr<Clusters> getClustersImpl(std::lock_guard<std::mutex> & lock) const;

    /// Throttling
public:
    ThrottlerPtr getReplicatedFetchesThrottler() const;
    ThrottlerPtr getReplicatedSendsThrottler() const;

    ThrottlerPtr getRemoteReadThrottler() const;
    ThrottlerPtr getRemoteWriteThrottler() const;

    ThrottlerPtr getLocalReadThrottler() const;
    ThrottlerPtr getLocalWriteThrottler() const;

    ThrottlerPtr getBackupsThrottler() const;

    ThrottlerPtr getMutationsThrottler() const;
    ThrottlerPtr getMergesThrottler() const;

    void reloadRemoteThrottlerConfig(size_t read_bandwidth, size_t write_bandwidth) const;
    void reloadLocalThrottlerConfig(size_t read_bandwidth, size_t write_bandwidth) const;

    /// Kitchen sink
    using ContextData::KitchenSink;
    using ContextData::kitchen_sink;
};

struct HTTPContext : public IHTTPContext
{
    explicit HTTPContext(ContextPtr context_)
        : context(Context::createCopy(context_))
    {}

    uint64_t getMaxHstsAge() const override;

    uint64_t getMaxUriSize() const override;

    uint64_t getMaxFields() const override;

    uint64_t getMaxFieldNameSize() const override;

    uint64_t getMaxFieldValueSize() const override;

    Poco::Timespan getReceiveTimeout() const override;

    Poco::Timespan getSendTimeout() const override;

    ContextPtr context;
};

}
