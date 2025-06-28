#pragma once

#include <Client/ConnectionPool.h>
#include <Client/IConnections.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Common/UniqueLock.h>
#include <Interpreters/ClientInfo.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <sys/types.h>


namespace DB
{

class Context;

class IThrottler;
using ThrottlerPtr = std::shared_ptr<IThrottler>;

struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;

struct ProfileInfo;
using ProfileInfoCallback = std::function<void(const ProfileInfo & info)>;

struct ClusterFunctionReadTaskResponse;
using ClusterFunctionReadTaskResponsePtr = std::shared_ptr<ClusterFunctionReadTaskResponse>;

class RemoteQueryExecutorReadContext;

class ParallelReplicasReadingCoordinator;

/// This is the same type as StorageS3Source::IteratorWrapper
using TaskIterator = std::function<ClusterFunctionReadTaskResponsePtr(size_t)>;

/// This class allows one to launch queries on remote replicas of one shard and get results
class RemoteQueryExecutor
{
public:
    using ReadContext = RemoteQueryExecutorReadContext;

    /// To avoid deadlock in case of OOM and timeout in CancellationChecker
    using LockAndBlocker = LockAndOverCommitTrackerBlocker<std::lock_guard, std::mutex>;

    /// We can provide additional logic for RemoteQueryExecutor
    /// For example for s3Cluster table function we provide an Iterator over tasks to do.
    /// Nodes involved into the query send request for a new task and we answer them using this object.
    /// In case of parallel reading from replicas we provide a Coordinator object
    /// Every replica will tell us about parts and mark ranges it wants to read and coordinator will
    /// decide whether to deny or to accept that request.
    struct Extension
    {
        std::shared_ptr<TaskIterator> task_iterator = nullptr;
        std::shared_ptr<ParallelReplicasReadingCoordinator> parallel_reading_coordinator = nullptr;
        std::optional<IConnections::ReplicaInfo> replica_info = {};
    };

    /// Takes a connection pool for a node (not cluster)
    RemoteQueryExecutor(
        ConnectionPoolPtr pool,
        const String & query_,
        const Block & header_,
        ContextPtr context_,
        ThrottlerPtr throttler = nullptr,
        const Scalars & scalars_ = Scalars(),
        const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
        std::optional<Extension> extension_ = std::nullopt,
        ConnectionPoolWithFailoverPtr connection_pool_with_failover_ = nullptr);

    /// Takes already set connection.
    RemoteQueryExecutor(
        Connection & connection,
        const String & query_,
        const Block & header_,
        ContextPtr context_,
        ThrottlerPtr throttler_ = nullptr,
        const Scalars & scalars_ = Scalars(),
        const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,

        std::optional<Extension> extension_ = std::nullopt);

    /// Takes already set connection.
    RemoteQueryExecutor(
        std::shared_ptr<Connection> connection,
        const String & query_,
        const Block & header_,
        ContextPtr context_,
        ThrottlerPtr throttler_ = nullptr,
        const Scalars & scalars_ = Scalars(),
        const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
        std::optional<Extension> extension_ = std::nullopt);

    /// Accepts several connections already taken from pool.
    RemoteQueryExecutor(
        std::vector<IConnectionPool::Entry> && connections_,
        const String & query_,
        const Block & header_,
        ContextPtr context_,
        const ThrottlerPtr & throttler = nullptr,
        const Scalars & scalars_ = Scalars(),
        const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
        std::shared_ptr<const QueryPlan> query_plan_ = nullptr,
        std::optional<Extension> extension_ = std::nullopt);

    /// Takes a pool and gets one or several connections from it.
    RemoteQueryExecutor(
        const ConnectionPoolWithFailoverPtr & pool,
        const String & query_,
        const Block & header_,
        ContextPtr context_,
        const ThrottlerPtr & throttler = nullptr,
        const Scalars & scalars_ = Scalars(),
        const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
        std::shared_ptr<const QueryPlan> query_plan_ = nullptr,
        std::optional<Extension> extension_ = std::nullopt,
        GetPriorityForLoadBalancing::Func priority_func = {});

    ~RemoteQueryExecutor();

    /// Create connection and send query, external tables and scalars.
    ///
    /// @param query_kind - kind of query, usually it is SECONDARY_QUERY,
    ///                     since this is the queries between servers
    ///                     (for which this code was written in general).
    ///                     But clickhouse-benchmark uses the same code,
    ///                     and it should pass INITIAL_QUERY.
    void sendQuery(ClientInfo::QueryKind query_kind = ClientInfo::QueryKind::SECONDARY_QUERY, AsyncCallback async_callback = {});
    void sendQueryUnlocked(ClientInfo::QueryKind query_kind = ClientInfo::QueryKind::SECONDARY_QUERY, AsyncCallback async_callback = {});

    int sendQueryAsync();

    /// Query is resent to a replica, the query itself can be modified.
    bool resent_query { false };
    bool recreate_read_context { false };

    struct ReadResult
    {
        enum class Type : uint8_t
        {
            Data,
            ParallelReplicasToken,
            FileDescriptor,
            Finished,
            Nothing
        };

        explicit ReadResult(Block block_)
            : type(Type::Data)
            , block(std::move(block_))
        {}

        explicit ReadResult(int fd_)
            : type(Type::FileDescriptor)
            , fd(fd_)
        {}

        explicit ReadResult(Type type_)
            : type(type_)
        {
            assert(type != Type::Data && type != Type::FileDescriptor);
        }

        Type getType() const { return type; }

        Block getBlock()
        {
            chassert(type == Type::Data);
            return std::move(block);
        }

        int getFileDescriptor() const
        {
            chassert(type == Type::FileDescriptor);
            return fd;
        }

        const Type type;
        Block block;
        const int fd{-1};
    };

    /// Read next block of data. Returns empty block if query is finished.
    Block readBlock();

    ReadResult read();

    /// Async variant of read. Returns ready block or file descriptor which may be used for polling.
    ReadResult readAsync();

    /// Receive all remain packets and finish query.
    /// It should be cancelled after read returned empty block.
    void finish();

    /// Cancel query execution. Sends Cancel packet and ignore others.
    /// This method may be called from separate thread.
    void cancel();

    /// Get totals and extremes if any.
    Block getTotals() { return std::move(totals); }
    Block getExtremes() { return std::move(extremes); }

    /// Set callback for progress. It will be called on Progress packet.
    void setProgressCallback(ProgressCallback callback);

    /// Set callback for profile info. It will be called on ProfileInfo packet.
    void setProfileInfoCallback(ProfileInfoCallback callback);

    /// Set the query_id. For now, used by performance test to later find the query
    /// in the server query_log. Must be called before sending the query to the server.
    void setQueryId(const std::string& query_id_) { assert(!sent_query); query_id = query_id_; }

    /// Specify how we allocate connections on a shard.
    void setPoolMode(PoolMode pool_mode_) { pool_mode = pool_mode_; }

    void setMainTable(StorageID main_table_) { main_table = std::move(main_table_); }

    void setLogger(LoggerPtr logger) { log = logger; }

    const Block & getHeader() const { return header; }

    IConnections & getConnections() { return *connections; }

    bool needToSkipUnavailableShard() const;

    bool isReplicaUnavailable() const { return extension && extension->parallel_reading_coordinator && connections->size() == 0; }

    /// return true if parallel replica packet was processed
    bool processParallelReplicaPacketIfAny();

private:
    RemoteQueryExecutor(
        const String & query_,
        const Block & header_,
        ContextPtr context_,
        const Scalars & scalars_,
        const Tables & external_tables_,
        QueryProcessingStage::Enum stage_,
        std::shared_ptr<const QueryPlan> query_plan_,
        std::optional<Extension> extension_,
        GetPriorityForLoadBalancing::Func priority_func = {});

    Block header;
    Block totals;
    Block extremes;

    std::function<std::unique_ptr<IConnections>(AsyncCallback)> create_connections;
    std::unique_ptr<IConnections> connections;
    std::unique_ptr<ReadContext> read_context;

    const String query;
    std::shared_ptr<const QueryPlan> query_plan;
    String query_id;
    ContextPtr context;

    ProgressCallback progress_callback;
    ProfileInfoCallback profile_info_callback;

    /// Scalars needed to be sent to remote servers
    Scalars scalars;
    /// Temporary tables needed to be sent to remote servers
    Tables external_tables;
    QueryProcessingStage::Enum stage;

    std::optional<Extension> extension;
    /// Initiator identifier for distributed task processing
    std::shared_ptr<TaskIterator> task_iterator;

    /// Streams for reading from temporary tables and following sending of data
    /// to remote servers for GLOBAL-subqueries
    std::vector<ExternalTablesData> external_tables_data;
    std::mutex external_tables_mutex;

    /// Connections to replicas are established, but no queries are sent yet
    bool established = false;

    /// Query is sent (used before getting first block)
    bool sent_query { false };

    /** All data from all replicas are received, before EndOfStream packet.
      * To prevent desynchronization, if not all data is read before object
      * destruction, it's required to send cancel query request to replicas and
      * read all packets before EndOfStream
      */
    bool finished = false;

    /** Cancel query request was sent to all replicas because data is not needed anymore
      * This behaviour may occur when:
      * - data size is already satisfactory (when using LIMIT, for example)
      * - an exception was thrown from client side
      */
    bool was_cancelled = false;
    std::mutex was_cancelled_mutex;

    /** An exception from replica was received. No need in receiving more packets or
      * requesting to cancel query execution
      */
    bool got_exception_from_replica = false;

    /** Unknown packet was received from replica. No need in receiving more packets or
      * requesting to cancel query execution
      */
    bool got_unknown_packet_from_replica = false;

    /** Got duplicated uuids from replica
      */
    bool got_duplicated_part_uuids = false;

#if defined(OS_LINUX)
    bool packet_in_progress = false;
#endif

    /// Parts uuids, collected from remote replicas
    std::vector<UUID> duplicated_part_uuids;

    PoolMode pool_mode = PoolMode::GET_MANY;
    StorageID main_table = StorageID::createEmpty();

    LoggerPtr log = nullptr;

    GetPriorityForLoadBalancing::Func priority_func;

    const bool read_packet_type_separately = false;

    /// Send all scalars to remote servers
    void sendScalars();

    /// Send all temporary tables to remote servers
    void sendExternalTables();

    /// Set part uuids to a query context, collected from remote replicas.
    /// Return true if duplicates found.
    bool setPartUUIDs(const std::vector<UUID> & uuids);

    void processReadTaskRequest();

    void processMergeTreeReadTaskRequest(ParallelReadRequest request);
    void processMergeTreeInitialReadAnnouncement(InitialAllRangesAnnouncement announcement);

    /// Cancel query and restart it with info about duplicate UUIDs
    /// only for `allow_experimental_query_deduplication`.
    ReadResult restartQueryWithoutDuplicatedUUIDs();

    /// If wasn't sent yet, send request to cancel all connections to replicas
    void cancelUnlocked();
    void tryCancel(const char * reason);

    /// Returns true if query was sent
    bool isQueryPending() const;

    /// Returns true if exception was thrown
    bool hasThrownException() const;

    /// Process packet for read and return data block if possible.
    ReadResult processPacket(Packet packet);
};

}
