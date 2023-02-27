#pragma once

#include <variant>

#include <Client/ConnectionPool.h>
#include <Client/IConnections.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Common/TimerDescriptor.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>


namespace DB
{

class Context;

class Throttler;
using ThrottlerPtr = std::shared_ptr<Throttler>;

struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;

struct ProfileInfo;
using ProfileInfoCallback = std::function<void(const ProfileInfo & info)>;

class RemoteQueryExecutorReadContext;

/// This is the same type as StorageS3Source::IteratorWrapper
using TaskIterator = std::function<String()>;

/// This class allows one to launch queries on remote replicas of one shard and get results
class RemoteQueryExecutor
{
public:
    using ReadContext = RemoteQueryExecutorReadContext;

    /// We can provide additional logic for RemoteQueryExecutor
    /// For example for s3Cluster table function we provide an Iterator over tasks to do.
    /// Nodes involved into the query send request for a new task and we answer them using this object.
    /// In case of parallel reading from replicas we provide a Coordinator object
    /// Every replica will tell us about parts and mark ranges it wants to read and coordinator will
    /// decide whether to deny or to accept that request.
    struct Extension
    {
      std::shared_ptr<TaskIterator> task_iterator{nullptr};
      std::shared_ptr<ParallelReplicasReadingCoordinator> parallel_reading_coordinator;
      std::optional<IConnections::ReplicaInfo> replica_info;
    };

    /// Takes already set connection.
    /// We don't own connection, thus we have to drain it synchronously.
    RemoteQueryExecutor(
        Connection & connection,
        const String & query_, const Block & header_, ContextPtr context_,
        ThrottlerPtr throttler_ = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete, std::optional<Extension> extension_ = std::nullopt);

    /// Takes already set connection.
    RemoteQueryExecutor(
        std::shared_ptr<Connection> connection,
        const String & query_, const Block & header_, ContextPtr context_,
        ThrottlerPtr throttler_ = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete, std::optional<Extension> extension_ = std::nullopt);

    /// Accepts several connections already taken from pool.
    RemoteQueryExecutor(
        const ConnectionPoolWithFailoverPtr & pool,
        std::vector<IConnectionPool::Entry> && connections_,
        const String & query_, const Block & header_, ContextPtr context_,
        const ThrottlerPtr & throttler = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete, std::optional<Extension> extension_ = std::nullopt);

    /// Takes a pool and gets one or several connections from it.
    RemoteQueryExecutor(
        const ConnectionPoolWithFailoverPtr & pool,
        const String & query_, const Block & header_, ContextPtr context_,
        const ThrottlerPtr & throttler = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete, std::optional<Extension> extension_ = std::nullopt);

    ~RemoteQueryExecutor();

    /// Create connection and send query, external tables and scalars.
    ///
    /// @param query_kind - kind of query, usually it is SECONDARY_QUERY,
    ///                     since this is the queries between servers
    ///                     (for which this code was written in general).
    ///                     But clickhouse-benchmark uses the same code,
    ///                     and it should pass INITIAL_QUERY.
    void sendQuery(ClientInfo::QueryKind query_kind = ClientInfo::QueryKind::SECONDARY_QUERY);

    /// Query is resent to a replica, the query itself can be modified.
    std::atomic<bool> resent_query { false };

    /// Read next block of data. Returns empty block if query is finished.
    Block read();

    /// Async variant of read. Returns ready block or file descriptor which may be used for polling.
    /// ReadContext is an internal read state. Pass empty ptr first time, reuse created one for every call.
    std::variant<Block, int> read(std::unique_ptr<ReadContext> & read_context);

    /// Receive all remain packets and finish query.
    /// It should be cancelled after read returned empty block.
    void finish(std::unique_ptr<ReadContext> * read_context = nullptr);

    /// Cancel query execution. Sends Cancel packet and ignore others.
    /// This method may be called from separate thread.
    void cancel(std::unique_ptr<ReadContext> * read_context = nullptr);

    /// Get totals and extremes if any.
    Block getTotals() { return std::move(totals); }
    Block getExtremes() { return std::move(extremes); }

    /// Set callback for progress. It will be called on Progress packet.
    void setProgressCallback(ProgressCallback callback) { progress_callback = std::move(callback); }

    /// Set callback for profile info. It will be called on ProfileInfo packet.
    void setProfileInfoCallback(ProfileInfoCallback callback) { profile_info_callback = std::move(callback); }

    /// Set the query_id. For now, used by performance test to later find the query
    /// in the server query_log. Must be called before sending the query to the server.
    void setQueryId(const std::string& query_id_) { assert(!sent_query); query_id = query_id_; }

    /// Specify how we allocate connections on a shard.
    void setPoolMode(PoolMode pool_mode_) { pool_mode = pool_mode_; }

    void setMainTable(StorageID main_table_) { main_table = std::move(main_table_); }

    void setLogger(Poco::Logger * logger) { log = logger; }

    const Block & getHeader() const { return header; }

private:
    RemoteQueryExecutor(
        const String & query_, const Block & header_, ContextPtr context_,
        const Scalars & scalars_, const Tables & external_tables_,
        QueryProcessingStage::Enum stage_, std::optional<Extension> extension_);

    Block header;
    Block totals;
    Block extremes;

    const String query;
    String query_id;
    ContextPtr context;

    ProgressCallback progress_callback;
    ProfileInfoCallback profile_info_callback;

    /// Scalars needed to be sent to remote servers
    Scalars scalars;
    /// Temporary tables needed to be sent to remote servers
    Tables external_tables;
    QueryProcessingStage::Enum stage;
    /// Initiator identifier for distributed task processing
    std::shared_ptr<TaskIterator> task_iterator;

    std::shared_ptr<ParallelReplicasReadingCoordinator> parallel_reading_coordinator;

    /// This is needed only for parallel reading from replicas, because
    /// we create a RemoteQueryExecutor per replica and have to store additional info
    /// about the number of the current replica or the count of replicas at all.
    IConnections::ReplicaInfo replica_info;

    std::function<std::shared_ptr<IConnections>()> create_connections;
    /// Hold a shared reference to the connection pool so that asynchronous connection draining will
    /// work safely. Make sure it's the first member so that we don't destruct it too early.
    const ConnectionPoolWithFailoverPtr pool;
    std::shared_ptr<IConnections> connections;

    /// Streams for reading from temporary tables and following sending of data
    /// to remote servers for GLOBAL-subqueries
    std::vector<ExternalTablesData> external_tables_data;
    std::mutex external_tables_mutex;

    /// Connections to replicas are established, but no queries are sent yet
    std::atomic<bool> established { false };

    /// Query is sent (used before getting first block)
    std::atomic<bool> sent_query { false };

    /** All data from all replicas are received, before EndOfStream packet.
      * To prevent desynchronization, if not all data is read before object
      * destruction, it's required to send cancel query request to replicas and
      * read all packets before EndOfStream
      */
    std::atomic<bool> finished { false };

    /** Cancel query request was sent to all replicas because data is not needed anymore
      * This behaviour may occur when:
      * - data size is already satisfactory (when using LIMIT, for example)
      * - an exception was thrown from client side
      */
    std::atomic<bool> was_cancelled { false };
    std::mutex was_cancelled_mutex;

    /** An exception from replica was received. No need in receiving more packets or
      * requesting to cancel query execution
      */
    std::atomic<bool> got_exception_from_replica { false };

    /** Unknown packet was received from replica. No need in receiving more packets or
      * requesting to cancel query execution
      */
    std::atomic<bool> got_unknown_packet_from_replica { false };

    /** Got duplicated uuids from replica
      */
    std::atomic<bool> got_duplicated_part_uuids{ false };

    /// Parts uuids, collected from remote replicas
    std::mutex duplicated_part_uuids_mutex;
    std::vector<UUID> duplicated_part_uuids;

    PoolMode pool_mode = PoolMode::GET_MANY;
    StorageID main_table = StorageID::createEmpty();

    Poco::Logger * log = nullptr;

    /// Send all scalars to remote servers
    void sendScalars();

    /// Send all temporary tables to remote servers
    void sendExternalTables();

    /// Set part uuids to a query context, collected from remote replicas.
    /// Return true if duplicates found.
    bool setPartUUIDs(const std::vector<UUID> & uuids);

    void processReadTaskRequest();

    void processMergeTreeReadTaskRequest(PartitionReadRequest request);

    /// Cancel query and restart it with info about duplicate UUIDs
    /// only for `allow_experimental_query_deduplication`.
    std::variant<Block, int> restartQueryWithoutDuplicatedUUIDs(std::unique_ptr<ReadContext> * read_context = nullptr);

    /// If wasn't sent yet, send request to cancel all connections to replicas
    void tryCancel(const char * reason, std::unique_ptr<ReadContext> * read_context);

    /// Returns true if query was sent
    bool isQueryPending() const;

    /// Returns true if exception was thrown
    bool hasThrownException() const;

    /// Process packet for read and return data block if possible.
    std::optional<Block> processPacket(Packet packet);

    /// Reads packet by packet
    Block readPackets();

};

}
