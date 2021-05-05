#pragma once

#include <Client/ConnectionPool.h>
#include <Client/IConnections.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Common/FiberStack.h>
#include <Common/TimerDescriptor.h>
#include <variant>

namespace DB
{

class Context;

class Throttler;
using ThrottlerPtr = std::shared_ptr<Throttler>;

struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;

struct BlockStreamProfileInfo;
using ProfileInfoCallback = std::function<void(const BlockStreamProfileInfo & info)>;

class RemoteQueryExecutorReadContext;

/// This is the same type as StorageS3Source::IteratorWrapper
using TaskIterator = std::function<String()>;

/// This class allows one to launch queries on remote replicas of one shard and get results
class RemoteQueryExecutor
{
public:
    using ReadContext = RemoteQueryExecutorReadContext;

    /// Takes already set connection.
    RemoteQueryExecutor(
        Connection & connection,
        const String & query_, const Block & header_, ContextPtr context_,
        ThrottlerPtr throttler_ = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete, std::shared_ptr<TaskIterator> task_iterator_ = {});

    /// Accepts several connections already taken from pool.
    RemoteQueryExecutor(
        std::vector<IConnectionPool::Entry> && connections_,
        const String & query_, const Block & header_, ContextPtr context_,
        const ThrottlerPtr & throttler = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete, std::shared_ptr<TaskIterator> task_iterator_ = {});

    /// Takes a pool and gets one or several connections from it.
    RemoteQueryExecutor(
        const ConnectionPoolWithFailoverPtr & pool,
        const String & query_, const Block & header_, ContextPtr context_,
        const ThrottlerPtr & throttler = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete, std::shared_ptr<TaskIterator> task_iterator_ = {});

    ~RemoteQueryExecutor();

    /// Create connection and send query, external tables and scalars.
    void sendQuery();

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
    Block header;
    Block totals;
    Block extremes;

    std::function<std::unique_ptr<IConnections>()> create_connections;
    std::unique_ptr<IConnections> connections;

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

    /// Cancell query and restart it with info about duplicated UUIDs
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
