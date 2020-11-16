#pragma once

#include <Interpreters/Context.h>
#include <Client/ConnectionPool.h>
#include <Client/MultiplexedConnections.h>

namespace DB
{

class Throttler;
using ThrottlerPtr = std::shared_ptr<Throttler>;

struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;

struct BlockStreamProfileInfo;
using ProfileInfoCallback = std::function<void(const BlockStreamProfileInfo & info)>;

/// This class allows one to launch queries on remote replicas of one shard and get results
class RemoteQueryExecutor
{
public:
    /// Takes already set connection.
    /// If `settings` is nullptr, settings will be taken from context.
    RemoteQueryExecutor(
        Connection & connection,
        const String & query_, const Block & header_, const Context & context_, const Settings * settings = nullptr,
        ThrottlerPtr throttler_ = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);

    /// Accepts several connections already taken from pool.
    /// If `settings` is nullptr, settings will be taken from context.
    RemoteQueryExecutor(
        std::vector<IConnectionPool::Entry> && connections,
        const String & query_, const Block & header_, const Context & context_, const Settings * settings = nullptr,
        const ThrottlerPtr & throttler = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);

    /// Takes a pool and gets one or several connections from it.
    /// If `settings` is nullptr, settings will be taken from context.
    RemoteQueryExecutor(
        const ConnectionPoolWithFailoverPtr & pool,
        const String & query_, const Block & header_, const Context & context_, const Settings * settings = nullptr,
        const ThrottlerPtr & throttler = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);

    ~RemoteQueryExecutor();

    /// Create connection and send query, external tables and scalars.
    void sendQuery();

    /// Read next block of data. Returns empty block if query is finished.
    Block read();

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

    std::function<std::unique_ptr<MultiplexedConnections>()> create_multiplexed_connections;
    std::unique_ptr<MultiplexedConnections> multiplexed_connections;

    const String query;
    String query_id = "";
    Context context;

    ProgressCallback progress_callback;
    ProfileInfoCallback profile_info_callback;

    /// Scalars needed to be sent to remote servers
    Scalars scalars;
    /// Temporary tables needed to be sent to remote servers
    Tables external_tables;
    QueryProcessingStage::Enum stage;

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

    PoolMode pool_mode = PoolMode::GET_MANY;
    StorageID main_table = StorageID::createEmpty();

    Poco::Logger * log = nullptr;

    /// Send all scalars to remote servers
    void sendScalars();

    /// Send all temporary tables to remote servers
    void sendExternalTables();

    /// If wasn't sent yet, send request to cancel all connections to replicas
    void tryCancel(const char * reason);

    /// Returns true if query was sent
    bool isQueryPending() const;

    /// Returns true if exception was thrown
    bool hasThrownException() const;
};

}
