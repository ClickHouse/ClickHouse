#pragma once

#include <optional>

#include <common/logger_useful.h>

#include <DataStreams/IBlockInputStream.h>
#include <Common/Throttler.h>
#include <Interpreters/Context.h>
#include <Client/ConnectionPool.h>
#include <Client/MultiplexedConnections.h>
#include <Interpreters/Cluster.h>


namespace DB
{

/** This class allows one to launch queries on remote replicas of one shard and get results
  */
class RemoteBlockInputStream : public IBlockInputStream
{
public:
    /// Takes already set connection.
    /// If `settings` is nullptr, settings will be taken from context.
    RemoteBlockInputStream(
            Connection & connection,
            const String & query_, const Block & header_, const Context & context_, const Settings * settings = nullptr,
            const ThrottlerPtr & throttler = nullptr, const Tables & external_tables_ = Tables(),
            QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);

    /// Accepts several connections already taken from pool.
    /// If `settings` is nullptr, settings will be taken from context.
    RemoteBlockInputStream(
            std::vector<IConnectionPool::Entry> && connections,
            const String & query_, const Block & header_, const Context & context_, const Settings * settings = nullptr,
            const ThrottlerPtr & throttler = nullptr, const Tables & external_tables_ = Tables(),
            QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);

    /// Takes a pool and gets one or several connections from it.
    /// If `settings` is nullptr, settings will be taken from context.
    RemoteBlockInputStream(
            const ConnectionPoolWithFailoverPtr & pool,
            const String & query_, const Block & header_, const Context & context_, const Settings * settings = nullptr,
            const ThrottlerPtr & throttler = nullptr, const Tables & external_tables_ = Tables(),
            QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);

    ~RemoteBlockInputStream() override;

    /// Set the query_id. For now, used by performance test to later find the query
    /// in the server query_log. Must be called before sending the query to the
    /// server.
    ///
    /// FIXME This should have been a parameter of the constructor, but I can't bring
    /// myself to add even more parameters. These constructors actually implement
    /// (in a quite bizarre way) an overloaded function that prepares the multiplexed
    /// connection wrapper. It should have been a plain function that is run by
    /// the caller, but apparently that would have been obscenely straighforward,
    /// too easy to understand and not insane at all, which is a blatant violation
    /// of our coding conventions.
    /// I'm not going to rewrite it now, so that I can get at least something done.
    void setQueryId(std::string _query_id) { assert(!sent_query); query_id = _query_id; }

    /// Specify how we allocate connections on a shard.
    void setPoolMode(PoolMode pool_mode_) { pool_mode = pool_mode_; }

    void setMainTable(QualifiedTableName main_table_) { main_table = std::move(main_table_); }

    /// Sends query (initiates calculation) before read()
    void readPrefix() override;

    /** Prevent default progress notification because progress' callback is
        called by its own
      */
    void progress(const Progress & /*value*/) override {}

    void cancel(bool kill) override;

    String getName() const override { return "Remote"; }

    Block getHeader() const override { return header; }

protected:
    /// Send all temporary tables to remote servers
    void sendExternalTables();

    Block readImpl() override;

    void readSuffixImpl() override;

    /// Returns true if query was sent
    bool isQueryPending() const;

    /// Returns true if exception was thrown
    bool hasThrownException() const;

private:
    void sendQuery();

    Block receiveBlock();

    /// If wasn't sent yet, send request to cancell all connections to replicas
    void tryCancel(const char * reason);

private:
    Block header;

    std::function<std::unique_ptr<MultiplexedConnections>()> create_multiplexed_connections;

    std::unique_ptr<MultiplexedConnections> multiplexed_connections;

    const String query;
    String query_id = "";
    Context context;

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

    /** An exception from replica was received. No need in receiving more packets or
      * requesting to cancel query execution
      */
    std::atomic<bool> got_exception_from_replica { false };

    /** Unkown packet was received from replica. No need in receiving more packets or
      * requesting to cancel query execution
      */
    std::atomic<bool> got_unknown_packet_from_replica { false };

    PoolMode pool_mode = PoolMode::GET_MANY;
    std::optional<QualifiedTableName> main_table;

    Logger * log = &Logger::get("RemoteBlockInputStream");
};

}
