#pragma once

#include <optional>

#include <common/logger_useful.h>

#include <DataStreams/IBlockInputStream.h>
#include <Common/Throttler.h>
#include <Interpreters/Context.h>
#include <Client/ConnectionPool.h>
#include <Client/MultiplexedConnections.h>
#include <Interpreters/Cluster.h>

#include <DataStreams/RemoteQueryExecutor.h>

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
            const ThrottlerPtr & throttler = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
            QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);

    /// Accepts several connections already taken from pool.
    /// If `settings` is nullptr, settings will be taken from context.
    RemoteBlockInputStream(
            std::vector<IConnectionPool::Entry> && connections,
            const String & query_, const Block & header_, const Context & context_, const Settings * settings = nullptr,
            const ThrottlerPtr & throttler = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
            QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);

    /// Takes a pool and gets one or several connections from it.
    /// If `settings` is nullptr, settings will be taken from context.
    RemoteBlockInputStream(
            const ConnectionPoolWithFailoverPtr & pool,
            const String & query_, const Block & header_, const Context & context_, const Settings * settings = nullptr,
            const ThrottlerPtr & throttler = nullptr, const Scalars & scalars_ = Scalars(), const Tables & external_tables_ = Tables(),
            QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);

    /// Set the query_id. For now, used by performance test to later find the query
    /// in the server query_log. Must be called before sending the query to the server.
    void setQueryId(const std::string & query_id) { query_executor.setQueryId(query_id); }

    /// Specify how we allocate connections on a shard.
    void setPoolMode(PoolMode pool_mode) { query_executor.setPoolMode(pool_mode); }

    void setMainTable(StorageID main_table_) { query_executor.setMainTable(std::move(main_table_)); }

    /// Sends query (initiates calculation) before read()
    void readPrefix() override;

    /// Prevent default progress notification because progress' callback is called by its own.
    void progress(const Progress & /*value*/) override {}

    void cancel(bool kill) override;

    String getName() const override { return "Remote"; }

    Block getHeader() const override { return query_executor.getHeader(); }
    Block getTotals() override { return query_executor.getTotals(); }
    Block getExtremes() override { return query_executor.getExtremes(); }

protected:
    Block readImpl() override;
    void readSuffixImpl() override;

private:
    RemoteQueryExecutor query_executor;
    Poco::Logger * log = &Poco::Logger::get("RemoteBlockInputStream");

    void init();
};

}
