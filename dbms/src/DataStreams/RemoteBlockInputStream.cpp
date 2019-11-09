#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Common/NetException.h>
#include <Common/CurrentThread.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Storages/IStorage.h>

#include <IO/ConnectionTimeouts.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int LOGICAL_ERROR;
}


RemoteBlockInputStream::RemoteBlockInputStream(
        Connection & connection,
        const String & query_, const Block & header_, const Context & context_, const Settings * settings,
        const ThrottlerPtr & throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : header(header_), query(query_), context(context_), scalars(scalars_), external_tables(external_tables_), stage(stage_)
{
    if (settings)
        context.setSettings(*settings);

    create_multiplexed_connections = [this, &connection, throttler]()
    {
        return std::make_unique<MultiplexedConnections>(connection, context.getSettingsRef(), throttler);
    };
}

RemoteBlockInputStream::RemoteBlockInputStream(
        std::vector<IConnectionPool::Entry> && connections,
        const String & query_, const Block & header_, const Context & context_, const Settings * settings,
        const ThrottlerPtr & throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : header(header_), query(query_), context(context_), scalars(scalars_), external_tables(external_tables_), stage(stage_)
{
    if (settings)
        context.setSettings(*settings);

    create_multiplexed_connections = [this, connections, throttler]() mutable
    {
        return std::make_unique<MultiplexedConnections>(
            std::move(connections), context.getSettingsRef(), throttler);
    };
}

RemoteBlockInputStream::RemoteBlockInputStream(
        const ConnectionPoolWithFailoverPtr & pool,
        const String & query_, const Block & header_, const Context & context_, const Settings * settings,
        const ThrottlerPtr & throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : header(header_), query(query_), context(context_), scalars(scalars_), external_tables(external_tables_), stage(stage_)
{
    if (settings)
        context.setSettings(*settings);

    create_multiplexed_connections = [this, pool, throttler]()
    {
        const Settings & current_settings = context.getSettingsRef();
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);
        std::vector<IConnectionPool::Entry> connections;
        if (main_table)
        {
            auto try_results = pool->getManyChecked(timeouts, &current_settings, pool_mode, *main_table);
            connections.reserve(try_results.size());
            for (auto & try_result : try_results)
                connections.emplace_back(std::move(try_result.entry));
        }
        else
            connections = pool->getMany(timeouts, &current_settings, pool_mode);

        return std::make_unique<MultiplexedConnections>(
            std::move(connections), current_settings, throttler);
    };
}

RemoteBlockInputStream::~RemoteBlockInputStream()
{
    /** If interrupted in the middle of the loop of communication with replicas, then interrupt
      * all connections, then read and skip the remaining packets to make sure
      * these connections did not remain hanging in the out-of-sync state.
      */
    if (established || isQueryPending())
        multiplexed_connections->disconnect();
}

void RemoteBlockInputStream::readPrefix()
{
    if (!sent_query)
        sendQuery();
}

void RemoteBlockInputStream::cancel(bool kill)
{
    if (kill)
        is_killed = true;

    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    {
        std::lock_guard lock(external_tables_mutex);

        /// Stop sending external data.
        for (auto & vec : external_tables_data)
            for (auto & elem : vec)
                elem.first->cancel(kill);
    }

    if (!isQueryPending() || hasThrownException())
        return;

    tryCancel("Cancelling query");
}

void RemoteBlockInputStream::sendScalars()
{
    multiplexed_connections->sendScalarsData(scalars);
}

void RemoteBlockInputStream::sendExternalTables()
{
    size_t count = multiplexed_connections->size();

    {
        std::lock_guard lock(external_tables_mutex);

        external_tables_data.reserve(count);

        for (size_t i = 0; i < count; ++i)
        {
            ExternalTablesData res;
            for (const auto & table : external_tables)
            {
                StoragePtr cur = table.second;
                QueryProcessingStage::Enum read_from_table_stage = cur->getQueryProcessingStage(context);
                BlockInputStreams input = cur->read(cur->getColumns().getNamesOfPhysical(), {}, context,
                    read_from_table_stage, DEFAULT_BLOCK_SIZE, 1);
                if (input.size() == 0)
                    res.push_back(std::make_pair(std::make_shared<OneBlockInputStream>(cur->getSampleBlock()), table.first));
                else
                    res.push_back(std::make_pair(input[0], table.first));
            }
            external_tables_data.push_back(std::move(res));
        }
    }

    multiplexed_connections->sendExternalTablesData(external_tables_data);
}


/** If we receive a block with slightly different column types, or with excessive columns,
  *  we will adapt it to expected structure.
  */
static Block adaptBlockStructure(const Block & block, const Block & header, const Context & context)
{
    /// Special case when reader doesn't care about result structure. Deprecated and used only in Benchmark, PerformanceTest.
    if (!header)
        return block;

    Block res;
    res.info = block.info;

    for (const auto & elem : header)
    {
        ColumnPtr column;

        if (elem.column && isColumnConst(*elem.column))
        {
            /// We expect constant column in block.
            /// If block is not empty, then get value for constant from it,
            /// because it may be different for remote server for functions like version(), uptime(), ...
            if (block.rows() > 0 && block.has(elem.name))
            {
                /// Const column is passed as materialized. Get first value from it.
                ///
                /// TODO: check that column contains the same value.
                /// TODO: serialize const columns.
                auto col = block.getByName(elem.name);
                col.column = block.getByName(elem.name).column->cut(0, 1);

                column = castColumn(col, elem.type, context);

                if (!isColumnConst(*column))
                    column = ColumnConst::create(column, block.rows());
                else
                    /// It is not possible now. Just in case we support const columns serialization.
                    column = column->cloneResized(block.rows());
            }
            else
                column = elem.column->cloneResized(block.rows());
        }
        else
            column = castColumn(block.getByName(elem.name), elem.type, context);

        res.insert({column, elem.type, elem.name});
    }
    return res;
}


Block RemoteBlockInputStream::readImpl()
{
    if (!sent_query)
    {
        sendQuery();

        if (context.getSettingsRef().skip_unavailable_shards && (0 == multiplexed_connections->size()))
            return {};
    }

    while (true)
    {
        if (isCancelledOrThrowIfKilled())
            return Block();

        Connection::Packet packet = multiplexed_connections->receivePacket();

        switch (packet.type)
        {
            case Protocol::Server::Data:
                /// If the block is not empty and is not a header block
                if (packet.block && (packet.block.rows() > 0))
                    return adaptBlockStructure(packet.block, header, context);
                break;  /// If the block is empty - we will receive other packets before EndOfStream.

            case Protocol::Server::Exception:
                got_exception_from_replica = true;
                packet.exception->rethrow();
                break;

            case Protocol::Server::EndOfStream:
                if (!multiplexed_connections->hasActiveConnections())
                {
                    finished = true;
                    return Block();
                }
                break;

            case Protocol::Server::Progress:
                /** We use the progress from a remote server.
                  * We also include in ProcessList,
                  * and we use it to check
                  * constraints (for example, the minimum speed of query execution)
                  * and quotas (for example, the number of lines to read).
                  */
                progressImpl(packet.progress);
                break;

            case Protocol::Server::ProfileInfo:
                /// Use own (client-side) info about read bytes, it is more correct info than server-side one.
                info.setFrom(packet.profile_info, true);
                break;

            case Protocol::Server::Totals:
                totals = packet.block;
                break;

            case Protocol::Server::Extremes:
                extremes = packet.block;
                break;

            case Protocol::Server::Log:
                /// Pass logs from remote server to client
                if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                    log_queue->pushBlock(std::move(packet.block));
                break;

            default:
                got_unknown_packet_from_replica = true;
                throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
        }
    }
}

void RemoteBlockInputStream::readSuffixImpl()
{
    /** If one of:
      * - nothing started to do;
      * - received all packets before EndOfStream;
      * - received exception from one replica;
      * - received an unknown packet from one replica;
      * then you do not need to read anything.
      */
    if (!isQueryPending() || hasThrownException())
        return;

    /** If you have not read all the data yet, but they are no longer needed.
      * This may be due to the fact that the data is sufficient (for example, when using LIMIT).
      */

    /// Send the request to abort the execution of the request, if not already sent.
    tryCancel("Cancelling query because enough data has been read");

    /// Get the remaining packets so that there is no out of sync in the connections to the replicas.
    Connection::Packet packet = multiplexed_connections->drain();
    switch (packet.type)
    {
        case Protocol::Server::EndOfStream:
            finished = true;
            break;

        case Protocol::Server::Exception:
            got_exception_from_replica = true;
            packet.exception->rethrow();
            break;

        default:
            got_unknown_packet_from_replica = true;
            throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    }
}

void RemoteBlockInputStream::sendQuery()
{
    multiplexed_connections = create_multiplexed_connections();

    const auto& settings = context.getSettingsRef();
    if (settings.skip_unavailable_shards && 0 == multiplexed_connections->size())
        return;

    established = true;

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    multiplexed_connections->sendQuery(timeouts, query, query_id, stage, &context.getClientInfo(), true);

    established = false;
    sent_query = true;

    if (settings.enable_scalar_subquery_optimization)
        sendScalars();
    sendExternalTables();
}

void RemoteBlockInputStream::tryCancel(const char * reason)
{
    bool old_val = false;
    if (!was_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    LOG_TRACE(log, "(" << multiplexed_connections->dumpAddresses() << ") " << reason);
    multiplexed_connections->sendCancel();
}

bool RemoteBlockInputStream::isQueryPending() const
{
    return sent_query && !finished;
}

bool RemoteBlockInputStream::hasThrownException() const
{
    return got_exception_from_replica || got_unknown_packet_from_replica;
}

}
