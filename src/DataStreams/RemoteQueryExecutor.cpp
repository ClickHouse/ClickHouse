#include <DataStreams/RemoteQueryExecutor.h>

#include <Columns/ColumnConst.h>
#include <Common/CurrentThread.h>
#include <Common/Throttler.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/ConcatProcessor.h>
#include <Storages/IStorage.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InternalTextLogsQueue.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

RemoteQueryExecutor::RemoteQueryExecutor(
    Connection & connection,
    const String & query_, const Block & header_, const Context & context_, const Settings * settings,
    ThrottlerPtr throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : header(header_), query(query_), context(context_)
    , scalars(scalars_), external_tables(external_tables_), stage(stage_)
{
    if (settings)
        context.setSettings(*settings);

    create_multiplexed_connections = [this, &connection, throttler]()
    {
        return std::make_unique<MultiplexedConnections>(connection, context.getSettingsRef(), throttler);
    };
}

RemoteQueryExecutor::RemoteQueryExecutor(
    std::vector<IConnectionPool::Entry> && connections,
    const String & query_, const Block & header_, const Context & context_, const Settings * settings,
    const ThrottlerPtr & throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : header(header_), query(query_), context(context_)
    , scalars(scalars_), external_tables(external_tables_), stage(stage_)
{
    if (settings)
        context.setSettings(*settings);

    create_multiplexed_connections = [this, connections, throttler]() mutable
    {
        return std::make_unique<MultiplexedConnections>(
                std::move(connections), context.getSettingsRef(), throttler);
    };
}

RemoteQueryExecutor::RemoteQueryExecutor(
    const ConnectionPoolWithFailoverPtr & pool,
    const String & query_, const Block & header_, const Context & context_, const Settings * settings,
    const ThrottlerPtr & throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : header(header_), query(query_), context(context_)
    , scalars(scalars_), external_tables(external_tables_), stage(stage_)
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
            auto try_results = pool->getManyChecked(timeouts, &current_settings, pool_mode, main_table.getQualifiedName());
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

RemoteQueryExecutor::~RemoteQueryExecutor()
{
    /** If interrupted in the middle of the loop of communication with replicas, then interrupt
      * all connections, then read and skip the remaining packets to make sure
      * these connections did not remain hanging in the out-of-sync state.
      */
    if (established || isQueryPending())
        multiplexed_connections->disconnect();
}

/** If we receive a block with slightly different column types, or with excessive columns,
  *  we will adapt it to expected structure.
  */
static Block adaptBlockStructure(const Block & block, const Block & header)
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

                column = castColumn(col, elem.type);

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
            column = castColumn(block.getByName(elem.name), elem.type);

        res.insert({column, elem.type, elem.name});
    }
    return res;
}

void RemoteQueryExecutor::sendQuery()
{
    if (sent_query)
        return;

    multiplexed_connections = create_multiplexed_connections();

    const auto& settings = context.getSettingsRef();
    if (settings.skip_unavailable_shards && 0 == multiplexed_connections->size())
        return;

    established = true;

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    ClientInfo modified_client_info = context.getClientInfo();
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    multiplexed_connections->sendQuery(timeouts, query, query_id, stage, modified_client_info, true);

    established = false;
    sent_query = true;

    if (settings.enable_scalar_subquery_optimization)
        sendScalars();
    sendExternalTables();
}

Block RemoteQueryExecutor::read()
{
    if (!sent_query)
    {
        sendQuery();

        if (context.getSettingsRef().skip_unavailable_shards && (0 == multiplexed_connections->size()))
            return {};
    }

    while (true)
    {
        if (was_cancelled)
            return Block();

        Packet packet = multiplexed_connections->receivePacket();

        switch (packet.type)
        {
            case Protocol::Server::Data:
                /// If the block is not empty and is not a header block
                if (packet.block && (packet.block.rows() > 0))
                    return adaptBlockStructure(packet.block, header);
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
                if (progress_callback)
                    progress_callback(packet.progress);
                break;

            case Protocol::Server::ProfileInfo:
                /// Use own (client-side) info about read bytes, it is more correct info than server-side one.
                if (profile_info_callback)
                    profile_info_callback(packet.profile_info);
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

void RemoteQueryExecutor::finish()
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
    Packet packet = multiplexed_connections->drain();
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

void RemoteQueryExecutor::cancel()
{
    {
        std::lock_guard lock(external_tables_mutex);

        /// Stop sending external data.
        for (auto & vec : external_tables_data)
            for (auto & elem : vec)
                elem->is_cancelled = true;
    }

    if (!isQueryPending() || hasThrownException())
        return;

    tryCancel("Cancelling query");
}

void RemoteQueryExecutor::sendScalars()
{
    multiplexed_connections->sendScalarsData(scalars);
}

void RemoteQueryExecutor::sendExternalTables()
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
                auto metadata_snapshot = cur->getInMemoryMetadataPtr();
                QueryProcessingStage::Enum read_from_table_stage = cur->getQueryProcessingStage(context);

                Pipes pipes;

                pipes = cur->read(
                    metadata_snapshot->getColumns().getNamesOfPhysical(),
                    metadata_snapshot, {}, context,
                    read_from_table_stage, DEFAULT_BLOCK_SIZE, 1);

                auto data = std::make_unique<ExternalTableData>();
                data->table_name = table.first;

                if (pipes.empty())
                    data->pipe = std::make_unique<Pipe>(
                            std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlock(), Chunk()));
                else if (pipes.size() == 1)
                    data->pipe = std::make_unique<Pipe>(std::move(pipes.front()));
                else
                {
                    auto concat = std::make_shared<ConcatProcessor>(pipes.front().getHeader(), pipes.size());
                    data->pipe = std::make_unique<Pipe>(std::move(pipes), std::move(concat));
                }

                res.emplace_back(std::move(data));
            }
            external_tables_data.push_back(std::move(res));
        }
    }

    multiplexed_connections->sendExternalTablesData(external_tables_data);
}

void RemoteQueryExecutor::tryCancel(const char * reason)
{
    {
        /// Flag was_cancelled is atomic because it is checked in read().
        std::lock_guard guard(was_cancelled_mutex);

        if (was_cancelled)
            return;

        was_cancelled = true;
        multiplexed_connections->sendCancel();
    }

    if (log)
        LOG_TRACE(log, "({}) {}", multiplexed_connections->dumpAddresses(), reason);
}

bool RemoteQueryExecutor::isQueryPending() const
{
    return sent_query && !finished;
}

bool RemoteQueryExecutor::hasThrownException() const
{
    return got_exception_from_replica || got_unknown_packet_from_replica;
}

}
