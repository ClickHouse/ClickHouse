#include <DataStreams/RemoteQueryExecutor.h>
#include <DataStreams/RemoteQueryExecutorReadContext.h>

#include <Columns/ColumnConst.h>
#include <Common/CurrentThread.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Common/FiberStack.h>
#include <Client/MultiplexedConnections.h>
#include <Client/HedgedConnections.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int DUPLICATED_PART_UUIDS;
}

RemoteQueryExecutor::RemoteQueryExecutor(
    Connection & connection,
    const String & query_, const Block & header_, const Context & context_,
    ThrottlerPtr throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : header(header_), query(query_), context(context_)
    , scalars(scalars_), external_tables(external_tables_), stage(stage_)
{
    create_connections = [this, &connection, throttler]()
    {
        return std::make_unique<MultiplexedConnections>(connection, context.getSettingsRef(), throttler);
    };
}

RemoteQueryExecutor::RemoteQueryExecutor(
    std::vector<IConnectionPool::Entry> && connections_,
    const String & query_, const Block & header_, const Context & context_,
    const ThrottlerPtr & throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : header(header_), query(query_), context(context_)
    , scalars(scalars_), external_tables(external_tables_), stage(stage_)
{
    create_connections = [this, connections_, throttler]() mutable
    {
        return std::make_unique<MultiplexedConnections>(
                std::move(connections_), context.getSettingsRef(), throttler);
    };
}

RemoteQueryExecutor::RemoteQueryExecutor(
    const ConnectionPoolWithFailoverPtr & pool,
    const String & query_, const Block & header_, const Context & context_,
    const ThrottlerPtr & throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : header(header_), query(query_), context(context_)
    , scalars(scalars_), external_tables(external_tables_), stage(stage_)
{
    create_connections = [this, pool, throttler]()->std::unique_ptr<IConnections>
    {
        const Settings & current_settings = context.getSettingsRef();
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);

#if defined(OS_LINUX)
        if (current_settings.use_hedged_requests)
        {
            std::shared_ptr<QualifiedTableName> table_to_check = nullptr;
            if (main_table)
                table_to_check = std::make_shared<QualifiedTableName>(main_table.getQualifiedName());

            return std::make_unique<HedgedConnections>(pool, current_settings, timeouts, throttler, pool_mode, table_to_check);
        }
#endif

        std::vector<IConnectionPool::Entry> connection_entries;
        if (main_table)
        {
            auto try_results = pool->getManyChecked(timeouts, &current_settings, pool_mode, main_table.getQualifiedName());
            connection_entries.reserve(try_results.size());
            for (auto & try_result : try_results)
                connection_entries.emplace_back(std::move(try_result.entry));
        }
        else
            connection_entries = pool->getMany(timeouts, &current_settings, pool_mode);

        return std::make_unique<MultiplexedConnections>(std::move(connection_entries), current_settings, throttler);
    };
}

RemoteQueryExecutor::~RemoteQueryExecutor()
{
    /** If interrupted in the middle of the loop of communication with replicas, then interrupt
      * all connections, then read and skip the remaining packets to make sure
      * these connections did not remain hanging in the out-of-sync state.
      */
    if (established || isQueryPending())
        connections->disconnect();
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

    connections = create_connections();

    const auto & settings = context.getSettingsRef();
    if (settings.skip_unavailable_shards && 0 == connections->size())
        return;

    /// Query cannot be canceled in the middle of the send query,
    /// since there are multiple packets:
    /// - Query
    /// - Data (multiple times)
    ///
    /// And after the Cancel packet none Data packet can be sent, otherwise the remote side will throw:
    ///
    ///     Unexpected packet Data received from client
    ///
    std::lock_guard guard(was_cancelled_mutex);

    established = true;
    was_cancelled = false;

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    ClientInfo modified_client_info = context.getClientInfo();
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    if (CurrentThread::isInitialized())
    {
        modified_client_info.client_trace_context = CurrentThread::get().thread_trace_context;
    }

    {
        std::lock_guard lock(duplicated_part_uuids_mutex);
        if (!duplicated_part_uuids.empty())
            connections->sendIgnoredPartUUIDs(duplicated_part_uuids);
    }

    connections->sendQuery(timeouts, query, query_id, stage, modified_client_info, true);

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

        if (context.getSettingsRef().skip_unavailable_shards && (0 == connections->size()))
            return {};
    }

    while (true)
    {
        if (was_cancelled)
            return Block();

        Packet packet = connections->receivePacket();

        if (auto block = processPacket(std::move(packet)))
            return *block;
        else if (got_duplicated_part_uuids)
            return std::get<Block>(restartQueryWithoutDuplicatedUUIDs());
    }
}

std::variant<Block, int> RemoteQueryExecutor::read(std::unique_ptr<ReadContext> & read_context [[maybe_unused]])
{

#if defined(OS_LINUX)
    if (!sent_query)
    {
        sendQuery();

        if (context.getSettingsRef().skip_unavailable_shards && (0 == connections->size()))
            return Block();
    }

    if (!read_context || resent_query)
    {
        std::lock_guard lock(was_cancelled_mutex);
        if (was_cancelled)
            return Block();

        read_context = std::make_unique<ReadContext>(*connections);
    }

    do
    {
        if (!read_context->resumeRoutine())
            return Block();

        if (read_context->is_read_in_progress.load(std::memory_order_relaxed))
        {
            read_context->setTimer();
            return read_context->epoll.getFileDescriptor();
        }
        else
        {
            if (auto data = processPacket(std::move(read_context->packet)))
                return std::move(*data);
            else if (got_duplicated_part_uuids)
                return restartQueryWithoutDuplicatedUUIDs(&read_context);
        }
    }
    while (true);
#else
    return read();
#endif
}


std::variant<Block, int> RemoteQueryExecutor::restartQueryWithoutDuplicatedUUIDs(std::unique_ptr<ReadContext> * read_context)
{
    /// Cancel previous query and disconnect before retry.
    cancel(read_context);
    connections->disconnect();

    /// Only resend once, otherwise throw an exception
    if (!resent_query)
    {
        if (log)
            LOG_DEBUG(log, "Found duplicate UUIDs, will retry query without those parts");

        resent_query = true;
        sent_query = false;
        got_duplicated_part_uuids = false;
        /// Consecutive read will implicitly send query first.
        if (!read_context)
            return read();
        else
            return read(*read_context);
    }
    throw Exception("Found duplicate uuids while processing query.", ErrorCodes::DUPLICATED_PART_UUIDS);
}

std::optional<Block> RemoteQueryExecutor::processPacket(Packet packet)
{
    switch (packet.type)
    {
        case Protocol::Server::PartUUIDs:
            if (!setPartUUIDs(packet.part_uuids))
                got_duplicated_part_uuids = true;
            break;
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
            if (!connections->hasActiveConnections())
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
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from one of the following replicas: {}",
                toString(packet.type),
                connections->dumpAddresses());
    }

    return {};
}

bool RemoteQueryExecutor::setPartUUIDs(const std::vector<UUID> & uuids)
{
    Context & query_context = const_cast<Context &>(context).getQueryContext();
    auto duplicates = query_context.getPartUUIDs()->add(uuids);

    if (!duplicates.empty())
    {
        std::lock_guard lock(duplicated_part_uuids_mutex);
        duplicated_part_uuids.insert(duplicated_part_uuids.begin(), duplicates.begin(), duplicates.end());
        return false;
    }
    return true;
}

void RemoteQueryExecutor::finish(std::unique_ptr<ReadContext> * read_context)
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
    tryCancel("Cancelling query because enough data has been read", read_context);

    /// Get the remaining packets so that there is no out of sync in the connections to the replicas.
    Packet packet = connections->drain();
    switch (packet.type)
    {
        case Protocol::Server::EndOfStream:
            finished = true;
            break;

        case Protocol::Server::Log:
            /// Pass logs from remote server to client
            if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                log_queue->pushBlock(std::move(packet.block));
            break;

        case Protocol::Server::Exception:
            got_exception_from_replica = true;
            packet.exception->rethrow();
            break;

        default:
            got_unknown_packet_from_replica = true;
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from one of the following replicas: {}",
                toString(packet.type),
                connections->dumpAddresses());
    }
}

void RemoteQueryExecutor::cancel(std::unique_ptr<ReadContext> * read_context)
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

    tryCancel("Cancelling query", read_context);
}

void RemoteQueryExecutor::sendScalars()
{
    connections->sendScalarsData(scalars);
}

void RemoteQueryExecutor::sendExternalTables()
{
    SelectQueryInfo query_info;

    size_t count = connections->size();

    {
        std::lock_guard lock(external_tables_mutex);

        external_tables_data.clear();
        external_tables_data.reserve(count);

        for (size_t i = 0; i < count; ++i)
        {
            ExternalTablesData res;
            for (const auto & table : external_tables)
            {
                StoragePtr cur = table.second;
                auto metadata_snapshot = cur->getInMemoryMetadataPtr();
                QueryProcessingStage::Enum read_from_table_stage = cur->getQueryProcessingStage(
                    context, QueryProcessingStage::Complete, query_info);

                Pipe pipe = cur->read(
                    metadata_snapshot->getColumns().getNamesOfPhysical(),
                    metadata_snapshot, query_info, context,
                    read_from_table_stage, DEFAULT_BLOCK_SIZE, 1);

                auto data = std::make_unique<ExternalTableData>();
                data->table_name = table.first;

                if (pipe.empty())
                    data->pipe = std::make_unique<Pipe>(
                            std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlock(), Chunk()));
                else
                    data->pipe = std::make_unique<Pipe>(std::move(pipe));

                res.emplace_back(std::move(data));
            }
            external_tables_data.push_back(std::move(res));
        }
    }

    connections->sendExternalTablesData(external_tables_data);
}

void RemoteQueryExecutor::tryCancel(const char * reason, std::unique_ptr<ReadContext> * read_context)
{
    {
        /// Flag was_cancelled is atomic because it is checked in read().
        std::lock_guard guard(was_cancelled_mutex);

        if (was_cancelled)
            return;

        was_cancelled = true;

        if (read_context && *read_context)
            (*read_context)->cancel();

        connections->sendCancel();
    }

    if (log)
        LOG_TRACE(log, "({}) {}", connections->dumpAddresses(), reason);
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
