#include <DataStreams/RemoteQueryExecutor.h>

#include <Columns/ColumnConst.h>
#include <Common/CurrentThread.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Common/FiberStack.h>

#include <sys/epoll.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_OPEN_FILE;
    extern const int SOCKET_TIMEOUT;
}

struct RemoteQueryExecutor::ReadContext
{
    bool is_read_in_progress = false;
    Packet packet;

    std::exception_ptr exception;
    FiberStack<> stack;
    boost::context::fiber fiber;

    Poco::Timespan receive_timeout;
    MultiplexedConnections & connections;

    TimerDescriptor timer{CLOCK_MONOTONIC, 0};
    int socket_fd;
    int epoll_df;

    explicit ReadContext(MultiplexedConnections & connections_) : connections(connections_)
    {
        auto & socket = connections.getSocket();
        socket_fd = socket.impl()->sockfd();
        receive_timeout = socket.impl()->getReceiveTimeout();

        if (-1 == epoll_create(2))
            throwFromErrno("Cannot create epoll descriptor", ErrorCodes::CANNOT_OPEN_FILE);

        {
            epoll_event socket_event;
            socket_event.events = EPOLLIN | EPOLLPRI;
            socket_event.data.fd = socket_fd;

            if (-1 == epoll_ctl(epoll_df, EPOLL_CTL_ADD, socket_event.data.fd, &socket_event))
                throwFromErrno("Cannot add socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
        }

        {
            epoll_event timer_event;
            timer_event.events = EPOLLIN | EPOLLPRI;
            timer_event.data.fd = timer.getDescriptor();

            if (-1 == epoll_ctl(epoll_df, EPOLL_CTL_ADD, timer_event.data.fd, &timer_event))
                throwFromErrno("Cannot add timer descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
        }

        fiber = boost::context::fiber(std::allocator_arg_t(), stack, Routine{connections, *this});
    }

    static void initialize(std::unique_ptr<ReadContext> & read_context, MultiplexedConnections & connections)
    {
        try
        {
            read_context = std::make_unique<ReadContext>(connections);
        }
        catch (DB::Exception & e)
        {
            e.addMessage(" while reading from socket ({})", connections.getSocket().peerAddress().toString());
        }
    }

    void checkTimeout() const
    {
        try
        {
            checkTimeoutImpl();
        }
        catch (DB::Exception & e)
        {
            e.addMessage(" while reading from socket ({})", connections.getSocket().peerAddress().toString());
        }
    }

    void checkTimeoutImpl() const
    {
        epoll_event events[2];

        /// Wait for epoll_fd will not block if it was polled externally.
        int num_events = epoll_wait(epoll_df, events, 2, 0);
        if (num_events == -1)
            throwFromErrno("Failed to epoll_wait", ErrorCodes::CANNOT_READ_FROM_SOCKET);

        bool is_socket_ready = false;
        bool has_timer_alarm = false;

        for (int i = 0; i < num_events; ++i)
        {
            if (events[i].data.fd == socket_fd)
                is_socket_ready = true;
            if (events[i].data.fd == timer.getDescriptor())
                has_timer_alarm = true;
        }

        if (has_timer_alarm && !is_socket_ready)
        {
            /// Socket receive timeout. Drain it in case or error, or it may be hide by timeout exception.
            timer.drain();
            throw NetException("Timeout exceeded", ErrorCodes::SOCKET_TIMEOUT);
        }
    }

    void setTimer() const
    {
        /// Did not get packet yet. Init timeout for the next async reading.
        timer.reset();

        if (receive_timeout.totalMicroseconds())
            timer.setRelative(receive_timeout);
    }

    void resumeRoutine()
    {
        fiber = std::move(fiber).resume();

        if (exception)
            std::rethrow_exception(std::move(exception));
    }

    ~ReadContext()
    {
        /// socket_fd is closed by Poco::Net::Socket
        /// timer_fd is closed by TimerDescriptor
        close(epoll_df);
    }

    struct Routine
    {
        MultiplexedConnections & connections;
        ReadContext & read_context;

        boost::context::fiber operator()(boost::context::fiber && sink) const
        {
            try
            {
                while (true)
                {
                    read_context.is_read_in_progress = true;
                    connections.setFiber(&sink);

                    read_context.packet = connections.receivePacket();

                    read_context.is_read_in_progress = false;
                    connections.setFiber(nullptr);

                    sink = std::move(sink).resume();
                }
            }
            catch (...)
            {
                read_context.exception = std::current_exception();
            }

            return std::move(sink);
        };
    };
};

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

    /// Query cannot be canceled in the middle of the send query,
    /// since there are multiple packages:
    /// - Query
    /// - Data (multiple times)
    ///
    /// And after the Cancel packet none Data packet can be sent, otherwise the remote side will throw:
    ///
    ///     Unexpected packet Data received from client
    ///
    std::lock_guard guard(was_cancelled_mutex);

    established = true;

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    ClientInfo modified_client_info = context.getClientInfo();
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    if (CurrentThread::isInitialized())
    {
        modified_client_info.client_trace_context = CurrentThread::get().thread_trace_context;
    }

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

        if (auto block = processPacket(std::move(packet)))
            return *block;
    }
}

std::variant<Block, int> RemoteQueryExecutor::read(std::unique_ptr<ReadContext> & read_context)
{
    if (!sent_query)
    {
        sendQuery();

        if (context.getSettingsRef().skip_unavailable_shards && (0 == multiplexed_connections->size()))
            return Block();
    }

    if (!read_context)
        ReadContext::initialize(read_context, *multiplexed_connections);

    if (read_context->is_read_in_progress)
        read_context->checkTimeout();

    do
    {
        read_context->resumeRoutine();

        if (read_context->is_read_in_progress)
        {
            read_context->setTimer();
            return read_context->epoll_df;
        }
        else
        {
            if (auto data = processPacket(std::move(read_context->packet)))
                return std::move(*data);
        }
    }
    while (true);
}

std::optional<Block> RemoteQueryExecutor::processPacket(Packet packet)
{
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
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from one of the following replicas: {}",
                toString(packet.type),
                multiplexed_connections->dumpAddresses());
    }

    return {};
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
                multiplexed_connections->dumpAddresses());
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
    SelectQueryInfo query_info;

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
