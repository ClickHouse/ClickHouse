#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Common/VirtualColumnUtils.h>
#include <Common/NetException.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int LOGICAL_ERROR;
}


RemoteBlockInputStream::RemoteBlockInputStream(Connection & connection_, const String & query_,
    const Settings * settings_, ThrottlerPtr throttler_, const Tables & external_tables_,
    QueryProcessingStage::Enum stage_, const Context & context_)
    : connection(&connection_), query(query_), throttler(throttler_), external_tables(external_tables_),
    stage(stage_), context(context_)
{
    init(settings_);
}

RemoteBlockInputStream::RemoteBlockInputStream(const ConnectionPoolWithFailoverPtr & pool_, const String & query_,
    const Settings * settings_, ThrottlerPtr throttler_, const Tables & external_tables_,
    QueryProcessingStage::Enum stage_, const Context & context_)
    : pool(pool_), query(query_), throttler(throttler_), external_tables(external_tables_),
    stage(stage_), context(context_)
{
    init(settings_);
}

RemoteBlockInputStream::RemoteBlockInputStream(ConnectionPoolWithFailoverPtrs && pools_, const String & query_,
    const Settings * settings_, ThrottlerPtr throttler_, const Tables & external_tables_,
    QueryProcessingStage::Enum stage_, const Context & context_)
    : pools(std::move(pools_)), query(query_), throttler(throttler_), external_tables(external_tables_),
    stage(stage_), context(context_)
{
    init(settings_);
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

void RemoteBlockInputStream::appendExtraInfo()
{
    append_extra_info = true;
}

void RemoteBlockInputStream::readPrefix()
{
    if (!sent_query)
        sendQuery();
}

void RemoteBlockInputStream::cancel()
{
    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    {
        std::lock_guard<std::mutex> lock(external_tables_mutex);

        /// Stop sending external data.
        for (auto & vec : external_tables_data)
            for (auto & elem : vec)
                if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(elem.first.get()))
                    stream->cancel();
    }

    if (!isQueryPending() || hasThrownException())
        return;

    tryCancel("Cancelling query");
}

void RemoteBlockInputStream::sendExternalTables()
{
    size_t count = multiplexed_connections->size();

    {
        std::lock_guard<std::mutex> lock(external_tables_mutex);

        external_tables_data.reserve(count);

        for (size_t i = 0; i < count; ++i)
        {
            ExternalTablesData res;
            for (const auto & table : external_tables)
            {
                StoragePtr cur = table.second;
                QueryProcessingStage::Enum stage = QueryProcessingStage::Complete;
                DB::BlockInputStreams input = cur->read(cur->getColumnNamesList(), ASTPtr(), context, settings,
                    stage, DEFAULT_BLOCK_SIZE, 1);
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

Block RemoteBlockInputStream::readImpl()
{
    if (!sent_query)
    {
        sendQuery();

        if (settings.skip_unavailable_shards && (0 == multiplexed_connections->size()))
            return {};
    }

    while (true)
    {
        if (isCancelled())
            return Block();

        Connection::Packet packet = multiplexed_connections->receivePacket();

        switch (packet.type)
        {
            case Protocol::Server::Data:
                /// If the block is not empty and is not a header block
                if (packet.block && (packet.block.rows() > 0))
                    return packet.block;
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

    /// Get the remaining packages so that there is no out of sync in the connections to the replicas.
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

void RemoteBlockInputStream::createMultiplexedConnections()
{
    Settings * multiplexed_connections_settings = send_settings ? &settings : nullptr;
    const QualifiedTableName * main_table_ptr = main_table ? &main_table.value() : nullptr;
    if (connection != nullptr)
        multiplexed_connections = std::make_unique<MultiplexedConnections>(
                connection, multiplexed_connections_settings, throttler);
    else if (pool != nullptr)
        multiplexed_connections = std::make_unique<MultiplexedConnections>(
                *pool, multiplexed_connections_settings, throttler,
                append_extra_info, pool_mode, main_table_ptr);
    else if (!pools.empty())
        multiplexed_connections = std::make_unique<MultiplexedConnections>(
                pools, multiplexed_connections_settings, throttler,
                append_extra_info, pool_mode, main_table_ptr);
    else
        throw Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
}

void RemoteBlockInputStream::init(const Settings * settings_)
{
    if (settings_)
    {
        send_settings = true;
        settings = *settings_;
    }
    else
        send_settings = false;
}

void RemoteBlockInputStream::sendQuery()
{
    createMultiplexedConnections();

    if (settings.skip_unavailable_shards && 0 == multiplexed_connections->size())
        return;

    established = true;

    multiplexed_connections->sendQuery(query, "", stage, &context.getClientInfo(), true);

    established = false;
    sent_query = true;

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
