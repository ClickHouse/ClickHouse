#include "LocalConnection.h"
#include <Interpreters/executeQuery.h>
#include <Storages/IStorage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

LocalConnection::LocalConnection(ContextPtr context_)
    : WithContext(context_)
{
}

void LocalConnection::setDefaultDatabase(const String & database)
{
    default_database = database;
}

void LocalConnection::getServerVersion(
    const ConnectionTimeouts & /* timeouts */, String & name,
    UInt64 & version_major, UInt64 & version_minor,
    UInt64 & version_patch, UInt64 & revision)
{
    name = server_name;
    version_major = server_version_major;
    version_minor = server_version_minor;
    version_patch = server_version_patch;
    revision = server_revision;
}

UInt64 LocalConnection::getServerRevision(const ConnectionTimeouts &)
{
    return server_revision;
}

const String & LocalConnection::getDescription() const
{
    return description;
}

const String & LocalConnection::getServerTimezone(const ConnectionTimeouts &)
{
    return server_timezone;
}

const String & LocalConnection::getServerDisplayName(const ConnectionTimeouts &)
{
    return server_display_name;
}

/*
 * SendQuery: execute query and suspend the result, which will be received back via poll.
**/
void LocalConnection::sendQuery(
    const ConnectionTimeouts &,
    const String & query_,
    const String & query_id_,
    UInt64,
    const Settings *,
    const ClientInfo *,
    bool)
{
    query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setProgressCallback([this] (const Progress & value) { return this->updateProgress(value); });
    query_context->setCurrentQueryId("");
    CurrentThread::QueryScope query_scope_holder(query_context);

    /// query_context->setCurrentDatabase(default_database);

    /// Send structure of columns to client for function input()
    // query_context->setInputInitializer([this] (ContextPtr context, const StoragePtr & input_storage)
    // {
    //     if (context != query_context)
    //         throw Exception("Unexpected context in Input initializer", ErrorCodes::LOGICAL_ERROR);

    //     auto metadata_snapshot = input_storage->getInMemoryMetadataPtr();
    //     state.need_receive_data_for_input = true;

    //     /// Send ColumnsDescription for input storage.
    //     // if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA
    //     //     && query_context->getSettingsRef().input_format_defaults_for_omitted_fields)
    //     // {
    //     //     sendTableColumns(metadata_snapshot->getColumns());
    //     // }

    //     /// Send block to the client - input storage structure.
    //     state.input_header = metadata_snapshot->getSampleBlock();
    //     next_packet_type = Protocol::Server::Data;
    //     state.block = state.input_header;
    // });

    state.query_id = query_id_;
    state.query = query_;

    state.io = executeQuery(state.query, query_context, false, state.stage, true);
    if (state.io.out)
    {
        state.need_receive_data_for_insert = true;
        processInsertQuery();
    }
    else if (state.io.pipeline.initialized())
    {
        state.executor = std::make_unique<PullingAsyncPipelineExecutor>(state.io.pipeline);
    }
    else if (state.io.in)
    {
        state.async_in = std::make_unique<AsynchronousBlockInputStream>(state.io.in);
        state.async_in->readPrefix();
    }
}

void LocalConnection::processInsertQuery()
{
    state.io.out->writePrefix();
    next_packet_type = Protocol::Server::Data;
    state.block = state.io.out->getHeader();
}


void LocalConnection::sendData(const Block & block, const String &, bool)
{
    if (block)
    {
        if (state.need_receive_data_for_input)
        {
            /// 'input' table function.
            state.block_for_input = block;
        }
        else
        {
            /// INSERT query.
            state.io.out->write(block);
        }
    }
}


void LocalConnection::sendCancel()
{
    if (state.async_in)
    {
        state.async_in->cancel(false);
    }
    else if (state.executor)
    {
        state.executor->cancel();
    }
}

Block LocalConnection::pullBlock()
{
    Block block;
    if (state.async_in)
    {
        if (state.async_in->poll(query_context->getSettingsRef().interactive_delay / 1000))
            return state.async_in->read();
    }
    else if (state.executor)
    {
        state.executor->pull(block, query_context->getSettingsRef().interactive_delay / 1000);
    }
    return block;
}

void LocalConnection::finishQuery()
{
    if (state.async_in)
    {
        state.async_in->readSuffix();
        state.async_in.reset();
    }
    else if (state.executor)
    {
        state.executor.reset();
    }

    // sendProgress();
    state.io.onFinish();
    query_context.reset();
}

bool LocalConnection::poll(size_t)
{
    if (after_send_progress.elapsed() / 1000 >= query_context->getSettingsRef().interactive_delay)
    {
        after_send_progress.restart();
        next_packet_type = Protocol::Server::Progress;

        return true;
    }

    auto block = pullBlock();
    if (block)
    {
        next_packet_type = Protocol::Server::Data;

        if (state.io.null_format)
            state.block.emplace();
        else
            state.block.emplace(block);
    }
    else
    {
        state.is_finished = true;
        next_packet_type = Protocol::Server::EndOfStream;
    }
    return true;
}

Packet LocalConnection::receivePacket()
{
    Packet packet;

    packet.type = next_packet_type.value();
    switch (next_packet_type.value())
    {
        case Protocol::Server::Data:
        {
            if (state.block)
            {
                packet.block = std::move(*state.block);
                state.block.reset();
            }

            break;
        }
        case Protocol::Server::Progress:
        {
            packet.progress = std::move(state.progress);
            state.progress.reset();
            break;
        }
        case Protocol::Server::EndOfStream:
        {
            finishQuery();
            break;
        }
        default:
            throw Exception("Unknown packet " + toString(packet.type)
                + " from server " + getDescription(), ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    }
    return packet;
}

bool LocalConnection::hasReadPendingData() const
{
    return !state.is_finished;
}

std::optional<UInt64> LocalConnection::checkPacket(size_t)
{
    return next_packet_type;
}

void LocalConnection::updateProgress(const Progress & value)
{
    state.progress.incrementPiecewiseAtomically(value);
}

}
