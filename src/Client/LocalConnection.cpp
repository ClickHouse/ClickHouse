#include "LocalConnection.h"
#include <Interpreters/executeQuery.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int UNKNOWN_EXCEPTION;
}

LocalConnection::LocalConnection(ContextPtr context_)
    : WithContext(context_)
    , session(getContext(), ClientInfo::Interface::TCP)
{
    /// Authenticate and create a context to execute queries.
    session.authenticate("default", "", Poco::Net::SocketAddress{});
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
    /// Use the same context for all queries.
    // applyCmdSettings(query_context);

    /// query_context->setCurrentDatabase(default_database);

    state.reset();
    state.emplace();

    state->query_id = query_id_;
    state->query = query_;

    query_context = session.makeQueryContext();
    query_context->makeSessionContext(); /// initial_create_query requires a session context to be set.
    query_context->setCurrentQueryId("");
    CurrentThread::QueryScope query_scope_holder(query_context);

    try
    {
        state->io = executeQuery(state->query, query_context, false, state->stage, true);
        if (state->io.out)
        {
            state->need_receive_data_for_insert = true;
            processInsertQuery();
        }
        else if (state->io.pipeline.initialized())
        {
            state->executor = std::make_unique<PullingAsyncPipelineExecutor>(state->io.pipeline);
        }
        else if (state->io.in)
        {
            state->async_in = std::make_unique<AsynchronousBlockInputStream>(state->io.in);
            state->async_in->readPrefix();
        }
    }
    catch (const Exception & e)
    {
        state->io.onException();
        state->exception.emplace(e);
    }
    catch (const std::exception & e)
    {
        state->io.onException();
        state->exception.emplace(Exception::CreateFromSTDTag{}, e);
    }
    catch (...)
    {
        state->io.onException();
        state->exception.emplace("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
    }
}

void LocalConnection::processInsertQuery()
{
    state->io.out->writePrefix();
    next_packet_type = Protocol::Server::Data;
    state->block = state->io.out->getHeader();
}

void LocalConnection::sendData(const Block & block, const String &, bool)
{
    if (block)
    {
        if (state->need_receive_data_for_input)
        {
            /// 'input' table function.
            state->block_for_input = block;
        }
        else
        {
            /// INSERT query.
            state->io.out->write(block);
        }
    }
}

void LocalConnection::sendCancel()
{
    if (state->async_in)
    {
        state->async_in->cancel(false);
    }
    else if (state->executor)
    {
        state->executor->cancel();
    }
}

bool LocalConnection::pullBlock(Block & block)
{
    if (state->async_in)
    {
        if (state->async_in->poll(interactive_delay / 1000))
            block = state->async_in->read();

        if (block)
            return true;
    }
    else if (state->executor)
    {
        return state->executor->pull(block, interactive_delay / 1000);
    }

    return false;
}

void LocalConnection::finishQuery()
{
    if (state->async_in)
    {
        state->async_in->readSuffix();
        state->async_in.reset();
    }
    else if (state->executor)
    {
        state->executor.reset();
    }

    state->io.onFinish();
    state.reset();
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

    try
    {
        pollImpl();
    }
    catch (const Exception & e)
    {
        state->io.onException();
        state->exception.emplace(e);
    }
    catch (const std::exception & e)
    {
        state->io.onException();
        state->exception.emplace(Exception::CreateFromSTDTag{}, e);
    }
    catch (...)
    {
        state->io.onException();
        state->exception.emplace("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
    }

    if (state->exception)
    {
        next_packet_type = Protocol::Server::Exception;
        return true;
    }

    if (state->is_finished && !state->sent_totals)
    {
        state->sent_totals = true;
        Block totals;

        if (state->io.in)
            totals = state->io.in->getTotals();
        else if (state->executor)
            totals = state->executor->getTotalsBlock();

        if (totals)
        {
            next_packet_type = Protocol::Server::Totals;
            state->block.emplace(totals);
            return true;
        }
    }

    if (state->is_finished && !state->sent_extremes)
    {
        state->sent_extremes = true;
        Block extremes;

        if (state->io.in)
            extremes = state->io.in->getExtremes();
        else if (state->executor)
            extremes = state->executor->getExtremesBlock();

        if (extremes)
        {
            next_packet_type = Protocol::Server::Extremes;
            state->block.emplace(extremes);
            return true;
        }
    }

    if (state->is_finished)
    {
        finishQuery();
        next_packet_type = Protocol::Server::EndOfStream;
        return true;
    }

    if (state->block)
    {
        next_packet_type = Protocol::Server::Data;
        return true;
    }

    return false;
}

bool LocalConnection::pollImpl()
{
    Block block;
    auto next_read = pullBlock(block);
    if (block)
    {
        if (state->io.null_format)
            state->block.emplace();
        else
            state->block.emplace(block);
    }
    else if (!next_read)
    {
        state->is_finished = true;
    }

    return true;
}

Packet LocalConnection::receivePacket()
{
    Packet packet;

    packet.type = next_packet_type.value();
    switch (next_packet_type.value())
    {
        case Protocol::Server::Totals: [[fallthrough]];
        case Protocol::Server::Extremes: [[fallthrough]];
        case Protocol::Server::Data:
        {
            if (state->block)
            {
                packet.block = std::move(*state->block);
                state->block.reset();
            }
            break;
        }
        case Protocol::Server::Exception:
        {
            packet.exception = std::make_unique<Exception>(*state->exception);
            break;
        }
        case Protocol::Server::Progress:
        {
            packet.progress = std::move(state->progress);
            state->progress.reset();
            break;
        }
        case Protocol::Server::EndOfStream:
        {
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
    return !state->is_finished;
}

std::optional<UInt64> LocalConnection::checkPacket(size_t)
{
    return next_packet_type;
}

void LocalConnection::updateProgress(const Progress & value)
{
    state->progress.incrementPiecewiseAtomically(value);
}

}
