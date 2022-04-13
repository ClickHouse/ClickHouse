#include "LocalConnection.h"
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Storages/IStorage.h>
#include <Core/Protocol.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int UNKNOWN_EXCEPTION;
    extern const int NOT_IMPLEMENTED;
}

LocalConnection::LocalConnection(ContextPtr context_, bool send_progress_, bool send_profile_events_, const String & server_display_name_)
    : WithContext(context_)
    , session(getContext(), ClientInfo::Interface::LOCAL)
    , send_progress(send_progress_)
    , send_profile_events(send_profile_events_)
    , server_display_name(server_display_name_)
{
    /// Authenticate and create a context to execute queries.
    session.authenticate("default", "", Poco::Net::SocketAddress{});
    session.makeSessionContext();

    if (!CurrentThread::isInitialized())
        thread_status.emplace();
}

LocalConnection::~LocalConnection()
{
    try
    {
        state.reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
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

void LocalConnection::getProfileEvents(Block & block)
{
    ProfileEvents::getProfileEvents(server_display_name, state->profile_queue, block, last_sent_snapshots);
}

void LocalConnection::sendQuery(
    const ConnectionTimeouts &,
    const String & query,
    const String & query_id,
    UInt64 stage,
    const Settings *,
    const ClientInfo *,
    bool)
{
    query_context = session.makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    if (send_progress)
    {
        query_context->setProgressCallback([this] (const Progress & value) { return this->updateProgress(value); });
        query_context->setFileProgressCallback([this](const FileProgress & value) { this->updateProgress(Progress(value)); });
    }
    if (!current_database.empty())
        query_context->setCurrentDatabase(current_database);


    state.reset();
    state.emplace();

    state->query_id = query_id;
    state->query = query;
    state->query_scope_holder = std::make_unique<CurrentThread::QueryScope>(query_context);
    state->stage = QueryProcessingStage::Enum(stage);
    state->profile_queue = std::make_shared<InternalProfileEventsQueue>(std::numeric_limits<int>::max());
    CurrentThread::attachInternalProfileEventsQueue(state->profile_queue);

    if (send_progress)
        state->after_send_progress.restart();

    if (send_profile_events)
        state->after_send_profile_events.restart();

    next_packet_type.reset();

    try
    {
        state->io = executeQuery(state->query, query_context, false, state->stage);

        if (state->io.pipeline.pushing())
        {
            size_t num_threads = state->io.pipeline.getNumThreads();
            if (num_threads > 1)
            {
                state->pushing_async_executor = std::make_unique<PushingAsyncPipelineExecutor>(state->io.pipeline);
                state->pushing_async_executor->start();
                state->block = state->pushing_async_executor->getHeader();
            }
            else
            {
                state->pushing_executor = std::make_unique<PushingPipelineExecutor>(state->io.pipeline);
                state->pushing_executor->start();
                state->block = state->pushing_executor->getHeader();
            }

            const auto & table_id = query_context->getInsertionTable();
            if (query_context->getSettingsRef().input_format_defaults_for_omitted_fields)
            {
                if (!table_id.empty())
                {
                    auto storage_ptr = DatabaseCatalog::instance().getTable(table_id, query_context);
                    state->columns_description = storage_ptr->getInMemoryMetadataPtr()->getColumns();
                }
            }
        }
        else if (state->io.pipeline.pulling())
        {
            state->block = state->io.pipeline.getHeader();
            state->executor = std::make_unique<PullingAsyncPipelineExecutor>(state->io.pipeline);
        }
        else if (state->io.pipeline.completed())
        {
            CompletedPipelineExecutor executor(state->io.pipeline);
            executor.execute();
        }

        if (state->columns_description)
            next_packet_type = Protocol::Server::TableColumns;
        else if (state->block)
            next_packet_type = Protocol::Server::Data;
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

void LocalConnection::sendData(const Block & block, const String &, bool)
{
    if (!block)
        return;

    if (state->pushing_async_executor)
    {
        state->pushing_async_executor->push(block);
    }
    else if (state->pushing_executor)
    {
        state->pushing_executor->push(block);
    }
}

void LocalConnection::sendCancel()
{
    if (state->executor)
        state->executor->cancel();
}

bool LocalConnection::pullBlock(Block & block)
{
    if (state->executor)
        return state->executor->pull(block, query_context->getSettingsRef().interactive_delay / 1000);

    return false;
}

void LocalConnection::finishQuery()
{
    next_packet_type = Protocol::Server::EndOfStream;

    if (state->executor)
    {
        state->executor.reset();
    }
    else if (state->pushing_async_executor)
    {
        state->pushing_async_executor->finish();
    }
    else if (state->pushing_executor)
    {
        state->pushing_executor->finish();
    }

    state->io.onFinish();
    state.reset();
    last_sent_snapshots.clear();
}

bool LocalConnection::poll(size_t)
{
    if (!state)
        return false;

    /// Wait for next poll to collect current packet.
    if (next_packet_type)
        return true;

    if (state->exception)
    {
        next_packet_type = Protocol::Server::Exception;
        return true;
    }

    if (!state->is_finished)
    {
        if (send_progress && (state->after_send_progress.elapsedMicroseconds() >= query_context->getSettingsRef().interactive_delay))
        {
            state->after_send_progress.restart();
            next_packet_type = Protocol::Server::Progress;
            return true;
        }

        if (send_profile_events && (state->after_send_profile_events.elapsedMicroseconds() >= query_context->getSettingsRef().interactive_delay))
        {
            Block block;
            state->after_send_profile_events.restart();
            next_packet_type = Protocol::Server::ProfileEvents;
            getProfileEvents(block);
            state->block.emplace(std::move(block));
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

        if (state->executor)
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

        if (state->executor)
            extremes = state->executor->getExtremesBlock();

        if (extremes)
        {
            next_packet_type = Protocol::Server::Extremes;
            state->block.emplace(extremes);
            return true;
        }
    }

    if (state->is_finished && !state->sent_profile_info)
    {
        state->sent_profile_info = true;

        if (state->executor)
        {
            next_packet_type = Protocol::Server::ProfileInfo;
            state->profile_info = state->executor->getProfileInfo();
            return true;
        }
    }

    if (state->is_finished && !state->sent_profile_events)
    {
        state->sent_profile_events = true;

        if (send_profile_events && state->executor)
        {
            Block block;
            state->after_send_profile_events.restart();
            next_packet_type = Protocol::Server::ProfileEvents;
            getProfileEvents(block);
            state->block.emplace(std::move(block));
            return true;
        }
    }

    if (state->is_finished)
    {
        finishQuery();
        return true;
    }

    if (state->block && state->block.value())
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

    if (block && !state->io.null_format)
    {
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
    if (!state)
    {
        packet.type = Protocol::Server::EndOfStream;
        return packet;
    }

    if (!next_packet_type)
        poll(0);

    if (!next_packet_type)
    {
        packet.type = Protocol::Server::EndOfStream;
        return packet;
    }

    packet.type = next_packet_type.value();
    switch (next_packet_type.value())
    {
        case Protocol::Server::Totals: [[fallthrough]];
        case Protocol::Server::Extremes: [[fallthrough]];
        case Protocol::Server::Log: [[fallthrough]];
        case Protocol::Server::Data:
        case Protocol::Server::ProfileEvents:
        {
            if (state->block && state->block.value())
            {
                packet.block = std::move(state->block.value());
                state->block.reset();
            }
            next_packet_type.reset();
            break;
        }
        case Protocol::Server::ProfileInfo:
        {
            if (state->profile_info)
            {
                packet.profile_info = std::move(*state->profile_info);
                state->profile_info.reset();
            }
            next_packet_type.reset();
            break;
        }
        case Protocol::Server::TableColumns:
        {
            if (state->columns_description)
            {
                /// Send external table name (empty name is the main table)
                /// (see TCPHandler::sendTableColumns)
                packet.multistring_message = {"", state->columns_description->toString()};
            }

            if (state->block)
            {
                next_packet_type = Protocol::Server::Data;
            }

            break;
        }
        case Protocol::Server::Exception:
        {
            packet.exception = std::make_unique<Exception>(*state->exception);
            next_packet_type.reset();
            break;
        }
        case Protocol::Server::Progress:
        {
            packet.progress = std::move(state->progress);
            state->progress.reset();
            next_packet_type.reset();
            break;
        }
        case Protocol::Server::EndOfStream:
        {
            next_packet_type.reset();
            break;
        }
        default:
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER,
                            "Unknown packet {} for {}", toString(packet.type), getDescription());
    }

    return packet;
}

void LocalConnection::getServerVersion(
    const ConnectionTimeouts & /* timeouts */, String & /* name */,
    UInt64 & /* version_major */, UInt64 & /* version_minor */,
    UInt64 & /* version_patch */, UInt64 & /* revision */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

void LocalConnection::setDefaultDatabase(const String & database)
{
    current_database = database;
}

UInt64 LocalConnection::getServerRevision(const ConnectionTimeouts &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

const String & LocalConnection::getServerTimezone(const ConnectionTimeouts &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

const String & LocalConnection::getServerDisplayName(const ConnectionTimeouts &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

void LocalConnection::sendExternalTablesData(ExternalTablesData &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

void LocalConnection::sendMergeTreeReadTaskResponse(const PartitionReadResponse &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

ServerConnectionPtr LocalConnection::createConnection(
    const ConnectionParameters &,
    ContextPtr current_context,
    bool send_progress,
    bool send_profile_events,
    const String & server_display_name)
{
    return std::make_unique<LocalConnection>(current_context, send_progress, send_profile_events, server_display_name);
}


}
