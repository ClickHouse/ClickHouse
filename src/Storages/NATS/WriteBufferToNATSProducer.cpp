#include <Storages/NATS/WriteBufferToNATSProducer.h>

#include <Core/Block.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <amqpcpp.h>
#include <uv.h>
#include <boost/algorithm/string/split.hpp>
#include <chrono>
#include <thread>
#include <atomic>


namespace DB
{

static const auto BATCH = 1000;

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
    extern const int LOGICAL_ERROR;
}

WriteBufferToNATSProducer::WriteBufferToNATSProducer(
        const NATSConfiguration & configuration_,
        ContextPtr global_context,
        const String & subject_,
        std::atomic<bool> & shutdown_called_,
        Poco::Logger * log_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_)
        : WriteBuffer(nullptr, 0)
        , connection(configuration_, log_)
        , subject(subject_)
        , shutdown_called(shutdown_called_)
        , payloads(BATCH)
        , log(log_)
        , delim(delimiter)
        , max_rows(rows_per_message)
        , chunk_size(chunk_size_)
{
    if (!connection.connect())
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot connect to NATS {}", connection.connectionInfoForLog());

    writing_task = global_context->getSchedulePool().createTask("NATSWritingTask", [this]{ writingFunc(); });
    writing_task->deactivate();

    reinitializeChunks();
}


WriteBufferToNATSProducer::~WriteBufferToNATSProducer()
{
    writing_task->deactivate();
    connection.disconnect();
    assert(rows == 0);
}


void WriteBufferToNATSProducer::countRow()
{
    if (++rows % max_rows == 0)
    {
        const std::string & last_chunk = chunks.back();
        size_t last_chunk_size = offset();

        if (last_chunk_size && delim && last_chunk[last_chunk_size - 1] == delim)
            --last_chunk_size;

        std::string payload;
        payload.reserve((chunks.size() - 1) * chunk_size + last_chunk_size);

        for (auto i = chunks.begin(), end = --chunks.end(); i != end; ++i)
            payload.append(*i);

        payload.append(last_chunk, 0, last_chunk_size);

        reinitializeChunks();

        ++payload_counter;
        if (!payloads.push(payload))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
    }
}

natsStatus WriteBufferToNATSProducer::publish()
{
    String payload;

    natsStatus status{NATS_OK};
    while (!payloads.empty())
    {
        bool pop_result = payloads.pop(payload);

        if (!pop_result)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not pop payload");
        status = natsConnection_PublishString(connection.getConnection(), subject.c_str(), payload.c_str());

        if (status != NATS_OK)
        {
            LOG_DEBUG(log, "Something went wrong during publishing to NATS subject. Nats status text: {}. Last error message: {}",
                      natsStatus_GetText(status), nats_GetLastError(nullptr));
            break;
        }
    }

    if (status == NATS_OK)
    {
        status = natsConnection_Flush(connection.getConnection());
        if (status != NATS_OK)
            LOG_DEBUG(log, "Something went wrong during flushing NATS connection. Nats status text: {}. Last error message: {}",
                      natsStatus_GetText(status), nats_GetLastError(nullptr));
    }

    iterateEventLoop();
    return status;
}


void WriteBufferToNATSProducer::writingFunc()
{
    while ((!payloads.empty() || wait_all) && !shutdown_called.load())
    {
        auto status = publish();

        if (wait_payloads.load() && payloads.empty())
            wait_all = false;

        if (status != NATS_OK && wait_all)
            connection.reconnect();

        iterateEventLoop();
    }

    LOG_DEBUG(log, "Producer on subject {} completed", subject);
}


void WriteBufferToNATSProducer::nextImpl()
{
    addChunk();
}

void WriteBufferToNATSProducer::addChunk()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}

void WriteBufferToNATSProducer::reinitializeChunks()
{
    rows = 0;
    chunks.clear();
    /// We cannot leave the buffer in the undefined state (i.e. without any
    /// underlying buffer), since in this case the WriteBuffeR::next() will
    /// not call our nextImpl() (due to available() == 0)
    addChunk();
}


void WriteBufferToNATSProducer::iterateEventLoop()
{
    connection.getHandler().iterateLoop();
}

}
