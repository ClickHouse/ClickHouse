#include <Storages/NATS/WriteBufferToNATSProducer.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <boost/algorithm/string/split.hpp>
#include <Common/logger_useful.h>


namespace DB
{

static const auto BATCH = 1000;
static const auto MAX_BUFFERED = 131072;

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

    writing_task = global_context->getSchedulePool().createTask("NATSWritingTask", [this] { writingFunc(); });
    writing_task->deactivate();

    reinitializeChunks();
}


WriteBufferToNATSProducer::~WriteBufferToNATSProducer()
{
    writing_task->deactivate();
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

        if (!payloads.push(payload))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
    }
}

void WriteBufferToNATSProducer::publish()
{
    uv_thread_t flush_thread;

    uv_thread_create(&flush_thread, publishThreadFunc, static_cast<void *>(this));

    connection.getHandler().startLoop();
    uv_thread_join(&flush_thread);
}

void WriteBufferToNATSProducer::publishThreadFunc(void * arg)
{
    WriteBufferToNATSProducer * buffer = static_cast<WriteBufferToNATSProducer *>(arg);
    String payload;

    natsStatus status;
    while (!buffer->payloads.empty())
    {
        if (natsConnection_Buffered(buffer->connection.getConnection()) > MAX_BUFFERED)
            break;
        bool pop_result = buffer->payloads.pop(payload);

        if (!pop_result)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not pop payload");
        status = natsConnection_PublishString(buffer->connection.getConnection(), buffer->subject.c_str(), payload.c_str());

        if (status != NATS_OK)
        {
            LOG_DEBUG(buffer->log, "Something went wrong during publishing to NATS subject. Nats status text: {}. Last error message: {}",
                      natsStatus_GetText(status), nats_GetLastError(nullptr));
            if (!buffer->payloads.push(std::move(payload)))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
            break;
        }
    }

    nats_ReleaseThreadMemory();
}


void WriteBufferToNATSProducer::writingFunc()
{
    try
    {
        while ((!payloads.empty() || wait_all) && !shutdown_called.load())
        {
            publish();

            LOG_DEBUG(
                log, "Writing func {} {} {}", wait_payloads.load(), payloads.empty(), natsConnection_Buffered(connection.getConnection()));
            if (wait_payloads.load() && payloads.empty() && natsConnection_Buffered(connection.getConnection()) == 0)
                wait_all = false;

            if (!connection.isConnected() && wait_all)
                connection.reconnect();

            iterateEventLoop();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
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
