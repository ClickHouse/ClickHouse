#include <Storages/NATS/INATSProducer.h>

#include <atomic>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>


namespace DB
{

static const auto BATCH = 1000;
static const auto MAX_BUFFERED = 131072;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_STATE;
    extern const int ABORTED;
}

INATSProducer::INATSProducer(NATSConnectionPtr connection_, String subject_, std::atomic<bool> & shutdown_called_, LoggerPtr log_)
    : AsynchronousMessageProducer(log_)
    , connection(std::move(connection_))
    , subject(std::move(subject_))
    , shutdown_called(shutdown_called_)
    , payloads(BATCH)
{
}

void INATSProducer::finishImpl()
{
    try
    {
        if (connection)
        {
            /// After the table shutdown the event loop of the connection is stopped, so flush would only wait for its timeout.
            if (!stopped_by_shutdown && !shutdown_called && connection->isConnected())
                natsConnection_Flush(connection->getConnection());

            connection->disconnect();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    if (stopped_by_shutdown)
        throw Exception(ErrorCodes::ABORTED, "Table is detached");
}

void INATSProducer::cancel() noexcept
{
    try
    {
        finish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void INATSProducer::produce(const String & message, size_t, const Columns &, size_t)
{
    if (shutdown_called)
        throw Exception(ErrorCodes::ABORTED, "Table is detached");

    if (!payloads.push(message))
    {
        if (shutdown_called)
            throw Exception(ErrorCodes::ABORTED, "Table is detached");
        throw Exception(ErrorCodes::INVALID_STATE, "Could not push to payloads queue");
    }
}

void INATSProducer::publish()
{
    String payload;

    natsStatus status = {};
    while (!payloads.empty() && !shutdown_called)
    {
        if (!connection->isConnected() || natsConnection_Buffered(connection->getConnection()) > MAX_BUFFERED)
            break;
        bool pop_result = payloads.pop(payload);

        if (!pop_result)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not pop payload");

        status = publishMessage(payload);
        if (status != NATS_OK)
        {
            LOG_DEBUG(log, "Something went wrong during publishing to NATS subject. Nats status text: {}. Last error message: {}",
                      natsStatus_GetText(status), getNATSLastError());
            if (!payloads.pushFront(payload))
                throw Exception(ErrorCodes::INVALID_STATE, "Could not push to payloads queue");
            break;
        }
    }
}

void INATSProducer::stopProducingTask()
{
    payloads.finish();
}

void INATSProducer::startProducingTaskLoop()
{
    SCOPE_EXIT(nats_ReleaseThreadMemory());

    try
    {
        while (!payloads.isFinishedAndEmpty())
        {
            if (shutdown_called)
            {
                /// The table is being shut down and its event loop, which writes the buffer of the connection, is stopped:
                /// nothing will be published anymore. Close the queue, so that `produce` does not wait for free space in it.
                stopped_by_shutdown = true;
                payloads.finish();
                LOG_DEBUG(log, "Producer on subject {} stopped by table shutdown", subject);
                return;
            }

            if (!connection->isConnected())
                std::this_thread::sleep_for(std::chrono::milliseconds(connection->getReconnectWait()));
            else
                publish();
        }

        while (!connection->isConnected() && !shutdown_called)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(connection->getReconnectWait()));
        }

        if (connection->isConnected() && natsConnection_Buffered(connection->getConnection()) > 0)
            natsConnection_Flush(connection->getConnection());
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    LOG_DEBUG(log, "Producer on subject {} completed", subject);
}

}
