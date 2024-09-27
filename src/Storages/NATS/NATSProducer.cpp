#include <Storages/NATS/NATSProducer.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>


namespace DB
{

static const auto BATCH = 1000;
static const auto MAX_BUFFERED = 131072;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

NATSProducer::NATSProducer(
    const NATSConfiguration & configuration_,
    NATSOptionsPtr options_,
    const String & subject_,
    std::atomic<bool> & shutdown_called_,
    LoggerPtr log_,
    ReconnectCallback reconnect_callback_)
    : AsynchronousMessageProducer(log_)
    , connection(std::make_shared<NATSConnection>(configuration_, log_, std::move(options_)))
    , subject(subject_)
    , shutdown_called(shutdown_called_)
    , reconnect_callback(std::move(reconnect_callback_))
    , payloads(BATCH)
{
}

void NATSProducer::initialize()
{
    if (!connection->isConnected())
        reconnect_callback(connection);
}

void NATSProducer::finishImpl()
{
    connection->disconnect();
}


void NATSProducer::produce(const String & message, size_t, const Columns &, size_t)
{
    if (!payloads.push(message))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
}

void NATSProducer::publish()
{
    String payload;

    natsStatus status;
    while (!payloads.empty())
    {
        if (natsConnection_Buffered(connection->getConnection()) > MAX_BUFFERED)
            break;
        bool pop_result = payloads.pop(payload);

        if (!pop_result)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not pop payload");

        status = natsConnection_Publish(connection->getConnection(), subject.c_str(), payload.c_str(), static_cast<int>(payload.size()));

        if (status != NATS_OK)
        {
            LOG_DEBUG(log, "Something went wrong during publishing to NATS subject. Nats status text: {}. Last error message: {}",
                      natsStatus_GetText(status), nats_GetLastError(nullptr));
            if (!payloads.pushFront(payload))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
            break;
        }
    }
}

void NATSProducer::stopProducingTask()
{
    payloads.finish();
}

void NATSProducer::startProducingTaskLoop()
{
    try
    {
        while ((!payloads.isFinishedAndEmpty() || natsConnection_Buffered(connection->getConnection()) != 0) && !shutdown_called.load())
        {
            if (!connection->isConnected())
                reconnect_callback(connection);
            else
                publish();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    LOG_DEBUG(log, "Producer on subject {} completed", subject);
}

}
