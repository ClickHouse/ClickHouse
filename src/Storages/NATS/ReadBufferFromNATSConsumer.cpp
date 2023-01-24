#include <atomic>
#include <chrono>
#include <memory>
#include <utility>
#include <Storages/NATS/ReadBufferFromNATSConsumer.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/bind.hpp>
#include "Poco/Timer.h"
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_CONNECT_NATS;
}

ReadBufferFromNATSConsumer::ReadBufferFromNATSConsumer(
    std::shared_ptr<NATSConnectionManager> connection_,
    StorageNATS & storage_,
    std::vector<String> & subjects_,
    const String & subscribe_queue_name,
    Poco::Logger * log_,
    char row_delimiter_,
    uint32_t queue_size_,
    const std::atomic<bool> & stopped_)
    : ReadBuffer(nullptr, 0)
    , connection(connection_)
    , storage(storage_)
    , subjects(subjects_)
    , log(log_)
    , row_delimiter(row_delimiter_)
    , stopped(stopped_)
    , queue_name(subscribe_queue_name)
    , received(queue_size_)
{
}

void ReadBufferFromNATSConsumer::subscribe()
{
    if (subscribed)
        return;

    for (const auto & subject : subjects)
    {
        natsSubscription * ns;
        auto status = natsConnection_QueueSubscribe(
            &ns, connection->getConnection(), subject.c_str(), queue_name.c_str(), onMsg, static_cast<void *>(this));
        if (status == NATS_OK)
        {
            LOG_DEBUG(log, "Subscribed to subject {}", subject);
            natsSubscription_SetPendingLimits(ns, -1, -1);
            subscriptions.emplace_back(ns, &natsSubscription_Destroy);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to subscribe to subject {}", subject);
        }
    }
    subscribed = true;
}

void ReadBufferFromNATSConsumer::unsubscribe()
{
    for (const auto & subscription : subscriptions)
        natsSubscription_Unsubscribe(subscription.get());
}

bool ReadBufferFromNATSConsumer::nextImpl()
{
    if (stopped || !allowed)
        return false;

    if (received.tryPop(current))
    {
        auto * new_position = const_cast<char *>(current.message.data());
        BufferBase::set(new_position, current.message.size(), 0);
        allowed = false;

        return true;
    }

    return false;
}

void ReadBufferFromNATSConsumer::onMsg(natsConnection *, natsSubscription *, natsMsg * msg, void * consumer)
{
    auto * buffer = static_cast<ReadBufferFromNATSConsumer *>(consumer);
    const int msg_length = natsMsg_GetDataLength(msg);

    if (msg_length)
    {
        String message_received = std::string(natsMsg_GetData(msg), msg_length);
        String subject = natsMsg_GetSubject(msg);
        if (buffer->row_delimiter != '\0')
            message_received += buffer->row_delimiter;

        MessageData data = {
            .message = message_received,
            .subject = subject,
        };
        if (!buffer->received.push(std::move(data)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to received queue");

        buffer->storage.startStreaming();
    }

    natsMsg_Destroy(msg);
}

}
