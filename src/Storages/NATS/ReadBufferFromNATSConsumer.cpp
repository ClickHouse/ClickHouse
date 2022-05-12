#include <utility>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <memory>
#include <Storages/NATS/ReadBufferFromNATSConsumer.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/bind.hpp>
#include <Common/logger_useful.h>
#include "Poco/Timer.h"
#include <amqpcpp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_CONNECT_NATS;
}

ReadBufferFromNATSConsumer::ReadBufferFromNATSConsumer(
        std::shared_ptr<NATSConnectionManager> connection_,
        std::vector<String> & subjects_,
        const String & subscribe_queue_name,
        Poco::Logger * log_,
        char row_delimiter_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , connection(connection_)
        , subjects(subjects_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , stopped(stopped_)
        , received(queue_size_)
{
    subscribe(subscribe_queue_name);
    LOG_DEBUG(log, "Started NATS consumer");
}


ReadBufferFromNATSConsumer::~ReadBufferFromNATSConsumer()
{
    for (const auto& subscription : subscriptions) {
        natsSubscription_Unsubscribe(subscription.get());
    }

    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromNATSConsumer::subscribe(const String & subscribe_queue_name)
{
    for (const auto & subject : subjects) {
        natsSubscription * ns;
        auto status = natsConnection_QueueSubscribe(
            &ns, connection->getConnection(), subject.c_str(), subscribe_queue_name.c_str(), onMsg, static_cast<void *>(this));
        if (status == NATS_OK)
        {
            LOG_DEBUG(log, "Subscribed to subject {}", subject);
            subscriptions.emplace_back(ns, &natsSubscription_Destroy);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to subscribe to subject {}", subject);
        }
    }
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

        if (!buffer->received.push({
                .message = std::move(message_received),
                .subject = std::move(subject),
            }))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to received queue");
    }

    natsMsg_Destroy(msg);
}

}
