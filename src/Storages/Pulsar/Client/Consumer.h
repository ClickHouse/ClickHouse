#pragma once

#include "config.h"

#if USE_PULSAR

#include <Storages/Pulsar/Client/Message.h>

#include <chrono>
#include <memory>
#include <span>


namespace DB::Pulsar
{

class ConsumerImpl;

/// A consumer of a `Shared` subscription to all partitions of some topics, created by `Client::subscribe`.
class Consumer
{
public:
    explicit Consumer(std::unique_ptr<ConsumerImpl> impl_);
    ~Consumer();

    /// Returns at most `max_messages`: as soon as that many are received, or as soon as some are received and no more
    /// can arrive before the received ones are returned, or when `timeout` expires (possibly none).
    Messages batchReceive(size_t max_messages, std::chrono::milliseconds timeout);

    /// Sends the acknowledgements and waits until the socket accepts them. Those on a connection lost before that are not
    /// sent, and the broker redelivers their messages. Those of a partition that is subscribing again are sent when it has
    /// subscribed.
    void acknowledge(std::span<const MessageId> ids);

    /// Requests redelivery. Does nothing on a consumer that has failed, because closing it redelivers anyway.
    void negativeAcknowledge(const MessageId & id) noexcept;

    /// The broker redelivers the unacknowledged messages.
    void close() noexcept;

private:
    std::unique_ptr<ConsumerImpl> impl;
};

}

#endif
