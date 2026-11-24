#pragma once

#include <IO/ReadBuffer.h>
#include <Common/Logger.h>
#include <Storages/Pulsar/Client/Consumer.h>
#include <Common/VectorWithMemoryTracking.h>

#include <chrono>
#include <memory>

namespace DB
{

class StoragePulsar;

class PulsarConsumer
{
    friend class StoragePulsar;

public:
    /// `consumer_` is a subscription created by `Pulsar::Client::subscribe`. One poll returns at most
    /// `max_batch_size_` messages and waits for them for at most `poll_timeout_`.
    PulsarConsumer(
        LoggerPtr logger_, std::unique_ptr<Pulsar::Consumer> consumer_, size_t max_batch_size_, std::chrono::milliseconds poll_timeout_);

    ReadBufferPtr getNextMessage();
    ReadBufferPtr consume();

    /// Acknowledge all consumed messages. Must be called only after the blocks built
    /// from these messages have been durably written, to keep at-least-once delivery.
    void commit();
    /// Negatively acknowledge all consumed but uncommitted messages and the prefetched unread
    /// tail of the current batch, requesting redelivery. The consumer becomes unusable: the
    /// storage closes it to also redeliver the messages prefetched by the client.
    void rollback();
    /// Give up all consumed but unacknowledged messages without negatively acknowledging them,
    /// after a read that completed normally but must not acknowledge (`pulsar_commit_on_select = 0`).
    /// The consumer becomes unusable: the storage closes it, and the broker redelivers its
    /// unacknowledged messages right away, without the negative acknowledgement delay.
    void release();

    /// Whether some messages were handed to the parser but not yet acknowledged.
    bool hasUncommitted() const { return !pending_acks.empty(); }

    String currentTopic() const { return next_message[-1].topic(); }
    String currentOrderingKey() const { return next_message[-1].orderingKey(); }
    String currentPartitionKey() const { return next_message[-1].partitionKey(); }
    /// The producer-set event timestamp in milliseconds. Presence is tracked separately, because a
    /// producer may legitimately set the timestamp to 0 (the Unix epoch), which is indistinguishable
    /// from an unset one by value alone. Unlike the publish timestamp, the broker never assigns it.
    bool currentHasTimestamp() const { return next_message[-1].hasEventTime(); }
    UInt64 currentTimestamp() const { return next_message[-1].eventTime(); }
    String currentPayload() const { return String{next_message[-1].payload()}; }

    bool isStalled() const { return polled_messages.empty(); }

    /// A consumer that hit a receive error, failed to
    /// acknowledge messages, or was rolled back must not be returned to the pool: the storage
    /// drops it and recreates the slot instead.
    bool isUsable() const { return usable; }

private:
    LoggerPtr log;
    std::unique_ptr<Pulsar::Consumer> consumer;
    const size_t max_batch_size;
    const std::chrono::milliseconds poll_timeout;
    bool usable = true;
    Pulsar::Messages polled_messages;
    Pulsar::Messages::const_iterator next_message;
    VectorWithMemoryTracking<Pulsar::MessageId> pending_acks;

    bool hasPolledMessages() const { return next_message != polled_messages.end(); }
};

}
