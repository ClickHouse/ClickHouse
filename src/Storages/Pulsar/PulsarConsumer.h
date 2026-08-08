#pragma once

#include <IO/ReadBuffer.h>
#include <Common/Logger.h>
#include <pulsar/BatchReceivePolicy.h>
#include <pulsar/Consumer.h>
#include <pulsar/MessageBatch.h>

namespace DB
{

class StoragePulsar;

class PulsarConsumer
{
    friend class StoragePulsar;

public:
    explicit PulsarConsumer(LoggerPtr logger_);

    ReadBufferPtr getNextMessage();
    ReadBufferPtr consume();

    /// Acknowledge all consumed messages. Must be called only after the blocks built
    /// from these messages have been durably written, to keep at-least-once delivery.
    void commit();
    /// Negatively acknowledge all consumed but uncommitted messages and the prefetched unread
    /// tail of the current batch, requesting redelivery.
    void rollback();

    /// Whether some messages were handed to the parser but not yet acknowledged.
    bool hasUncommitted() const { return !pending_acks.empty(); }

    String currentTopic() const { return next_message[-1].getTopicName(); }
    String currentOrderingKey() const { return next_message[-1].getOrderingKey(); }
    String currentPartitionKey() const { return next_message[-1].getPartitionKey(); }
    UInt64 currentTimestamp() const { return next_message[-1].getPublishTimestamp(); }
    String currentPayload() const { return next_message[-1].getDataAsString(); }

    bool isStalled() const { return polled_messages.empty(); }

    /// A consumer that hit a terminal receive error (e.g. `ResultAlreadyClosed`) must not be
    /// returned to the pool: the storage drops it and recreates the slot instead.
    bool isUsable() const { return usable; }

private:
    LoggerPtr log;
    pulsar::Consumer consumer;
    bool usable = true;
    pulsar::Messages polled_messages;
    pulsar::Messages::const_iterator next_message;
    pulsar::MessageIdList pending_acks;

    bool hasPolledMessages() const { return next_message != polled_messages.end(); }
};

}
