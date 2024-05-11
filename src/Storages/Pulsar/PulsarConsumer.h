#pragma once

#include <IO/ReadBuffer.h>
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
    PulsarConsumer() = default;

    ReadBufferPtr getNextMessage();
    ReadBufferPtr consume();

    String currentTopic() const { return next_message[-1].getTopicName(); }
    String currentOrderingKey() const { return next_message[-1].getOrderingKey(); }
    String currentPartitionKey() const { return next_message[-1].getPartitionKey(); }
    UInt64 currentTimestamp() const { return next_message[-1].getPublishTimestamp(); }
    String currentPayload() const { return next_message[-1].getDataAsString(); }

private:
    pulsar::Consumer consumer;
    pulsar::Messages polled_messages;
    pulsar::Messages::const_iterator next_message;

    bool hasPolledMessages() const { return next_message != polled_messages.end(); }
};

}
