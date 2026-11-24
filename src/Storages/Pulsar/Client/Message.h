#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <Storages/Pulsar/Client/BatchIndexAck.h>

namespace DB::Pulsar
{

/// Identifies a message on the broker. For a message that belongs to a batched entry, `batch_index`
/// is its position in the batch and `batch_acker` is shared by all messages of that entry (see
/// `BatchIndexAck`). For a non-batched message `batch_index` is -1 and `batch_acker` is null.
struct MessageId
{
    int64_t ledger_id = -1;
    int64_t entry_id = -1;
    int32_t partition = -1;
    int32_t batch_index = -1;
    int32_t batch_size = 0;
    /// Which sub-consumer (topic) of a multi-topic subscription this message came from, so an
    /// acknowledgement is sent to the right consumer on the right connection.
    int32_t consumer_index = 0;
    std::shared_ptr<BatchIndexAck> batch_acker;
};

/// A message delivered to a consumer, or built for a producer. Immutable after construction; copies
/// share the underlying data cheaply.
class Message
{
public:
    struct Data
    {
        std::string payload;
        std::string partition_key;
        std::string ordering_key;
        std::string topic;
        uint64_t event_timestamp = 0;
        bool has_event_timestamp = false;
        MessageId message_id;
    };

    Message() = default;
    explicit Message(std::shared_ptr<const Data> data_) : data(std::move(data_)) { }

    const void * getData() const { return data ? data->payload.data() : nullptr; }
    size_t getLength() const { return data ? data->payload.size() : 0; }
    std::string getDataAsString() const { return data ? data->payload : std::string{}; }

    const MessageId & getMessageId() const { static const MessageId empty; return data ? data->message_id : empty; }
    std::string getTopicName() const { return data ? data->topic : std::string{}; }
    std::string getPartitionKey() const { return data ? data->partition_key : std::string{}; }
    std::string getOrderingKey() const { return data ? data->ordering_key : std::string{}; }

    bool hasEventTimestamp() const { return data && data->has_event_timestamp; }
    uint64_t getEventTimestamp() const { return data ? data->event_timestamp : 0; }

private:
    std::shared_ptr<const Data> data;
};

using Messages = std::vector<Message>;
using MessageIdList = std::vector<MessageId>;

/// Builds a message to publish. Only the features the engine uses are exposed: a payload and an
/// optional ordering key.
class MessageBuilder
{
public:
    MessageBuilder & setContent(std::string content)
    {
        data->payload = std::move(content);
        return *this;
    }

    MessageBuilder & setOrderingKey(std::string key)
    {
        data->ordering_key = std::move(key);
        return *this;
    }

    MessageBuilder & setPartitionKey(std::string key)
    {
        data->partition_key = std::move(key);
        return *this;
    }

    Message build() { return Message(data); }

private:
    std::shared_ptr<Message::Data> data = std::make_shared<Message::Data>();
};

}
