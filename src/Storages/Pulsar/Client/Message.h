#pragma once

#include "config.h"

#if USE_PULSAR

#include <base/types.h>
#include <Common/StringWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <memory>
#include <optional>
#include <string_view>


namespace DB::Pulsar
{

/// The position of a message (or of a batch of messages, or of a chunk) in a topic partition.
struct EntryId
{
    UInt64 ledger_id = 0;
    UInt64 entry_id = 0;

    bool operator==(const EntryId &) const = default;
};

/// Identifies a received message in `Consumer::acknowledge` and `Consumer::negativeAcknowledge`.
class MessageId
{
public:
    UInt64 ledgerId() const { return entry.ledger_id; }
    UInt64 entryId() const { return entry.entry_id; }
    /// -1 when the message is not a part of a batch.
    Int32 batchIndex() const { return batch_index; }

    String toString() const;

private:
    friend class ConsumerImpl;

    /// The consumer that received the message.
    UInt64 consumer_serial = 0;
    size_t sub_consumer = 0;
    EntryId entry;
    Int32 batch_index = -1;
    Int32 batch_size = 0;
    /// For a chunked message, the positions of all its chunks.
    std::shared_ptr<const VectorWithMemoryTracking<EntryId>> chunks;
};

class Message
{
public:
    std::string_view payload() const { return data; }
    /// The topic partition the message was received from.
    const String & topic() const { return *topic_name; }
    /// Empty if not set.
    const String & orderingKey() const { return ordering_key; }
    const String & partitionKey() const { return partition_key; }
    /// The event time set by the producer, in milliseconds since the epoch. It may be set to 0.
    bool hasEventTime() const { return event_time.has_value(); }
    UInt64 eventTime() const { return event_time.value_or(0); }
    const MessageId & id() const { return message_id; }

private:
    friend class ConsumerImpl;

    std::shared_ptr<const StringWithMemoryTracking> buffer;
    std::string_view data;
    std::shared_ptr<const String> topic_name;
    String ordering_key;
    String partition_key;
    std::optional<UInt64> event_time;
    MessageId message_id;
};

using Messages = VectorWithMemoryTracking<Message>;

}

#endif
