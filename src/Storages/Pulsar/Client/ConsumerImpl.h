#pragma once

#include "config.h"

#if USE_PULSAR

#include <Storages/Pulsar/Client/Client.h>
#include <Storages/Pulsar/Client/Connection.h>
#include <Storages/Pulsar/Client/Message.h>
#include <Storages/Pulsar/Client/Protocol.h>

#include <span>


namespace DB::Pulsar
{

class ConsumerImpl final : public Session::Owner
{
public:
    ConsumerImpl(ClientStatePtr client_, VectorWithMemoryTracking<String> topics_, String subscription_, ConsumerConfiguration configuration_);
    ~ConsumerImpl();

    /// Within the operation timeout.
    void subscribe();

    Messages batchReceive(size_t max_messages, std::chrono::milliseconds timeout);
    void acknowledge(std::span<const MessageId> ids);
    void negativeAcknowledge(const MessageId & id) noexcept;
    void close() noexcept;

private:
    enum class State : uint8_t
    {
        NeedLookup,
        LookupPending,
        Connecting,
        SubscribePending,
        Ready,
    };

    /// The consumer of one topic partition.
    struct SubConsumer
    {
        SubConsumer(String topic_, size_t index_, size_t window_);

        std::shared_ptr<const String> topic;
        const size_t index;
        /// The number of messages to prefetch.
        const size_t window;

        State state = State::NeedLookup;
        Session::RequestPtr lookup;
        UInt64 connection_id = 0;
        /// A new one for every subscription.
        UInt64 consumer_id = 0;
        UInt64 subscribe_request_id = 0;
        TimePoint step_started;

        /// Since the subscription. The broker sends an entry only while it has permits left (counted in messages),
        /// so it never sends more entries than the granted permits.
        size_t granted = 0;
        size_t received_messages = 0;
        size_t received_entries = 0;
        /// The permits to return with the next `FLOW`.
        size_t pending_flow = 0;
        /// The messages in the queue.
        size_t queued = 0;

        ChunkAssembler chunks;
        /// The acknowledgements to send when the sub-consumer is ready.
        VectorWithMemoryTracking<EntryId> pending_acks;
    };

    struct QueuedEntry
    {
        size_t sub_consumer = 0;
        UInt64 consumer_id = 0;
        EntryId entry;
        BatchCursor cursor;
        std::shared_ptr<const VectorWithMemoryTracking<EntryId>> chunks;
    };

    struct BatchKey
    {
        size_t sub_consumer = 0;
        EntryId entry;
        bool operator==(const BatchKey &) const = default;
    };

    struct BatchKeyHash
    {
        size_t operator()(const BatchKey & key) const;
    };

    struct PartialBatch
    {
        VectorWithMemoryTracking<bool> acked;
        size_t unacked = 0;
    };

    struct PartitionedTopic
    {
        String name;
        UInt32 partitions = 0;
    };

    void onFrame(Connection & connection, Frame & frame, const SharedBuffer & buffer) override;
    void onConnectionClosed(Connection & connection) override;
    void onMessage(Connection & connection, SubConsumer & sub, Frame & frame, const SharedBuffer & buffer);

    void checkUsable() const;
    size_t windowFor(size_t total_sub_consumers) const;
    void addSubConsumer(String topic, size_t window);
    void advance();
    void refreshPartitions(TimePoint now);
    void resetSubConsumer(SubConsumer & sub, const char * reason);
    SubConsumer * findSubConsumer(Connection & connection, UInt64 consumer_id);
    void sendFlow(SubConsumer & sub);
    void sendAck(SubConsumer & sub, const VectorWithMemoryTracking<EntryId> & ids, std::optional<ValidationError> error);
    void sendRedeliver(SubConsumer & sub, const VectorWithMemoryTracking<EntryId> & ids);
    bool nothingMoreCanArrive() const;
    Messages takeMessages(size_t max_messages);
    void flushAcks();

    ClientStatePtr client;
    Session session;
    const VectorWithMemoryTracking<String> topics;
    const String subscription;
    const ConsumerConfiguration configuration;
    const UInt64 serial;
    const String consumer_name;
    LoggerPtr log;

    VectorWithMemoryTracking<std::unique_ptr<SubConsumer>> sub_consumers;
    UnorderedMapWithMemoryTracking<UInt64, SubConsumer *> by_consumer_id;
    UInt64 next_consumer_id = 1;

    /// The received messages in the order of arrival.
    DequeWithMemoryTracking<QueuedEntry> queue;
    size_t queued_messages = 0;

    /// The acknowledged messages of the batches that are not acknowledged completely.
    UnorderedMapWithMemoryTracking<BatchKey, PartialBatch, BatchKeyHash> partial_acks;
    /// The connections with acknowledgements to send.
    VectorWithMemoryTracking<UInt64> acked_connections;

    VectorWithMemoryTracking<PartitionedTopic> partitioned_topics;
    TimePoint next_refresh;
    VectorWithMemoryTracking<Session::RequestPtr> refresh_requests;

    Entry entry;
    bool failed = false;
    bool closed = false;
};

}

#endif
