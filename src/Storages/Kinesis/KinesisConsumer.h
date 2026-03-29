#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <optional>

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/logger_useful.h>
#include <Core/Types.h>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/ShardIteratorType.h>

namespace DB
{

struct KinesisShardState
{
    String iterator;        /// current shard iterator (empty = not yet initialized or shard is closed)
    String last_sequence;  /// last successfully consumed sequence number (empty if nothing consumed yet)
    bool is_closed = false;
};

class KinesisConsumer
{
public:
    struct Message
    {
        String data;
        String sequence_number;
        String partition_key;
        String shard_id;
        UInt64 approximate_arrival_timestamp = 0; /// unix timestamp in seconds
    };

    KinesisConsumer(
        const String & stream_name_,
        std::shared_ptr<Aws::Kinesis::KinesisClient> client_,
        std::map<String, KinesisShardState> shard_states_,
        size_t max_records_per_request_,
        Aws::Kinesis::Model::ShardIteratorType starting_position_type_,
        UInt64 at_timestamp_,
        size_t internal_queue_size_);

    ~KinesisConsumer();

    void stop();
    bool isStopped() const { return !is_running; }

    /// Poll one round of records across all assigned shards
    bool receive();

    /// Pop one message from the internal queue
    std::optional<Message> getMessage();

    /// Get shard states (last seqnos) for checkpointng
    std::map<String, KinesisShardState> getShardStates() const;

private:
    /// Get or refresh the shard iterator for a given shard
    String getOrRefreshIterator(const String & shard_id, KinesisShardState & state);

    const String stream_name;
    std::shared_ptr<Aws::Kinesis::KinesisClient> client;
    const size_t max_records_per_request;
    const Aws::Kinesis::Model::ShardIteratorType starting_position_type;
    const UInt64 at_timestamp;

    std::map<String, KinesisShardState> shard_states;
    mutable std::mutex shard_mutex;

    ConcurrentBoundedQueue<Message> queue;
    std::atomic<bool> is_running{true};

    LoggerPtr log;
};

using KinesisConsumerPtr = std::shared_ptr<KinesisConsumer>;

}

#endif // USE_AWS_KINESIS