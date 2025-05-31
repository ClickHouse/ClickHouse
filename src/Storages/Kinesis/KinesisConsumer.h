#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Core/Types.h>
#include <IO/ReadBufferFromMemory.h>

#include <mutex>
#include <optional>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/Record.h>

namespace Poco { class Logger; }

namespace DB
{

using LoggerPtr = std::shared_ptr<Poco::Logger>;

struct ShardState
{
    Aws::Kinesis::Model::Shard shard;
    String iterator;
    String checkpoint;
    bool is_closed = false;
};

class KinesisConsumer
{
public:
    struct Message
    {
        String data;
        String partition_key;
        String sequence_number;
        String shard_id;
        UInt64 approximate_arrival_timestamp = 0;
        UInt64 received_at = 0;
    };

    enum StartingPositionType
    {
        LATEST,
        TRIM_HORIZON,
        AT_TIMESTAMP
    };
    
    KinesisConsumer(
        const String & stream_name_,
        const Aws::Kinesis::KinesisClient & client_,
        const std::map<String, ShardState> & shard_states_,
        size_t max_messages_per_batch_,
        StartingPositionType starting_position_ = LATEST,
        time_t timestamp_ = 0,
        const String & consumer_name_ = "",
        size_t internal_queue_size_ = 1000,
        bool is_enhanced_consumer_ = false,
        UInt64 max_execution_time_ms_ = 0);
    
    ~KinesisConsumer();

    bool receive();
    std::optional<Message> getMessage();
    
    void stop();
    
    bool isRunning() const { return is_running; }

    /// Update the list of shards assigned to this consumer
    /// This method is used for shard rebalancing
    void updateShardsState(
        std::map<String, ShardState> & new_shard_states);

    std::map<String, ShardState> getShardsState();

    bool commit();
    void rollback();

    String consumer_name; // For enhanced consumer and debug

    size_t getQueueSize() const { return queue.size(); }

private:
    String getShardIterator(const String & shard_id, StartingPositionType position_type, time_t timestamp_value = 0);
    
    void processRecords(const std::vector<Aws::Kinesis::Model::Record> & records, const String & shard_id);

    bool receiveSimple();
    bool receiveEnhanced();

    const String stream_name;
    String consumer_arn;
    const Aws::Kinesis::KinesisClient & client;
    size_t max_messages_per_batch;
    StartingPositionType starting_position_type;
    time_t timestamp;
    
    ConcurrentBoundedQueue<Message> queue;
    std::atomic<bool> is_running{true};
    std::atomic<bool> is_enhanced_recieving{false};
    
    std::map<String, ShardState> shard_states; // shard_id -> {shard, iterator, checkpoint}
    std::map<String, ShardState> to_commit;    // shard_id -> {shard, iterator, checkpoint}
    std::atomic<bool> waiting_commit{false};
    std::mutex shard_mutex;
    
    UInt64 total_messages_received{0};
    UInt64 last_receive_time{0};
    UInt64 max_execution_time_ms{0};
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

    bool is_enhanced_consumer = false;
};

using KinesisConsumerPtr = std::shared_ptr<KinesisConsumer>;

}

#endif // USE_AWS_KINESIS
