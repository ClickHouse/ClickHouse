#pragma once

#include <boost/circular_buffer.hpp>

#include <Core/Names.h>
#include <base/types.h>
#include <IO/ReadBuffer.h>

#include <cppkafka/cppkafka.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric KafkaConsumers;
}

namespace Poco
{
    class Logger;
}

namespace DB
{

class StorageSystemKafkaConsumers;

using ConsumerPtr = std::shared_ptr<cppkafka::Consumer>;

class KafkaConsumer
{
public:
    struct ExceptionInfo
    {
        String text;
        UInt64 timestamp_usec;
    };
    using ExceptionsBuffer = boost::circular_buffer<ExceptionInfo>;

    struct Stat // system.kafka_consumers data
    {
        struct Assignment
        {
            String topic_str;
            Int32 partition_id;
            Int64 current_offset;
        };
        using Assignments = std::vector<Assignment>;

        String consumer_id;
        Assignments assignments;
        UInt64 last_poll_time;
        UInt64 num_messages_read;
        UInt64 last_commit_timestamp_usec;
        UInt64 last_rebalance_timestamp_usec;
        UInt64 num_commits;
        UInt64 num_rebalance_assignments;
        UInt64 num_rebalance_revocations;
        KafkaConsumer::ExceptionsBuffer exceptions_buffer;
        bool in_use;
        UInt64 last_used_usec;
        std::string rdkafka_stat;
    };

    KafkaConsumer(
        LoggerPtr log_,
        size_t max_batch_size,
        size_t poll_timeout_,
        bool intermediate_commit_,
        const std::atomic<bool> & stopped_,
        const Names & _topics
    );

    ~KafkaConsumer();

    void createConsumer(cppkafka::Configuration consumer_config);
    bool hasConsumer() const { return consumer.get() != nullptr; }
    ConsumerPtr && moveConsumer();

    void commit(); // Commit all processed messages.
    void subscribe(); // Subscribe internal consumer to topics.
    void unsubscribe(); // Unsubscribe internal consumer in case of failure.

    auto pollTimeout() const { return poll_timeout; }

    bool hasMorePolledMessages() const
    {
        return (stalled_status == NOT_STALLED) && (current != messages.end());
    }

    bool polledDataUnusable() const
    {
        return  (stalled_status != NOT_STALLED) && (stalled_status != NO_MESSAGES_RETURNED);
    }

    bool isStalled() const { return stalled_status != NOT_STALLED; }

    void storeLastReadMessageOffset();
    void resetToLastCommitted(const char * msg);

    /// Polls batch of messages from Kafka and returns read buffer containing the next message or
    /// nullptr when there are no messages to process.
    ReadBufferPtr consume();

    // Return values for the message that's being read.
    String currentTopic() const { return current[-1].get_topic(); }
    String currentKey() const { return current[-1].get_key(); }
    auto currentOffset() const { return current[-1].get_offset(); }
    auto currentPartition() const { return current[-1].get_partition(); }
    auto currentTimestamp() const { return current[-1].get_timestamp(); }
    const auto & currentHeaderList() const { return current[-1].get_header_list(); }
    const cppkafka::Buffer & currentPayload() const { return current[-1].get_payload(); }
    void setExceptionInfo(const cppkafka::Error & err, bool with_stacktrace = true);
    void setExceptionInfo(const std::string & text, bool with_stacktrace = true);
    void setRDKafkaStat(const std::string & stat_json_string)
    {
        std::lock_guard<std::mutex> lock(rdkafka_stat_mutex);
        rdkafka_stat = stat_json_string;
    }
    void inUse() { in_use = true; }
    void notInUse()
    {
        in_use = false;
        last_used_usec = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }

    // For system.kafka_consumers
    Stat getStat() const;

    bool isInUse() const { return in_use; }
    UInt64 getLastUsedUsec() const { return last_used_usec; }

    std::string getMemberId() const;

private:
    using Messages = std::vector<cppkafka::Message>;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaConsumers};

    enum StalledStatus
    {
        NOT_STALLED,
        NO_MESSAGES_RETURNED,
        REBALANCE_HAPPENED,
        CONSUMER_STOPPED,
        NO_ASSIGNMENT,
        ERRORS_RETURNED
    };

    // order is important, need to be destructed *after* consumer
    mutable std::mutex rdkafka_stat_mutex;
    std::string rdkafka_stat;

    ConsumerPtr consumer;
    LoggerPtr log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;
    size_t offsets_stored = 0;

    StalledStatus stalled_status = NO_MESSAGES_RETURNED;

    bool intermediate_commit = true;
    size_t waited_for_assignment = 0;

    const std::atomic<bool> & stopped;

    // order is important, need to be destructed *before* consumer
    Messages messages;
    Messages::const_iterator current;

    // order is important, need to be destructed *before* consumer
    std::optional<cppkafka::TopicPartitionList> assignment;
    const Names topics;

    /// system.kafka_consumers data is retrieved asynchronously
    ///  so we have to protect exceptions_buffer
    mutable std::mutex exception_mutex;
    const size_t EXCEPTIONS_DEPTH = 10;
    ExceptionsBuffer exceptions_buffer;

    std::atomic<UInt64> last_exception_timestamp_usec = 0;
    std::atomic<UInt64> last_poll_timestamp_usec = 0;
    std::atomic<UInt64> num_messages_read = 0;
    std::atomic<UInt64> last_commit_timestamp_usec = 0;
    std::atomic<UInt64> num_commits = 0;
    std::atomic<UInt64> last_rebalance_timestamp_usec = 0;
    std::atomic<UInt64> num_rebalance_assignments = 0;
    std::atomic<UInt64> num_rebalance_revocations = 0;
    std::atomic<bool> in_use = false;
    /// Last used time (for TTL)
    std::atomic<UInt64> last_used_usec = 0;

    void drain();
    void cleanUnprocessed();
    void resetIfStopped();
    void filterMessageErrors();
    ReadBufferPtr getNextMessage();
};

}
