#pragma once

#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <Storages/Kafka/IKafkaExceptionInfoSink.h>
#include <Storages/Kafka/StorageKafkaUtils.h>
#include <boost/circular_buffer.hpp>
#include <cppkafka/cppkafka.h>
#include <Common/CurrentMetrics.h>
#include <Common/DateLUT.h>

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

using ConsumerPtr = std::shared_ptr<cppkafka::Consumer>;
using LoggerPtr = std::shared_ptr<Poco::Logger>;

class KafkaConsumer : public IKafkaExceptionInfoSink
{
public:
    using Stat = StorageKafkaUtils::ConsumerStatistics;

    KafkaConsumer(
        LoggerPtr log_,
        size_t max_batch_size,
        size_t poll_timeout_,
        bool intermediate_commit_,
        const std::atomic<bool> & stopped_,
        const Names & _topics,
        size_t skip_bytes_ = 0
    );

    ~KafkaConsumer() override;

    void createConsumer(cppkafka::Configuration consumer_config);
    bool hasConsumer() const { return consumer.get() != nullptr; }
    ConsumerPtr && moveConsumer();

    void commit(); // Commit all processed messages.
    void subscribe(); // Subscribe internal consumer to topics.

    // used during exception processing to restart the consumption from last committed offset
    // Notes: duplicates can appear if the some data were already flushed
    // it causes rebalance (and is an expensive way of exception handling)
    void markDirty();

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
    void setExceptionInfo(const std::string & text, bool with_stacktrace) override;
    void setRDKafkaStat(const std::string & stat_json_string)
    {
        std::lock_guard<std::mutex> lock(rdkafka_stat_mutex);
        rdkafka_stat = stat_json_string;
    }
    void inUse() { in_use = true; }
    void notInUse()
    {
        in_use = false;
        last_used_usec = timeInMicroseconds(std::chrono::system_clock::now());
    }

    // For system.kafka_consumers
    Stat getStat() const;

    bool isInUse() const { return in_use; }
    UInt64 getLastUsedUsec() const { return last_used_usec; }

    std::string getMemberId() const;

private:
    using Messages = std::vector<cppkafka::Message>;
    using ExceptionsBuffer = StorageKafkaUtils::ConsumerStatistics::ExceptionsBuffer;
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
    const size_t skip_bytes = 0;
    size_t offsets_stored = 0;
    bool current_subscription_valid = false;

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

    std::atomic<UInt64> last_poll_timestamp = 0;
    std::atomic<UInt64> num_messages_read = 0;
    std::atomic<UInt64> last_commit_timestamp = 0;
    std::atomic<UInt64> num_commits = 0;
    std::atomic<UInt64> last_rebalance_timestamp = 0;
    std::atomic<UInt64> num_rebalance_assignments = 0;
    std::atomic<UInt64> num_rebalance_revocations = 0;
    std::atomic<bool> in_use = false;
    /// Last used time (for TTL)
    std::atomic<UInt64> last_used_usec = 0;

    void doPoll();
    void cleanUnprocessed();
    void cleanAssignment();
    void resetIfStopped();
    ReadBufferPtr getNextMessage();
};

}
