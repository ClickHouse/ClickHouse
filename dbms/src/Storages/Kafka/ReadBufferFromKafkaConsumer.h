#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>

#include <cppkafka/cppkafka.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

using ConsumerPtr = std::shared_ptr<cppkafka::Consumer>;

class ReadBufferFromKafkaConsumer : public ReadBuffer
{
public:
    ReadBufferFromKafkaConsumer(ConsumerPtr consumer_, Poco::Logger * log_, size_t max_batch_size, size_t poll_timeout_);
    ~ReadBufferFromKafkaConsumer() override;

    void commit(); // Commit all processed messages.
    void subscribe(const Names & topics); // Subscribe internal consumer to topics.
    void unsubscribe(); // Unsubscribe internal consumer in case of failure.

    // Return values for the message that's being read.
    String currentTopic() const { return current->get_topic(); }
    String currentKey() const { return current->get_key(); }
    auto currentOffset() const { return current->get_offset(); }
    auto currentPartition() const { return current->get_partition(); }
    auto currentTimestamp() const { return current->get_timestamp(); }

    bool reset() override;

private:
    using Messages = std::vector<cppkafka::Message>;

    ConsumerPtr consumer;
    Poco::Logger * log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;

    Messages messages;
    Messages::const_iterator current;

    bool nextImpl() override;
    bool poll(size_t timeout);
};

}
