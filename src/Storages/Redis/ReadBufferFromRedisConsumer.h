#pragma once

#include <Core/Names.h>
#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <Poco/JSON/Object.h>

#include <cppkafka/cppkafka.h>
#include <redis++/redis++.h>
#include <boost/algorithm/string.hpp>

namespace Poco
{
    class Logger;
}

namespace DB
{

using RedisPtr = std::shared_ptr<sw::redis::Redis>;

class ReadBufferFromRedisConsumer : public ReadBuffer
{
public:
    ReadBufferFromRedisConsumer(
        RedisPtr redis_,
        std::string group_name_,
        std::string consumer_name_,
        Poco::Logger * log_,
        size_t max_batch_size,
        size_t poll_timeout_,
        bool intermediate_ack_,
        const Names & _streams
    );
    ~ReadBufferFromRedisConsumer() override;
    void ack(); // Acknowledge all processed messages.

    auto pollTimeout() const { return poll_timeout; }

    inline bool hasMorePolledMessages() const
    {
        return (stalled_status == NOT_STALLED) && (current != messages.end());
    }

    inline bool isStalled() const
    {
        return stalled_status != NOT_STALLED;
    }

    // Polls batch of messages from Redis or allows to read consecutive message by nextImpl
    // returns true if there are some messages to process
    // return false and sets stalled to false if there are no messages to process.
    bool poll();

    struct Message {
        String stream;
        String key;
        uint64_t timestamp;
        uint64_t sequence_number;
        String attrs;
    };

    // Return values for the message that's being read.
    String currentTopic() const { return current[-1].stream; }
    String currentKey() const { return current[-1].key; }
    auto currentTimestamp() const { return current[-1].timestamp; }
    auto currentSequenceNumber() const { return current[-1].sequence_number; }
    String currentPayload() const { return current[-1].attrs; }

private:
    using Attrs = std::vector<std::pair<std::string, std::string>>;
    using Item = std::pair<std::string, sw::redis::Optional<Attrs>>;
    using ItemStream = std::vector<Item>;
    using StreamsOutput = std::vector<std::pair<std::string, ItemStream>>;
    using Messages = std::vector<Message>;

    enum StalledStatus
    {
        NOT_STALLED,
        NO_MESSAGES_RETURNED
    };

    RedisPtr redis;
    std::string group_name;
    std::string consumer_name;
    Poco::Logger * log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;

    StalledStatus stalled_status = NO_MESSAGES_RETURNED;
    bool intermediate_ack = true;
    bool allowed = true;

    Messages messages;
    Messages::const_iterator current;

    std::unordered_map<std::string, std::string> streams_with_ids;
    std::unordered_map<std::string, std::vector<std::string>> last_read_ids;

    void convertStreamsOutputToMessages(const StreamsOutput& output);

    bool nextImpl() override;
};

}
