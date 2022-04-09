#include <Storages/Redis/ReadBufferFromRedisConsumer.h>

#include <base/logger_useful.h>

#include <cppkafka/cppkafka.h>
#include <boost/algorithm/string/join.hpp>
#include <fmt/ostream.h>
#include <algorithm>

namespace DB
{

using namespace std::chrono_literals;

ReadBufferFromRedisConsumer::ReadBufferFromRedisConsumer(
    RedisPtr redis_,
    std::string group_name_,
    std::string consumer_name_,
    Poco::Logger * log_,
    size_t max_batch_size,
    size_t poll_timeout_,
    bool intermediate_ack_,
    const Names & _streams)
    : ReadBuffer(nullptr, 0)
    , redis(redis_)
    , group_name(group_name_)
    , consumer_name(consumer_name_)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , intermediate_ack(intermediate_ack_)
    , current(messages.begin())
{
    for (const auto& stream: _streams)
    {
        streams_with_ids[stream] = ">";
    }
}

ReadBufferFromRedisConsumer::~ReadBufferFromRedisConsumer() = default;


void ReadBufferFromRedisConsumer::ack()
{
    for (auto& [stream_name, ids] : last_read_ids)
    {
        if (!ids.empty())
        {
            redis->xack(stream_name, group_name, ids.begin(), ids.end());
            ids.clear();
        }
    }
}

bool ReadBufferFromRedisConsumer::poll()
{
    if (hasMorePolledMessages())
    {
        allowed = true;
        return true;
    }

    stalled_status = NO_MESSAGES_RETURNED;

    StreamsOutput new_messages;
    redis->xreadgroup(group_name, consumer_name, streams_with_ids.begin(), streams_with_ids.end(),
                      std::chrono::milliseconds(poll_timeout), batch_size, std::inserter(new_messages, new_messages.end()));

    if (new_messages.empty())
    {
        LOG_TRACE(log, "Stalled");
        return false;
    }
    else
    {
        convertStreamsOutputToMessages(new_messages);
        LOG_TRACE(log, "Polled batch of {} messages.",
            messages.size());
    }

    stalled_status = NOT_STALLED;
    allowed = true;
    return true;
}

/// Do commit messages implicitly after we processed the previous batch.
bool ReadBufferFromRedisConsumer::nextImpl()
{
    if (!allowed || !hasMorePolledMessages())
        return false;

    const auto * message_data = current->attrs.data();
    size_t message_size = current->attrs.size();

    allowed = false;
    ++current;

    /// If message is empty, return end of stream.
    if (message_data == nullptr)
        return false;

    /// const_cast is needed, because ReadBuffer works with non-const char *.
    auto * new_position = const_cast<char *>(message_data);
    BufferBase::set(new_position, message_size, 0);
    return true;
}

void ReadBufferFromRedisConsumer::convertStreamsOutputToMessages(const StreamsOutput& output) {
    if (intermediate_ack)
        ack();
    messages.clear();
    for (const auto& [stream_name, msg_stream] : output)
    {
        for (const auto& [id, attrs] : msg_stream)
        {
            last_read_ids[stream_name].push_back(id);
            Message msg;
            msg.stream = stream_name;
            msg.key = id;
            std::vector<std::string> tmp;
            boost::split(tmp, id, boost::is_any_of("-"));
            msg.timestamp = std::stoull(tmp.front());
            msg.sequence_number = std::stoull(tmp.back());

            Poco::JSON::Object json;
            if (attrs.has_value()) {
                for (const auto& [key, value] : attrs.value()) {
                    json.set(key, value);
                }
            }
            std::stringstream stream;
            json.stringify(stream);
            msg.attrs = stream.str();
            messages.push_back(std::move(msg));
        }
    }
    current = messages.begin();
}

}
