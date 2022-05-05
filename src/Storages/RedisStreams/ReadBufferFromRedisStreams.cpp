#include <Storages/RedisStreams/ReadBufferFromRedisStreams.h>

#include <base/logger_useful.h>

#include <fmt/ostream.h>
#include <algorithm>

namespace DB
{

using namespace std::chrono_literals;

ReadBufferFromRedisStreams::ReadBufferFromRedisStreams(
    RedisPtr redis_,
    std::string group_name_,
    std::string consumer_name_,
    Poco::Logger * log_,
    size_t max_batch_size,
    size_t max_claim_size,
    size_t poll_timeout_,
    bool intermediate_ack_,
    const Names & _streams)
    : ReadBuffer(nullptr, 0)
    , redis(redis_)
    , group_name(group_name_)
    , consumer_name(consumer_name_)
    , log(log_)
    , batch_size(max_batch_size)
    , claim_batch_size(max_claim_size)
    , poll_timeout(poll_timeout_)
    , intermediate_ack(intermediate_ack_)
    , current(messages.begin())
{
    for (const auto & stream: _streams)
    {
        streams_with_ids[stream] = ">";
    }
}

ReadBufferFromRedisStreams::~ReadBufferFromRedisStreams() = default;


void ReadBufferFromRedisStreams::ack()
{
    for (auto & [stream_name, ids] : last_read_ids)
    {
        if (!ids.empty())
        {
            redis->xack(stream_name, group_name, ids.begin(), ids.end());
            ids.clear();
        }
    }
}

bool ReadBufferFromRedisStreams::poll()
{
    if (hasMorePolledMessages())
    {
        allowed = true;
        return true;
    }

    stalled_status = NO_MESSAGES_RETURNED;

    StreamsOutput new_messages;
    try
    {
        redis->xreadgroup(group_name, consumer_name, streams_with_ids.begin(), streams_with_ids.end(),
                          std::chrono::milliseconds(poll_timeout), batch_size, std::inserter(new_messages, new_messages.end()));
        std::unordered_map<std::string, std::vector<std::string>> pending_items_for_streams;
        std::vector<PendingItem> pending_items;
        size_t num_claimed = 0;
        for (const auto & [stream_name, id] : streams_with_ids)
        {
            redis->command("XPENDING", stream_name, group_name, "IDLE", min_pending_time_for_claim, "-", "+",
                           claim_batch_size - num_claimed, std::inserter(pending_items, pending_items.end()));
            for (const auto& item : pending_items)
            {
                pending_items_for_streams[stream_name].push_back(std::get<0>(item));
            }
            num_claimed += pending_items.size();
            pending_items.clear();
            if (num_claimed == claim_batch_size)
                break;
        }
        for (const auto& [stream, ids] : pending_items_for_streams)
        {
            ItemStream claimed_items;
            redis->xclaim(stream, group_name, consumer_name, std::chrono::milliseconds(min_pending_time_for_claim),
                          ids.begin(), ids.end(), std::inserter(claimed_items, claimed_items.end()));
            new_messages.emplace_back(stream, std::move(claimed_items));
        }
    }
    catch (const sw::redis::Error & e)
    {
        LOG_DEBUG(log, "Failed to poll messages from Redis in group {} consumer {}. Error message: {}", group_name, consumer_name, e.what());
    }

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
bool ReadBufferFromRedisStreams::nextImpl()
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

void ReadBufferFromRedisStreams::convertStreamsOutputToMessages(const StreamsOutput& output) {
    if (intermediate_ack)
        ack();
    messages.clear();
    for (const auto & [stream_name, msg_stream] : output)
    {
        for (const auto & [id, attrs] : msg_stream)
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
