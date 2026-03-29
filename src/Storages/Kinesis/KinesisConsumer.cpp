#include <Storages/Kinesis/KinesisConsumer.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/KinesisErrors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_KINESIS;
}

KinesisConsumer::KinesisConsumer(
    const String & stream_name_,
    std::shared_ptr<Aws::Kinesis::KinesisClient> client_,
    std::map<String, KinesisShardState> shard_states_,
    size_t max_records_per_request_,
    Aws::Kinesis::Model::ShardIteratorType starting_position_type_,
    UInt64 at_timestamp_,
    size_t internal_queue_size_)
    : stream_name(stream_name_)
    , client(std::move(client_))
    , shard_states(std::move(shard_states_))
    , max_records_per_request(max_records_per_request_)
    , starting_position_type(starting_position_type_)
    , at_timestamp(at_timestamp_)
    , queue(internal_queue_size_)
    , log(getLogger("KinesisConsumer"))
{
}

KinesisConsumer::~KinesisConsumer()
{
    is_running = false;
    queue.clear();
}

void KinesisConsumer::stop()
{
    is_running = false;
}

String KinesisConsumer::getOrRefreshIterator(const String & shard_id, KinesisShardState & state)
{
    if (!state.iterator.empty())
        return state.iterator;

    Aws::Kinesis::Model::GetShardIteratorRequest request;
    request.SetStreamName(stream_name);
    request.SetShardId(shard_id);

    if (!state.last_sequence.empty())
    {
        request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::AFTER_SEQUENCE_NUMBER);
        request.SetStartingSequenceNumber(state.last_sequence);
    }
    else
    {
        request.SetShardIteratorType(starting_position_type);
        if (starting_position_type == Aws::Kinesis::Model::ShardIteratorType::AT_TIMESTAMP && at_timestamp > 0)
        {
            Aws::Utils::DateTime ts(static_cast<int64_t>(at_timestamp * 1000));
            request.SetTimestamp(ts);
        }
    }

    auto outcome = client->GetShardIterator(request);
    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_KINESIS,
            "Failed to get shard iterator for stream {} shard {}: {} ({})",
            stream_name, shard_id,
            error.GetMessage(), error.GetExceptionName());
    }

    state.iterator = outcome.GetResult().GetShardIterator();
    return state.iterator;
}

bool KinesisConsumer::receive()
{
    if (!is_running)
        return false;

    std::lock_guard lock(shard_mutex);

    bool got_any = false;

    for (auto & [shard_id, state] : shard_states)
    {
        if (state.is_closed)
            continue;

        String iterator;
        try
        {
            iterator = getOrRefreshIterator(shard_id, state);
        }
        catch (const Exception & e)
        {
            LOG_WARNING(log, "Could not get shard iterator for shard {}: {}", shard_id, e.message());
            continue;
        }

        if (iterator.empty())
            continue;

        Aws::Kinesis::Model::GetRecordsRequest request;
        request.SetShardIterator(iterator);
        request.SetLimit(static_cast<int>(max_records_per_request));

        auto outcome = client->GetRecords(request);

        if (!outcome.IsSuccess())
        {
            const auto & error = outcome.GetError();
            const auto error_type = error.GetErrorType();

            if (error_type == Aws::Kinesis::KinesisErrors::EXPIRED_ITERATOR
                || error_type == Aws::Kinesis::KinesisErrors::EXPIRED_NEXT_TOKEN)
            {
                LOG_WARNING(log, "Shard iterator for shard {} expired, will refresh", shard_id);
                state.iterator.clear();
                continue;
            }

            LOG_WARNING(log, "GetRecords failed for shard {}: {} ({})",
                shard_id, error.GetMessage(), error.GetExceptionName());
            continue;
        }

        const auto & result = outcome.GetResult();
        const auto & records = result.GetRecords();
        const auto & next_iterator = result.GetNextShardIterator();

        state.iterator = next_iterator;
        if (next_iterator.empty())
        {
            state.is_closed = true;
            LOG_DEBUG(log, "Shard {} has been closed", shard_id);
        }

        for (const auto & record : records)
        {
            const auto & data = record.GetData();

            Message msg;
            msg.data = String(reinterpret_cast<const char *>(data.GetUnderlyingData()), data.GetLength());
            msg.sequence_number = record.GetSequenceNumber();
            msg.partition_key = record.GetPartitionKey();
            msg.shard_id = shard_id;
            msg.approximate_arrival_timestamp =
                static_cast<UInt64>(record.GetApproximateArrivalTimestamp().Millis()) / 1000;

            if (queue.tryPush(msg))
            {
                state.last_sequence = record.GetSequenceNumber();
                got_any = true;
            }
            else
            {
                LOG_WARNING(log, "Internal queue is full, dropping record from shard {}", shard_id);
            }
        }
    }

    return got_any;
}

std::optional<KinesisConsumer::Message> KinesisConsumer::getMessage()
{
    if (!is_running)
        return std::nullopt;

    Message msg;
    if (!queue.tryPop(msg))
        return std::nullopt;

    return msg;
}

std::map<String, KinesisShardState> KinesisConsumer::getShardStates() const
{
    std::lock_guard lock(shard_mutex);
    return shard_states;
}

}

#endif // USE_AWS_KINESIS