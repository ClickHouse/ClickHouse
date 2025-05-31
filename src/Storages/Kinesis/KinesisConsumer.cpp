#include <Storages/Kinesis/KinesisConsumer.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#if USE_AWS_KINESIS

#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/kinesis/model/RegisterStreamConsumerRequest.h>
#include <aws/kinesis/model/SubscribeToShardRequest.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int KINESIS_ERROR;
    extern const int KINESIS_RESHARDING;
    extern const int TIMEOUT_EXCEEDED;
}

KinesisConsumer::KinesisConsumer(
    const String & stream_name_,
    const Aws::Kinesis::KinesisClient & client_,
    const std::map<String, ShardState> & shard_states_,
    size_t max_messages_per_batch_,
    StartingPositionType starting_position_type_,
    time_t timestamp_,
    const String & consumer_name_,
    size_t internal_queue_size_,
    bool is_enhanced_consumer_,
    UInt64 max_execution_time_ms_)
    : consumer_name(consumer_name_)
    , stream_name(stream_name_)
    , client(client_)
    , max_messages_per_batch(max_messages_per_batch_)
    , starting_position_type(starting_position_type_)
    , timestamp(timestamp_)
    , queue(internal_queue_size_)
    , shard_states(shard_states_)
    , max_execution_time_ms(max_execution_time_ms_)
    , is_enhanced_consumer(is_enhanced_consumer_)
{

    if (is_enhanced_consumer)
    {
        Aws::Kinesis::Model::RegisterStreamConsumerRequest request;
        String stream_arn_value;
        try
        {
            Aws::Kinesis::Model::DescribeStreamRequest describe_request;
            describe_request.SetStreamName(stream_name);
            auto describe_outcome = client.DescribeStream(describe_request);

            if (!describe_outcome.IsSuccess())
            {
                const auto & error = describe_outcome.GetError();
                throw Exception(ErrorCodes::KINESIS_ERROR, "Failed to describe stream {}: {} ({})", stream_name, error.GetMessage(), error.GetExceptionName());
            }
            stream_arn_value = describe_outcome.GetResult().GetStreamDescription().GetStreamARN();
        }
        catch (const DB::Exception &)
        {
            throw;
        }
        catch (const std::exception & e)
        {
            throw Exception(ErrorCodes::KINESIS_ERROR, "Exception while describing stream {}: {}", stream_name, e.what());
        }
        
        request.SetStreamARN(stream_arn_value);
        request.SetConsumerName(consumer_name);
        
        auto outcome = client.RegisterStreamConsumer(request);
        if (!outcome.IsSuccess())
        {
            const auto & error = outcome.GetError();
            throw Exception(ErrorCodes::KINESIS_ERROR, "Failed to register consumer: {} ({})", error.GetMessage(), error.GetExceptionName());
        }
        consumer_arn = outcome.GetResult().GetConsumer().GetConsumerARN();
    }
}

String KinesisConsumer::getShardIterator(const String & shard_id, StartingPositionType position_type, time_t timestamp_value)
{
    Aws::Kinesis::Model::GetShardIteratorRequest request;
    request.SetStreamName(stream_name);
    request.SetShardId(shard_id);
    
    switch (position_type)
    {
        case LATEST:
            request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::LATEST);
            break;
        case TRIM_HORIZON:
            request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
            break;
        case AT_TIMESTAMP:
            request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::AT_TIMESTAMP);
            
            Aws::Utils::DateTime aws_timestamp(static_cast<int64_t>(timestamp_value));
            request.SetTimestamp(aws_timestamp);
            break;
    }
    
    if (shard_states.find(shard_id) != shard_states.end() && !shard_states[shard_id].checkpoint.empty())
    {
        request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::AFTER_SEQUENCE_NUMBER);
        request.SetStartingSequenceNumber(shard_states[shard_id].checkpoint);
    }
    
    auto outcome = client.GetShardIterator(request);
    
    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        throw Exception(
            ErrorCodes::KINESIS_ERROR,
            "Failed to get shard iterator for stream {} shard {}: {} ({})",
            stream_name, shard_id, error.GetMessage(), error.GetExceptionName());
    }

    return outcome.GetResult().GetShardIterator();
}

bool KinesisConsumer::receive()
{
    if (is_enhanced_consumer)
        return receiveEnhanced();
    else
        return receiveSimple();
}


bool KinesisConsumer::receiveEnhanced()
{
    if (!is_running.load() || waiting_commit.load() || shard_states.empty())
        return false;

    bool received_any_records = false;

    for (const auto & [shard_id, state] : shard_states)
    {
        if (!is_running.load())
            break;
        if (state.is_closed)
            continue;

        Aws::Kinesis::Model::SubscribeToShardRequest request;
        request.SetConsumerARN(consumer_arn);
        request.SetShardId(shard_id);

        Aws::Kinesis::Model::StartingPosition pos;
        if (!state.checkpoint.empty())
        {
            pos.SetType(Aws::Kinesis::Model::ShardIteratorType::AFTER_SEQUENCE_NUMBER);
            pos.SetSequenceNumber(state.checkpoint);
        }
        else
        {
            switch (starting_position_type)
            {
                case LATEST:       
                    pos.SetType(Aws::Kinesis::Model::ShardIteratorType::LATEST);
                    break;
                case TRIM_HORIZON:
                    pos.SetType(Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
                    break;
                case AT_TIMESTAMP:
                    pos.SetType(Aws::Kinesis::Model::ShardIteratorType::AT_TIMESTAMP);
                    pos.SetTimestamp(Aws::Utils::DateTime(static_cast<int64_t>(timestamp * 1000))); // ms
                    break;
            }
        }
        request.SetStartingPosition(pos);
        Aws::Kinesis::Model::SubscribeToShardHandler handler;

        handler.SetSubscribeToShardEventCallback([this, shard_id_cb = shard_id, &received_any_records](const Aws::Kinesis::Model::SubscribeToShardEvent &event)
        {
            bool is_time_limit_exceeded = false;
            if (max_execution_time_ms)
            {
                uint64_t time_for_one_shard = max_execution_time_ms / shard_states.size();
                uint64_t elapsed_time_ms = total_stopwatch.elapsedMilliseconds();
                is_time_limit_exceeded = time_for_one_shard <= elapsed_time_ms;
            }
            
            if (!event.GetRecords().empty())
            {
                this->processRecords(event.GetRecords(), shard_id_cb);
                received_any_records = true;
            }

            if (!is_running)
                throw DB::Exception(
                    ErrorCodes::TIMEOUT_EXCEEDED,
                    "Consumer is stopped, stopped on shard {}", shard_id_cb);

            if (is_time_limit_exceeded)
                throw DB::Exception(
                    ErrorCodes::TIMEOUT_EXCEEDED,
                    "Time limit for EFO shard {} exceeded, subscription actively stopped", shard_id_cb);
        });

        request.SetEventStreamHandler(handler);
        
        try
        {
            total_stopwatch.restart();
            client.SubscribeToShard(request);
        }
        catch (const DB::Exception & e)
        {
            LOG_DEBUG(&Poco::Logger::get("KinesisConsumer"), "EFO DB::Exception during SubscribeToShard for shard {}, error: {}", shard_id, e.what());
        }
        catch (const Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>& e)
        {
            LOG_DEBUG(&Poco::Logger::get("KinesisConsumer"), "EFO AWSError during SubscribeToShard for shard {}, error: {}", shard_id, e.GetExceptionName());
        }
        catch (const std::exception& e)
        {
            LOG_DEBUG(&Poco::Logger::get("KinesisConsumer"), "EFO std::exception during SubscribeToShard for shard {}, error: {}", shard_id, e.what());
        }
    }

    if (received_any_records)
        waiting_commit = true;

    return received_any_records;
}

bool KinesisConsumer::receiveSimple()
{
    if (!is_running || waiting_commit)
        return false;

    std::map<String, ShardState> states;
    {
        std::lock_guard lock(shard_mutex);
        states = shard_states;
    }
    
    bool received_any_records = false;
    
    for (auto & [shard_id, state] : states)
    {
        if (!is_running)
            break;
        if (state.is_closed)
            continue;

        if (state.iterator.empty())
        {
            try
            {
                std::lock_guard lock(shard_mutex);
                state.iterator = getShardIterator(shard_id, starting_position_type, timestamp);
            }
            catch (const std::exception & e)
            {
                LOG_WARNING(&Poco::Logger::get("KinesisConsumer"), "Failed to get iterator for shard {}: {}", shard_id, e.what());
                continue;
            }
        }
        
        Aws::Kinesis::Model::GetRecordsRequest request;
        request.SetShardIterator(state.iterator);
        request.SetLimit(static_cast<int>(max_messages_per_batch));

        try
        {
            auto outcome = client.GetRecords(request);
            
            if (!outcome.IsSuccess())
            {
                const auto & error = outcome.GetError();
                LOG_WARNING(&Poco::Logger::get("KinesisConsumer"),
                    "Shard {}: GetRecords failed. Error: {} ({}), Iterator used: {}",
                    shard_id, error.GetMessage(), error.GetExceptionName(), state.iterator);
                
                if (error.GetErrorType() == Aws::Kinesis::KinesisErrors::EXPIRED_ITERATOR 
                    || error.GetErrorType() == Aws::Kinesis::KinesisErrors::RESOURCE_NOT_FOUND)
                {
                    std::lock_guard lock(shard_mutex);
                    state.iterator = "";
                }
                continue;
            }
            
            const auto & result = outcome.GetResult();
            const auto & records = result.GetRecords();
            
            {
                std::lock_guard lock(shard_mutex);
                
                to_commit[shard_id] = state;
                to_commit[shard_id].iterator = result.GetNextShardIterator();
                to_commit[shard_id].is_closed = result.GetNextShardIterator().empty();
            }
            
            if (records.empty())
                continue;
            
            processRecords(records, shard_id);
            received_any_records = true;
        }
        catch (const std::exception & e)
        {
            LOG_WARNING(&Poco::Logger::get("KinesisConsumer"), "Exception when receiving from shard {}: {}", shard_id, e.what());
        }
    }

    if (received_any_records)
        waiting_commit = true;
    
    UInt64 now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    last_receive_time = now;
    
    return received_any_records;
}

void KinesisConsumer::processRecords(const std::vector<Aws::Kinesis::Model::Record> & records, const String & shard_id)
{
    UInt64 now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    for (const auto & record : records)
    {
        Message msg;
        
        const Aws::Utils::ByteBuffer& data = record.GetData();
        msg.data = String(reinterpret_cast<const char*>(data.GetUnderlyingData()), data.GetLength());
        msg.partition_key = record.GetPartitionKey();
        msg.sequence_number = record.GetSequenceNumber();
        msg.shard_id = shard_id;
        msg.approximate_arrival_timestamp = static_cast<UInt64>(record.GetApproximateArrivalTimestamp().SecondsWithMSPrecision());
        msg.received_at = now;

        if (!queue.tryPush(msg))
        {
            LOG_WARNING(&Poco::Logger::get("KinesisConsumer"), "Message queue is full, dropping Kinesis record for shard {}", shard_id);
            return;
        }
        
        if (!msg.sequence_number.empty())
        {
            std::lock_guard lock(shard_mutex);
            to_commit[shard_id].checkpoint = msg.sequence_number;
        }
        
        total_messages_received++;
    }
}

bool KinesisConsumer::commit()
{
    std::lock_guard lock(shard_mutex);
    if (!waiting_commit.exchange(false))
        return false;

    if (to_commit.empty())
        return false;

    for (auto & [shard_id, state] : to_commit)
        shard_states[shard_id] = state;
    to_commit.clear();

    return true;
}

void KinesisConsumer::rollback()
{
    std::lock_guard lock(shard_mutex);
    waiting_commit.store(false);
    to_commit.clear();
}

std::optional<KinesisConsumer::Message> KinesisConsumer::getMessage()
{
    if (!is_running)
        return std::nullopt;

    Message message;
    bool success = queue.tryPop(message);
    
    if (!success)
        return std::nullopt;
    
    return message;
}

void KinesisConsumer::updateShardsState(std::map<String, ShardState> & new_shard_states)
{
    std::lock_guard lock(shard_mutex);
    shard_states = new_shard_states;
}

std::map<String, ShardState> KinesisConsumer::getShardsState()
{
    std::lock_guard lock(shard_mutex);
    return shard_states;
}

void KinesisConsumer::stop()
{
    is_running.store(false);
    queue.clear();
}

KinesisConsumer::~KinesisConsumer()
{
    stop();
}

}

#endif // USE_AWS_KINESIS
