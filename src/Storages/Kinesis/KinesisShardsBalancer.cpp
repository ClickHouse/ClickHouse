#include <Storages/Kinesis/StorageKinesis.h>
#include <Storages/Kinesis/KinesisShardsBalancer.h>
#include <Storages/Kinesis/KinesisConsumer.h>

#include <Common/logger_useful.h>
#include <Common/Exception.h>

#if USE_AWS_KINESIS

namespace DB
{

namespace ErrorCodes
{
    extern const int KINESIS_ERROR;
    extern const int LOGICAL_ERROR;
}

KinesisShardsBalancer::KinesisShardsBalancer(
    StorageKinesis & storage_,
    const String & stream_name_,
    Aws::Kinesis::KinesisClient & client_)
    : storage(storage_)
    , stream_name(stream_name_)
    , client(client_)
{
}

bool KinesisShardsBalancer::isStreamChanged()
{
    std::vector<Aws::Kinesis::Model::Shard> current_shards = getActualShards();

    if (current_shards.size() != shards.size())
    {
        shards = current_shards;
        return true;
    }

    // Compare shards by ID to detect changes
    std::set<String> current_shard_ids;
    for (const auto & shard : current_shards)
        current_shard_ids.insert(shard.GetShardId());

    std::set<String> existing_shard_ids;
    for (const auto & shard : shards)
        existing_shard_ids.insert(shard.GetShardId());

    if (current_shard_ids != existing_shard_ids)
    {
        shards = current_shards;
        return true;
    }

    return false;
}

std::vector<Aws::Kinesis::Model::Shard> KinesisShardsBalancer::getActualShards()
{
    Aws::Kinesis::Model::DescribeStreamRequest describe_stream_request;
    describe_stream_request.SetStreamName(stream_name);

    auto describe_outcome = client.DescribeStream(describe_stream_request);

    if (!describe_outcome.IsSuccess())
    {
        const auto & error = describe_outcome.GetError();
        throw Exception(
            ErrorCodes::KINESIS_ERROR,
            "Error describing Kinesis stream: {}, error: {}", 
            stream_name, error.GetMessage());
    }

    const auto & stream_description = describe_outcome.GetResult().GetStreamDescription();
    return stream_description.GetShards();
}

void KinesisShardsBalancer::balanceShards(bool load_from_storage)
{
    size_t num_consumers = storage.getNumConsumers();
    if (num_consumers == 0)
        return;
    
    // 1. Collect all consumers and their shard states
    std::vector<KinesisConsumerPtr> consumers;
    std::map<String, DB::ShardState> all_shard_states; // shard_id -> state
    
    if (load_from_storage)
        all_shard_states = storage.loadCheckpoints();

    for (size_t i = 0; i < num_consumers; ++i)
    {
        KinesisConsumerPtr consumer = storage.popConsumer();
        if (!consumer)
            continue;
            
        consumers.push_back(consumer);
        
        if (!load_from_storage)
        {
            auto consumer_shard_states = consumer->getShardsState();
            for (const auto & [shard_id, state] : consumer_shard_states)
                all_shard_states[shard_id] = state;
        }
    }
    
    if (consumers.empty())
        return;
    
    // Sync shards from Kinesis API with local states
    for (const auto & api_shard : shards)
    {
        String shard_id = api_shard.GetShardId();
        
        // If shard exists in API, but not in local state, add it
        if (all_shard_states.find(shard_id) == all_shard_states.end())
        {            
            DB::ShardState new_state;
            new_state.shard = api_shard;
            new_state.is_closed = false;
            all_shard_states[shard_id] = new_state;
        }
        else
        {
            // Update shard object from API
            all_shard_states[shard_id].shard = api_shard;
        }
    }
    
    // 2. Filter out closed shards
    std::vector<String> active_shard_ids;
    
    for (const auto & [shard_id, state] : all_shard_states)
    {
        if (state.is_closed)
            continue;
        
        active_shard_ids.push_back(shard_id);
    }
    
    // 3. Balance active shards among consumers
    if (active_shard_ids.empty())
    {
        for (auto & consumer : consumers)
            storage.pushConsumer(consumer);
            
        return;
    }
    
    const size_t shards_per_consumer = active_shard_ids.size() / consumers.size();
    const size_t extra_shards = active_shard_ids.size() % consumers.size();
    
    std::vector<std::map<String, DB::ShardState>> consumer_shard_states(consumers.size());
    
    size_t shard_idx = 0;
    for (size_t consumer_idx = 0; consumer_idx < consumers.size(); ++consumer_idx)
    {
        size_t consumer_shard_count = shards_per_consumer + (consumer_idx < extra_shards ? 1 : 0);
        
        for (size_t i = 0; i < consumer_shard_count && shard_idx < active_shard_ids.size(); ++i, ++shard_idx)
        {
            const String & shard_id = active_shard_ids[shard_idx];
            consumer_shard_states[consumer_idx][shard_id] = all_shard_states[shard_id];
        }
    }
    
    // 4. Update consumers with new shard assignments and return them to storage
    for (size_t consumer_idx = 0; consumer_idx < consumers.size(); ++consumer_idx)
    {
        KinesisConsumerPtr & consumer = consumers[consumer_idx];
        
        consumer->updateShardsState(consumer_shard_states[consumer_idx]);
        storage.pushConsumer(consumer);
    }
}

}

#endif // USE_AWS_KINESIS
