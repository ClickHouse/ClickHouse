#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;


#define REDIS_RELATED_SETTINGS(M) \
    M(String, redis_broker_list, "", "A comma-separated list of brokers for Redis engine.", 0) \
    M(String, redis_stream_list, "", "A list of Redis topics.", 0) \
    M(String, redis_group_name, "", "Client group id string. All Redis consumers sharing the same group.id belong to the same group.", 0) \
    M(String, redis_client_id, "", "Client identifier.", 0) \
    M(UInt64, redis_num_consumers, 1, "The number of consumers per table for Redis engine.", 0) \
    M(Bool, redis_commit_every_batch, false, "Commit every consumed and handled batch instead of a single commit after writing a whole block", 0) \
    /* default is stream_poll_timeout_ms */ \
    M(Milliseconds, redis_poll_timeout_ms, 0, "Timeout for single poll from Redis.", 0) \
    /* default is min(max_block_size, redis_max_block_size)*/ \
    M(UInt64, redis_poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single Redis poll.", 0) \
    /* default is = max_insert_block_size / redis_num_consumers  */ \
    M(UInt64, redis_max_block_size, 0, "Number of row collected by poll(s) for flushing data from Redis.", 0) \
    /* default is stream_flush_interval_ms */ \
    M(Milliseconds, redis_flush_interval_ms, 0, "Timeout for flushing data from Redis.", 0) \
    M(Bool, redis_thread_per_consumer, false, "Provide independent thread for each consumer", 0) \

#define LIST_OF_REDIS_SETTINGS(M) \
    REDIS_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(RedisSettingsTraits, LIST_OF_REDIS_SETTINGS)


/** Settings for the Redis engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct RedisSettings : public BaseSettings<RedisSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
