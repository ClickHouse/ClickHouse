#pragma once

#include <Core/BaseSettings.h>
#include <Core/FormatFactorySettingsDeclaration.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsObsoleteMacros.h>


namespace DB
{
class ASTStorage;

const auto KAFKA_RESCHEDULE_MS = 500;
const auto KAFKA_CLEANUP_TIMEOUT_MS = 3000;
// once per minute leave do reschedule (we can't lock threads in pool forever)
const auto KAFKA_MAX_THREAD_WORK_DURATION_MS = 60000;
// 10min
const auto KAFKA_CONSUMERS_POOL_TTL_MS_MAX = 600'000;

#define KAFKA_RELATED_SETTINGS(M, ALIAS) \
    M(String, kafka_broker_list, "", "A comma-separated list of brokers for Kafka engine.", 0) \
    M(String, kafka_topic_list, "", "A list of Kafka topics.", 0) \
    M(String, kafka_group_name, "", "Client group id string. All Kafka consumers sharing the same group.id belong to the same group.", 0) \
    /* those are mapped to format factory settings */ \
    M(String, kafka_format, "", "The message format for Kafka engine.", 0) \
    M(String, kafka_schema, "", "Schema identifier (used by schema-based formats) for Kafka engine", 0) \
    M(UInt64, kafka_num_consumers, 1, "The number of consumers per table for Kafka engine.", 0) \
    /* default is = max_insert_block_size / kafka_num_consumers  */ \
    M(UInt64, kafka_max_block_size, 0, "Number of row collected by poll(s) for flushing data from Kafka.", 0) \
    M(UInt64, kafka_skip_broken_messages, 0, "Skip at least this number of broken messages from Kafka topic per block", 0) \
    M(Bool, kafka_commit_every_batch, false, "Commit every consumed and handled batch instead of a single commit after writing a whole block", 0) \
    M(String, kafka_client_id, "", "Client identifier.", 0) \
    /* default is stream_poll_timeout_ms */ \
    M(Milliseconds, kafka_poll_timeout_ms, 0, "Timeout for single poll from Kafka.", 0) \
    M(UInt64, kafka_poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single Kafka poll.", 0) \
    M(UInt64, kafka_consumers_pool_ttl_ms, 60'000, "TTL for Kafka consumers (in milliseconds)", 0) \
    /* default is stream_flush_interval_ms */ \
    M(Milliseconds, kafka_flush_interval_ms, 0, "Timeout for flushing data from Kafka.", 0) \
    M(Bool, kafka_thread_per_consumer, false, "Provide independent thread for each consumer", 0) \
    M(StreamingHandleErrorMode, kafka_handle_error_mode, StreamingHandleErrorMode::DEFAULT, "How to handle errors for Kafka engine. Possible values: default (throw an exception after rabbitmq_skip_broken_messages broken messages), stream (save broken messages and errors in virtual columns _raw_message, _error).", 0) \
    M(Bool, kafka_commit_on_select, false, "Commit messages when select query is made", 0) \
    M(UInt64, kafka_max_rows_per_message, 1, "The maximum number of rows produced in one kafka message for row-based formats.", 0) \
    M(String, kafka_keeper_path, "", "The path to the table in ClickHouse Keeper", 0) \
    M(String, kafka_replica_name, "", "The replica name in ClickHouse Keeper", 0) \

#define OBSOLETE_KAFKA_SETTINGS(M, ALIAS) \
    MAKE_OBSOLETE(M, Char, kafka_row_delimiter, '\0') \

    /** TODO: */
    /* https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md */
    /* https://github.com/edenhill/librdkafka/blob/v1.4.2/src/rdkafka_conf.c */

#define LIST_OF_KAFKA_SETTINGS(M, ALIAS)  \
    KAFKA_RELATED_SETTINGS(M, ALIAS)      \
    OBSOLETE_KAFKA_SETTINGS(M, ALIAS)     \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS) \

DECLARE_SETTINGS_TRAITS(KafkaSettingsTraits, LIST_OF_KAFKA_SETTINGS)


/** Settings for the Kafka engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct KafkaSettings : public BaseSettings<KafkaSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);

    void sanityCheck() const;
};

}
