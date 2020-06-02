#pragma once

#include <Core/SettingsCollection.h>
#include <Core/Settings.h>

namespace DB
{

class ASTStorage;

/** Settings for the Kafka engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct KafkaSettings : public SettingsCollection<KafkaSettings>
{


#define KAFKA_RELATED_SETTINGS(M)                                      \
    M(SettingString, kafka_broker_list, "", "A comma-separated list of brokers for Kafka engine.", 0) \
    M(SettingString, kafka_topic_list, "", "A list of Kafka topics.", 0) \
    M(SettingString, kafka_group_name, "", "A group of Kafka consumers.", 0) \
    M(SettingString, kafka_client_id, "", "A client id of Kafka consumer.", 0) \
    M(SettingUInt64, kafka_num_consumers, 1, "The number of consumers per table for Kafka engine.", 0) \
    M(SettingBool, kafka_commit_every_batch, false, "Commit every consumed and handled batch instead of a single commit after writing a whole block", 0) \
    /* default is stream_poll_timeout_ms */ \
    M(SettingMilliseconds, kafka_poll_timeout_ms, 0, "Timeout for single poll from Kafka.", 0) \
    /* default is min(max_block_size, kafka_max_block_size)*/ \
    M(SettingUInt64, kafka_poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single Kafka poll.", 0) \
    /* default is = min_insert_block_size / kafka_num_consumers  */ \
    M(SettingUInt64, kafka_max_block_size, 0, "Number of row collected by poll(s) for flushing data from Kafka.", 0) \
    /* default is stream_flush_interval_ms */ \
    M(SettingMilliseconds, kafka_flush_interval_ms, 0, "Timeout for flushing data from Kafka.", 0) \
    /* those are mapped to format factory settings */ \
    M(SettingString, kafka_format, "", "The message format for Kafka engine.", 0) \
    M(SettingChar, kafka_row_delimiter, '\0', "The character to be considered as a delimiter in Kafka message.", 0) \
    M(SettingString, kafka_schema, "", "Schema identifier (used by schema-based formats) for Kafka engine", 0) \
    M(SettingUInt64, kafka_skip_broken_messages, 0, "Skip at least this number of broken messages from Kafka topic per block", 0)

#define LIST_OF_KAFKA_SETTINGS(M) \
    KAFKA_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

    DECLARE_SETTINGS_COLLECTION(LIST_OF_KAFKA_SETTINGS)

    void loadFromQuery(ASTStorage & storage_def);
};

}
