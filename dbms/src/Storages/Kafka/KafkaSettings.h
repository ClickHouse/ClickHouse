#pragma once

#include <Core/SettingsCommon.h>


namespace DB
{

class ASTStorage;

/** Settings for the Kafka engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct KafkaSettings : public SettingsCollection<KafkaSettings>
{


/// M (mutable) for normal settings, IM (immutable) for not updateable settings.
#define LIST_OF_KAFKA_SETTINGS(M, IM)                                      \
    IM(SettingString, kafka_broker_list, "", "A comma-separated list of brokers for Kafka engine.") \
    IM(SettingString, kafka_topic_list, "", "A list of Kafka topics.") \
    IM(SettingString, kafka_group_name, "", "A group of Kafka consumers.") \
    IM(SettingString, kafka_format, "", "The message format for Kafka engine.") \
    IM(SettingChar, kafka_row_delimiter, '\0', "The character to be considered as a delimiter in Kafka message.") \
    IM(SettingString, kafka_schema, "", "Schema identifier (used by schema-based formats) for Kafka engine") \
    IM(SettingUInt64, kafka_num_consumers, 1, "The number of consumers per table for Kafka engine.") \
    IM(SettingUInt64, kafka_max_block_size, 0, "The maximum block size per table for Kafka engine.") \
    IM(SettingUInt64, kafka_skip_broken_messages, 0, "Skip at least this number of broken messages from Kafka topic per block") \
    IM(SettingUInt64, kafka_commit_every_batch, 0, "Commit every consumed and handled batch instead of a single commit after writing a whole block")

    DECLARE_SETTINGS_COLLECTION(LIST_OF_KAFKA_SETTINGS)

    void loadFromQuery(ASTStorage & storage_def);
};

}
