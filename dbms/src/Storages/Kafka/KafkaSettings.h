#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <Interpreters/SettingsCommon.h>


namespace DB
{

class ASTStorage;

/** Settings for the Kafka engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct KafkaSettings
{

#define APPLY_FOR_KAFKA_SETTINGS(M) \
    M(SettingString, kafka_broker_list, "", "A comma-separated list of brokers for Kafka engine.") \
    M(SettingString, kafka_topic_list, "", "A list of Kafka topics.") \
    M(SettingString, kafka_group_name, "", "A group of Kafka consumers.") \
    M(SettingString, kafka_format, "", "The message format for Kafka engine.") \
    M(SettingChar, kafka_row_delimiter, '\0', "The character to be considered as a delimiter in Kafka message.") \
    M(SettingString, kafka_schema, "", "Schema identifier (used by schema-based formats) for Kafka engine") \
    M(SettingUInt64, kafka_num_consumers, 1, "The number of consumers per table for Kafka engine.") \
    M(SettingUInt64, kafka_max_block_size, 0, "The maximum block size per table for Kafka engine.") \
    M(SettingUInt64, kafka_skip_broken_messages, 0, "Skip at least this number of broken messages from Kafka topic per block")

#define DECLARE(TYPE, NAME, DEFAULT, DESCRIPTION) \
    TYPE NAME {DEFAULT};

    APPLY_FOR_KAFKA_SETTINGS(DECLARE)

#undef DECLARE

public:
    void loadFromQuery(ASTStorage & storage_def);
};

}
