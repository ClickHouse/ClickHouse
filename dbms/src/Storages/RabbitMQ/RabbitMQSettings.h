#pragma once

#include <Core/SettingsCollection.h>


namespace DB
{

    class ASTStorage;

    struct RabbitMQSettings : public SettingsCollection<RabbitMQSettings>
    {

#define LIST_OF_RABBITMQ_SETTINGS(M)                                      \
    M(SettingString, rabbitmq_broker_list, "", "A comma-separated list of brokers for RabbitMQHandler constructor.", 0) \
    M(SettingString, rabbitmq_routing_key_list, "", "A list of routing keys to connect producer->exchange->queue<->consumer.", 0) \
    M(SettingString, rabbitmq_format, "", "The message format for RabbitMQ engine.", 0) \
    M(SettingChar, rabbitmq_row_delimiter, '\0', "The character to be considered as a delimiter.", 0) \
    M(SettingUInt64, rabbitmq_num_consumers, 1, "The number of consumers per table for RabbitMQ engine.", 0) \
    M(SettingUInt64, rabbitmq_max_block_size, 0, "The maximum block size per table for RabbitMQ engine.", 0) \
    M(SettingUInt64, rabbitmq_skip_broken_messages, 0, "Skip at least this number of broken messages per queue", 0)

    DECLARE_SETTINGS_COLLECTION(LIST_OF_RABBITMQ_SETTINGS)

    void loadFromQuery(ASTStorage & storage_def);
    };

}
