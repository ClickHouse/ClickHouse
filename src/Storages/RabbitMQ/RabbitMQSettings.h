#pragma once

#include <Core/SettingsCollection.h>

namespace DB
{
    class ASTStorage;

    struct RabbitMQSettings : public SettingsCollection<RabbitMQSettings>
    {

#define LIST_OF_RABBITMQ_SETTINGS(M)                                      \
    M(SettingString, rabbitmq_host_port, "", "A host-port to connect to RabbitMQ server.", 0) \
    M(SettingString, rabbitmq_routing_key_list, "5672", "A string of routing keys, separated by dots.", 0) \
    M(SettingString, rabbitmq_exchange_name, "clickhouse-exchange", "The exchange name, to which messages are sent.", 0) \
    M(SettingString, rabbitmq_format, "", "The message format.", 0) \
    M(SettingChar, rabbitmq_row_delimiter, '\0', "The character to be considered as a delimiter.", 0) \
    M(SettingUInt64, rabbitmq_num_consumers, 1, "The number of consumer channels per table.", 0) \
    M(SettingUInt64, rabbitmq_num_queues, 1, "The number of queues per consumer.", 0) \
    M(SettingString, rabbitmq_exchange_type, "default", "The exchange type.", 0) \

    DECLARE_SETTINGS_COLLECTION(LIST_OF_RABBITMQ_SETTINGS)

    void loadFromQuery(ASTStorage & storage_def);
    };
}
