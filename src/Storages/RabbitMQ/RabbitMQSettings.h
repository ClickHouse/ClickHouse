#pragma once

#include <Core/SettingsCollection.h>

namespace DB
{
    class ASTStorage;

    struct RabbitMQSettings : public SettingsCollection<RabbitMQSettings>
    {

#define LIST_OF_RABBITMQ_SETTINGS(M)                                      \
    M(SettingString, rabbitmq_host_port, "", "A host-port to connect to RabbitMQ server.", 0) \
    M(SettingString, rabbitmq_routing_key, "5672", "A routing key to connect producer->exchange->queue<->consumer.", 0) \
    M(SettingString, rabbitmq_exchange_name, "clickhouse-exchange", "The exhange name, to which messages are sent. Needed to bind queues to it.", 0) \
    M(SettingString, rabbitmq_format, "", "The message format.", 0) \
    M(SettingChar, rabbitmq_row_delimiter, '\0', "The character to be considered as a delimiter.", 0) \
    M(SettingUInt64, rabbitmq_num_consumers, 1, "The number of consumer channels per table.", 0) \
    M(SettingUInt64, rabbitmq_num_queues, 1, "The number of queues per consumer.", 0) \
    M(SettingUInt64, rabbitmq_hash_exchange, 0, "A flag which indicates whether consistent-hash-exchange should be used.", 0) \

    DECLARE_SETTINGS_COLLECTION(LIST_OF_RABBITMQ_SETTINGS)

    void loadFromQuery(ASTStorage & storage_def);
    };
}
