#pragma once

#include <Core/BaseSettings.h>


namespace DB
{
    class ASTStorage;


#define LIST_OF_RABBITMQ_SETTINGS(M) \
    M(String, rabbitmq_host_port, "", "A host-port to connect to RabbitMQ server.", 0) \
    M(String, rabbitmq_routing_key_list, "5672", "A string of routing keys, separated by dots.", 0) \
    M(String, rabbitmq_exchange_name, "clickhouse-exchange", "The exchange name, to which messages are sent.", 0) \
    M(String, rabbitmq_format, "", "The message format.", 0) \
    M(Char, rabbitmq_row_delimiter, '\0', "The character to be considered as a delimiter.", 0) \
    M(String, rabbitmq_exchange_type, "default", "The exchange type.", 0) \
    M(UInt64, rabbitmq_num_consumers, 1, "The number of consumer channels per table.", 0) \
    M(UInt64, rabbitmq_num_queues, 1, "The number of queues per consumer.", 0) \
    M(Bool, rabbitmq_transactional_channel, false, "Use transactional channel for publishing.", 0) \

    DECLARE_SETTINGS_TRAITS(RabbitMQSettingsTraits, LIST_OF_RABBITMQ_SETTINGS)


struct RabbitMQSettings : public BaseSettings<RabbitMQSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};
}
