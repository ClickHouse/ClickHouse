#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>

namespace DB
{
    class ASTStorage;


#define RABBITMQ_RELATED_SETTINGS(M) \
    M(String, rabbitmq_host_port, "", "A host-port to connect to RabbitMQ server.", 0) \
    M(String, rabbitmq_exchange_name, "clickhouse-exchange", "The exchange name, to which messages are sent.", 0) \
    M(String, rabbitmq_format, "", "The message format.", 0) \
    M(String, rabbitmq_exchange_type, "default", "The exchange type.", 0) \
    M(String, rabbitmq_routing_key_list, "5672", "A string of routing keys, separated by dots.", 0) \
    M(Char, rabbitmq_row_delimiter, '\0', "The character to be considered as a delimiter.", 0) \
    M(String, rabbitmq_schema, "", "Schema identifier (used by schema-based formats) for RabbitMQ engine", 0) \
    M(UInt64, rabbitmq_num_consumers, 1, "The number of consumer channels per table.", 0) \
    M(UInt64, rabbitmq_num_queues, 1, "The number of queues per consumer.", 0) \
    M(String, rabbitmq_queue_base, "", "Base for queue names to be able to reopen non-empty queues in case of failure.", 0) \
    M(String, rabbitmq_deadletter_exchange, "", "Exchange name to be passed as a dead-letter-exchange name.", 0) \
    M(Bool, rabbitmq_persistent, false, "If set, delivery mode will be set to 2 (makes messages 'persistent', durable).", 0) \
    M(UInt64, rabbitmq_skip_broken_messages, 0, "Skip at least this number of broken messages from RabbitMQ per block", 0) \
    M(UInt64, rabbitmq_max_block_size, 0, "Number of row collected before flushing data from RabbitMQ.", 0) \
    M(Milliseconds, rabbitmq_flush_interval_ms, 0, "Timeout for flushing data from RabbitMQ.", 0) \

#define LIST_OF_RABBITMQ_SETTINGS(M) \
    RABBITMQ_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(RabbitMQSettingsTraits, LIST_OF_RABBITMQ_SETTINGS)

struct RabbitMQSettings : public BaseSettings<RabbitMQSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};
}
