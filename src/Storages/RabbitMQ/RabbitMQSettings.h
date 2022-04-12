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
    M(Bool, rabbitmq_persistent, false, "For insert query messages will be made 'persistent', durable.", 0) \
    M(Bool, rabbitmq_secure, false, "Use SSL connection", 0) \
    M(String, rabbitmq_address, "", "Address for connection", 0) \
    M(UInt64, rabbitmq_skip_broken_messages, 0, "Skip at least this number of broken messages from RabbitMQ per block", 0) \
    M(UInt64, rabbitmq_max_block_size, 0, "Number of row collected before flushing data from RabbitMQ.", 0) \
    M(Milliseconds, rabbitmq_flush_interval_ms, 0, "Timeout for flushing data from RabbitMQ.", 0) \
    M(String, rabbitmq_vhost, "/", "RabbitMQ vhost.", 0) \
    M(String, rabbitmq_queue_settings_list, "", "A list of rabbitmq queue settings", 0) \
    M(Bool, rabbitmq_queue_consume, false, "Use user-defined queues and do not make any RabbitMQ setup: declaring exchanges, queues, bindings", 0) \
    M(String, rabbitmq_username, "", "RabbitMQ username", 0) \
    M(String, rabbitmq_password, "", "RabbitMQ password", 0) \
    M(Bool, rabbitmq_commit_on_select, false, "Commit messages when select query is made", 0) \

#define LIST_OF_RABBITMQ_SETTINGS(M) \
    RABBITMQ_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(RabbitMQSettingsTraits, LIST_OF_RABBITMQ_SETTINGS)

struct RabbitMQSettings : public BaseSettings<RabbitMQSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};
}
