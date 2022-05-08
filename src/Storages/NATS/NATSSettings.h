#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>

namespace DB
{
    class ASTStorage;


#define RABBITMQ_RELATED_SETTINGS(M) \
    M(String, nats_host_port, "", "A host-port to connect to NATS server.", 0) \
    M(String, nats_exchange_name, "clickhouse-exchange", "The exchange name, to which messages are sent.", 0) \
    M(String, nats_format, "", "The message format.", 0) \
    M(String, nats_exchange_type, "default", "The exchange type.", 0) \
    M(String, nats_routing_key_list, "5672", "A string of routing keys, separated by dots.", 0) \
    M(Char, nats_row_delimiter, '\0', "The character to be considered as a delimiter.", 0) \
    M(String, nats_schema, "", "Schema identifier (used by schema-based formats) for NATS engine", 0) \
    M(UInt64, nats_num_consumers, 1, "The number of consumer channels per table.", 0) \
    M(UInt64, nats_num_queues, 1, "The number of queues per consumer.", 0) \
    M(String, nats_queue_base, "", "Base for queue names to be able to reopen non-empty queues in case of failure.", 0) \
    M(Bool, nats_persistent, false, "For insert query messages will be made 'persistent', durable.", 0) \
    M(Bool, nats_secure, false, "Use SSL connection", 0) \
    M(String, nats_address, "", "Address for connection", 0) \
    M(UInt64, nats_skip_broken_messages, 0, "Skip at least this number of broken messages from NATS per block", 0) \
    M(UInt64, nats_max_block_size, 0, "Number of row collected before flushing data from NATS.", 0) \
    M(Milliseconds, nats_flush_interval_ms, 0, "Timeout for flushing data from NATS.", 0) \
    M(String, nats_vhost, "/", "NATS vhost.", 0) \
    M(String, nats_queue_settings_list, "", "A list of nats queue settings", 0) \
    M(Bool, nats_queue_consume, false, "Use user-defined queues and do not make any NATS setup: declaring exchanges, queues, bindings", 0) \
    M(String, nats_username, "", "NATS username", 0) \
    M(String, nats_password, "", "NATS password", 0) \
    M(Bool, nats_commit_on_select, false, "Commit messages when select query is made", 0) \

#define LIST_OF_RABBITMQ_SETTINGS(M) \
    RABBITMQ_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(NATSSettingsTraits, LIST_OF_RABBITMQ_SETTINGS)

struct NATSSettings : public BaseSettings<NATSSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};
}
