#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>

namespace DB
{
class ASTStorage;

#define NATS_RELATED_SETTINGS(M) \
    M(String, nats_url, "", "A host-port to connect to NATS server.", 0) \
    M(String, nats_subjects, "", "List of subject for NATS table to subscribe/publish to.", 0) \
    M(String, nats_format, "", "The message format.", 0) \
    M(Char, nats_row_delimiter, '\0', "The character to be considered as a delimiter.", 0) \
    M(String, nats_schema, "", "Schema identifier (used by schema-based formats) for NATS engine", 0) \
    M(UInt64, nats_num_consumers, 1, "The number of consumer channels per table.", 0) \
    M(String, nats_queue_group, "", "Name for queue group of NATS subscribers.", 0) \
    M(Bool, nats_secure, false, "Use SSL connection", 0) \
    M(UInt64, nats_max_reconnect, 5, "Maximum amount of reconnection attempts.", 0) \
    M(UInt64, nats_reconnect_wait, 2000, "Amount of time in milliseconds to sleep between each reconnect attempt.", 0) \
    M(String, nats_server_list, "", "Server list for connection", 0) \
    M(UInt64, nats_skip_broken_messages, 0, "Skip at least this number of broken messages from NATS per block", 0) \
    M(UInt64, nats_max_block_size, 0, "Number of row collected before flushing data from NATS.", 0) \
    M(Milliseconds, nats_flush_interval_ms, 0, "Timeout for flushing data from NATS.", 0) \
    M(String, nats_username, "", "NATS username", 0) \
    M(String, nats_password, "", "NATS password", 0) \
    M(String, nats_token, "", "NATS token", 0)

#define LIST_OF_NATS_SETTINGS(M) \
    NATS_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(NATSSettingsTraits, LIST_OF_NATS_SETTINGS)

struct NATSSettings : public BaseSettings<NATSSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};
}
