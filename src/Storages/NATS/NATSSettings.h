#pragma once

#include <Core/BaseSettings.h>
#include <Core/FormatFactorySettingsDeclaration.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsObsoleteMacros.h>

namespace DB
{
class ASTStorage;

#define NATS_RELATED_SETTINGS(M, ALIAS) \
    M(String, nats_url, "", "A host-port to connect to NATS server.", 0) \
    M(String, nats_subjects, "", "List of subject for NATS table to subscribe/publish to.", 0) \
    M(String, nats_format, "", "The message format.", 0) \
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
    M(String, nats_token, "", "NATS token", 0) \
    M(String, nats_credential_file, "", "Path to a NATS credentials file", 0) \
    M(UInt64, nats_startup_connect_tries, 5, "Number of connect tries at startup", 0) \
    M(UInt64, nats_max_rows_per_message, 1, "The maximum number of rows produced in one message for row-based formats.", 0) \
    M(StreamingHandleErrorMode, nats_handle_error_mode, StreamingHandleErrorMode::DEFAULT, "How to handle errors for NATS engine. Possible values: default (throw an exception after nats_skip_broken_messages broken messages), stream (save broken messages and errors in virtual columns _raw_message, _error).", 0) \

#define OBSOLETE_NATS_SETTINGS(M, ALIAS) \
    MAKE_OBSOLETE(M, Char, nats_row_delimiter, '\0') \

#define LIST_OF_NATS_SETTINGS(M, ALIAS)   \
    NATS_RELATED_SETTINGS(M, ALIAS)       \
    OBSOLETE_NATS_SETTINGS(M, ALIAS)      \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS) \

DECLARE_SETTINGS_TRAITS(NATSSettingsTraits, LIST_OF_NATS_SETTINGS)

struct NATSSettings : public BaseSettings<NATSSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};
}
