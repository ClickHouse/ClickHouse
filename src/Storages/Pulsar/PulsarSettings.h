#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;

const auto PULSAR_RESCHEDULE_MS = 500;
const auto PULSAR_MAX_THREAD_WORK_DURATION_MS = 60'000;

#define PULSAR_RELATED_SETTINGS(M, ALIAS) \
    M(String, pulsar_service_url, "", "A broker url for Pulsar engine.", 0) \
    M(String, pulsar_topic_list, "", "A list of Pulsar topics.", 0) \
    M(String, \
      pulsar_group_name, \
      "", \
      "Client group id string. All Pulsar consumers sharing the same group.id belong to the same group.", \
      0) \
    M(String, pulsar_format, "", "The message format for Pulsar engine.", 0) \
    M(String, pulsar_schema, "", "Schema identifier (used by schema-based formats) for Pulsar engine", 0) \
    M(UInt64, pulsar_num_consumers, 1, "The number of consumers per table for Pulsar engine.", 0) \
    M(UInt64, pulsar_max_block_size, 0, "Number of row collected by poll(s) for flushing data from Pulsar.", 0) \
    M(UInt64, pulsar_skip_broken_messages, 0, "Skip at least this number of broken messages from Pulsar topic per block", 0) \
    M(Milliseconds, pulsar_poll_timeout_ms, 0, "Timeout for single poll from Pulsar.", 0) \
    M(UInt64, pulsar_poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single Pulsar poll.", 0) \
    M(Milliseconds, pulsar_flush_interval_ms, 0, "Timeout for flushing data from Pulsar.", 0) \
    M(StreamingHandleErrorMode, \
      pulsar_handle_error_mode, \
      StreamingHandleErrorMode::DEFAULT, \
      "How to handle errors for Pulsar engine. Possible values: default (throw an exception after pulsar_skip_broken_messages broken " \
      "messages), stream (save broken messages and errors in virtual columns _raw_message, _error).", \
      0) \
    M(UInt64, pulsar_max_rows_per_message, 1, "The maximum number of rows produced in one Pulsar message for row-based formats.", 0)


#define OBSOLETE_PULSAR_SETTINGS(M, ALIAS) MAKE_OBSOLETE(M, Char, pulsar_row_delimiter, '\0')

#define LIST_OF_PULSAR_SETTINGS(M, ALIAS) \
    PULSAR_RELATED_SETTINGS(M, ALIAS) \
    OBSOLETE_PULSAR_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(PulsarSettingsTraits, LIST_OF_PULSAR_SETTINGS)


/** Settings for the Pulsar engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct PulsarSettings : public BaseSettings<PulsarSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
