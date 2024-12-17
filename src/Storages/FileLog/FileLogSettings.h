#pragma once

#include <Core/BaseSettings.h>
#include <Core/FormatFactorySettingsDeclaration.h>
#include <Core/SettingsEnums.h>

namespace DB
{
class ASTStorage;


#define FILELOG_RELATED_SETTINGS(M, ALIAS) \
    /* default is stream_poll_timeout_ms */ \
    M(Milliseconds, poll_timeout_ms, 0, "Timeout for single poll from StorageFileLog.", 0) \
    M(UInt64, poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single StorageFileLog poll.", 0) \
    M(UInt64, max_block_size, 0, "Number of row collected by poll(s) for flushing data from StorageFileLog.", 0) \
    M(UInt64, max_threads, 0, "Number of max threads to parse files, default is 0, which means the number will be max(1, physical_cpu_cores / 4)", 0) \
    M(Milliseconds, poll_directory_watch_events_backoff_init, 500, "The initial sleep value for watch directory thread.", 0) \
    M(Milliseconds, poll_directory_watch_events_backoff_max, 32000, "The max sleep value for watch directory thread.", 0) \
    M(UInt64, poll_directory_watch_events_backoff_factor, 2, "The speed of backoff, exponential by default", 0) \
    M(StreamingHandleErrorMode, handle_error_mode, StreamingHandleErrorMode::DEFAULT, "How to handle errors for FileLog engine. Possible values: default (throw an exception after nats_skip_broken_messages broken messages), stream (save broken messages and errors in virtual columns _raw_message, _error).", 0) \

#define LIST_OF_FILELOG_SETTINGS(M, ALIAS) \
    FILELOG_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(FileLogSettingsTraits, LIST_OF_FILELOG_SETTINGS)


/** Settings for the FileLog engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct FileLogSettings : public BaseSettings<FileLogSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
