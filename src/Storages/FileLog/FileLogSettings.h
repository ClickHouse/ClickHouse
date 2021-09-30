#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;


#define FILELOG_RELATED_SETTINGS(M) \
    /* default is stream_poll_timeout_ms */ \
    M(Milliseconds, poll_timeout_ms, 0, "Timeout for single poll from StorageFileLog.", 0) \
    M(UInt64, poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single StorageFileLog poll.", 0) \
    M(UInt64, max_block_size, 0, "Number of row collected by poll(s) for flushing data from StorageFileLog.", 0) \
    M(UInt64, max_threads, 8, "Number of max threads to parse files, default is 8", 0)

#define LIST_OF_FILELOG_SETTINGS(M) \
    FILELOG_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(FileLogSettingsTraits, LIST_OF_FILELOG_SETTINGS)


/** Settings for the Kafka engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct FileLogSettings : public BaseSettings<FileLogSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
