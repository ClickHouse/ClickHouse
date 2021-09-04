#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;


#define FILELOG_RELATED_SETTINGS(M) \
    /* default is stream_poll_timeout_ms */ \
    M(Milliseconds, filelog_poll_timeout_ms, 0, "Timeout for single poll from FileLog.", 0) \
    /* default is min(max_block_size, kafka_max_block_size)*/ \
    M(UInt64, filelog_poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single Kafka poll.", 0) \
    /* default is = max_insert_block_size / kafka_num_consumers  */ \
    M(UInt64, filelog_max_block_size, 0, "Number of row collected by poll(s) for flushing data from Kafka.", 0) \
    M(UInt64, filelog_max_threads, 8, "Number of max threads to parse files, default is 8", 0)

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
