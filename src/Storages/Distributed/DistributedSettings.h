#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;

#define LIST_OF_DISTRIBUTED_SETTINGS(M) \
    M(Bool, fsync_after_insert, false, "Do fsync for every inserted. Will decreases performance of inserts (only for async INSERT, i.e. insert_distributed_sync=false)", 0) \
    M(Bool, fsync_directories, false, "Do fsync for temporary directory (that is used for async INSERT only) after all part operations (writes, renames, etc.).", 0) \
    /** Inserts settings. */ \
    M(UInt64, bytes_to_throw_insert, 0, "If more than this number of compressed bytes will be pending for async INSERT, an exception will be thrown. 0 - do not throw.", 0) \
    M(UInt64, bytes_to_delay_insert, 0, "If more than this number of compressed bytes will be pending for async INSERT, the query will be delayed. 0 - do not delay.", 0) \
    M(UInt64, max_delay_to_insert, 60, "Max delay of inserting data into Distributed table in seconds, if there are a lot of pending bytes for async send.", 0) \
    /** Directory monitor settings */ \
    M(UInt64, monitor_batch_inserts, 0, "Default - distributed_directory_monitor_batch_inserts", 0) \
    M(UInt64, monitor_split_batch_on_failure, 0, "Default - distributed_directory_monitor_split_batch_on_failure", 0) \
    M(Milliseconds, monitor_sleep_time_ms, 0, "Default - distributed_directory_monitor_sleep_time_ms", 0) \
    M(Milliseconds, monitor_max_sleep_time_ms, 0, "Default - distributed_directory_monitor_max_sleep_time_ms", 0) \

DECLARE_SETTINGS_TRAITS(DistributedSettingsTraits, LIST_OF_DISTRIBUTED_SETTINGS)


/** Settings for the Distributed family of engines.
  */
struct DistributedSettings : public BaseSettings<DistributedSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
