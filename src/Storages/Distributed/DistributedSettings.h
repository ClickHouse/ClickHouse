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

#define LIST_OF_DISTRIBUTED_SETTINGS(M, ALIAS) \
    M(Bool, fsync_after_insert, false, "Do fsync for every inserted. Will decreases performance of inserts (only for background INSERT, i.e. distributed_foreground_insert=false)", 0) \
    M(Bool, fsync_directories, false, "Do fsync for temporary directory (that is used for background INSERT only) after all part operations (writes, renames, etc.).", 0) \
    /** This is the distributed version of the skip_unavailable_shards setting available in src/Core/Settings.cpp */ \
    M(Bool, skip_unavailable_shards, false, "If true, ClickHouse silently skips unavailable shards. Shard is marked as unavailable when: 1) The shard cannot be reached due to a connection failure. 2) Shard is unresolvable through DNS. 3) Table does not exist on the shard.", 0) \
    /** Inserts settings. */ \
    M(UInt64, bytes_to_throw_insert, 0, "If more than this number of compressed bytes will be pending for background INSERT, an exception will be thrown. 0 - do not throw.", 0) \
    M(UInt64, bytes_to_delay_insert, 0, "If more than this number of compressed bytes will be pending for background INSERT, the query will be delayed. 0 - do not delay.", 0) \
    M(UInt64, max_delay_to_insert, 60, "Max delay of inserting data into Distributed table in seconds, if there are a lot of pending bytes for background send.", 0) \
    /** Async INSERT settings */ \
    M(UInt64, background_insert_batch, 0, "Default - distributed_background_insert_batch", 0) ALIAS(monitor_batch_inserts) \
    M(UInt64, background_insert_split_batch_on_failure, 0, "Default - distributed_background_insert_split_batch_on_failure", 0) ALIAS(monitor_split_batch_on_failure) \
    M(Milliseconds, background_insert_sleep_time_ms, 0, "Default - distributed_background_insert_sleep_time_ms", 0) ALIAS(monitor_sleep_time_ms) \
    M(Milliseconds, background_insert_max_sleep_time_ms, 0, "Default - distributed_background_insert_max_sleep_time_ms", 0) ALIAS(monitor_max_sleep_time_ms) \
    M(Bool, flush_on_detach, true, "Flush data to remote nodes on DETACH/DROP/server shutdown", 0) \

DECLARE_SETTINGS_TRAITS(DistributedSettingsTraits, LIST_OF_DISTRIBUTED_SETTINGS)


/** Settings for the Distributed family of engines.
  */
struct DistributedSettings : public BaseSettings<DistributedSettingsTraits>
{
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
    void loadFromQuery(ASTStorage & storage_def);
};

}
