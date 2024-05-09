#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;


#define STREAMQUEUE_RELATED_SETTINGS(M, ALIAS) \
    M(String, keeper_path, "/clickhouse/streamqueue/", "Zookeeper node path", 0) \
    M(UInt32, streamqueue_polling_min_timeout_ms, 1000, "Minimal timeout before next polling", 0) \
    M(UInt32, streamqueue_max_rows_per_iter, 100, "Max forward rows loaded in one iteration", 0) \
    M(UInt32, streamqueue_max_shift_back_per_iter, 0, "Max shift back in one iteration", 0) \
    M(UInt32, streamqueue_min_key, 0, "Minimal key to load from", 0)

#define LIST_OF_STREAMQUEUE_SETTINGS(M, ALIAS) \
    STREAMQUEUE_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(StreamQueueSettingsTraits, LIST_OF_STREAMQUEUE_SETTINGS)


struct StreamQueueSettings : public BaseSettings<StreamQueueSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
