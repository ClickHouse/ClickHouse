#include <Storages/System/StorageSystemServerSettings.h>
#include <Core/BackgroundSchedulePool.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Core/ServerSettings.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>


namespace CurrentMetrics
{
    extern const Metric BackgroundSchedulePoolSize;
    extern const Metric BackgroundBufferFlushSchedulePoolSize;
    extern const Metric BackgroundDistributedSchedulePoolSize;
    extern const Metric BackgroundMessageBrokerSchedulePoolSize;
}

namespace DB
{
ColumnsDescription StorageSystemServerSettings::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Server setting name."},
        {"value", std::make_shared<DataTypeString>(), "Server setting value."},
        {"default", std::make_shared<DataTypeString>(), "Server setting default value."},
        {"changed", std::make_shared<DataTypeUInt8>(), "Shows whether a setting was specified in config.xml"},
        {"description", std::make_shared<DataTypeString>(), "Short server setting description."},
        {"type", std::make_shared<DataTypeString>(), "Server setting value type."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."},
        {"is_hot_reloadable", std::make_shared<DataTypeUInt8>(), "Shows whether a setting can be changed at runtime."}
    };
}

void StorageSystemServerSettings::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    // Server settings that have been reloaded from the config file.
    std::unordered_map<std::string, std::string> updated = {
        {"max_server_memory_usage", std::to_string(total_memory_tracker.getHardLimit())},
        {"allow_use_jemalloc_memory", std::to_string(total_memory_tracker.getAllowUseJemallocMmemory())},

        {"max_table_size_to_drop", std::to_string(context->getMaxTableSizeToDrop())},
        {"max_partition_size_to_drop", std::to_string(context->getMaxPartitionSizeToDrop())},

        {"max_concurrent_queries", std::to_string(context->getProcessList().getMaxSize())},
        {"max_concurrent_insert_queries", std::to_string(context->getProcessList().getMaxInsertQueriesAmount())},
        {"max_concurrent_select_queries", std::to_string(context->getProcessList().getMaxSelectQueriesAmount())},

        {"background_buffer_flush_schedule_pool_size", std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundBufferFlushSchedulePoolSize))},
        {"background_schedule_pool_size", std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundSchedulePoolSize))},
        {"background_message_broker_schedule_pool_size", std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundMessageBrokerSchedulePoolSize))},
        {"background_distributed_schedule_pool_size", std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundDistributedSchedulePoolSize))}
    };

    if (context->areBackgroundExecutorsInitialized())
    {
        updated.insert({"background_pool_size", std::to_string(context->getMergeMutateExecutor()->getMaxThreads())});
        updated.insert({"background_move_pool_size", std::to_string(context->getMovesExecutor()->getMaxThreads())});
        updated.insert({"background_fetches_pool_size", std::to_string(context->getFetchesExecutor()->getMaxThreads())});
        updated.insert({"background_common_pool_size", std::to_string(context->getCommonExecutor()->getMaxThreads())});
    }

    const auto & config = context->getConfigRef();
    ServerSettings settings;
    settings.loadSettingsFromConfig(config);

    for (const auto & setting : settings.all())
    {
        const auto & setting_name = setting.getName();
        const auto & it = updated.find(setting_name);

        res_columns[0]->insert(setting_name);
        res_columns[1]->insert((it != updated.end()) ? it->second : setting.getValueString());
        res_columns[2]->insert(setting.getDefaultValueString());
        res_columns[3]->insert(setting.isValueChanged());
        res_columns[4]->insert(setting.getDescription());
        res_columns[5]->insert(setting.getTypeName());
        res_columns[6]->insert(setting.isObsolete());
        res_columns[7]->insert((it != updated.end()) ? true : false);
    }
}

}
