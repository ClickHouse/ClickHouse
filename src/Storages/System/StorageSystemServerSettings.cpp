#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerSettings.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/MMappedFileCache.h>
#include <IO/UncompressedCache.h>
#include <Interpreters/Context.h>
#include <Common/Config/ConfigReloader.h>
#include <Interpreters/ProcessList.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <Storages/System/StorageSystemServerSettings.h>


namespace CurrentMetrics
{
    extern const Metric BackgroundSchedulePoolSize;
    extern const Metric BackgroundBufferFlushSchedulePoolSize;
    extern const Metric BackgroundDistributedSchedulePoolSize;
    extern const Metric BackgroundMessageBrokerSchedulePoolSize;
}

namespace DB
{

enum class ChangeableWithoutRestart : uint8_t
{
    No,
    IncreaseOnly,
    DecreaseOnly,
    Yes
};

ColumnsDescription StorageSystemServerSettings::getColumnsDescription()
{
    auto changeable_without_restart_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"No",              static_cast<Int8>(ChangeableWithoutRestart::No)},
            {"IncreaseOnly",    static_cast<Int8>(ChangeableWithoutRestart::IncreaseOnly)},
            {"DecreaseOnly",    static_cast<Int8>(ChangeableWithoutRestart::DecreaseOnly)},
            {"Yes",             static_cast<Int8>(ChangeableWithoutRestart::Yes)},
        });

    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Server setting name."},
        {"value", std::make_shared<DataTypeString>(), "Server setting value."},
        {"default", std::make_shared<DataTypeString>(), "Server setting default value."},
        {"changed", std::make_shared<DataTypeUInt8>(), "Shows whether a setting was specified in config.xml"},
        {"description", std::make_shared<DataTypeString>(), "Short server setting description."},
        {"type", std::make_shared<DataTypeString>(), "Server setting value type."},
        {"changeable_without_restart", std::move(changeable_without_restart_type), "Shows whether a setting can be changed at runtime."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."}
    };
}

void StorageSystemServerSettings::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// When the server configuration file is periodically re-loaded from disk, the server components (e.g. memory tracking) are updated
    /// with new the setting values but the settings themselves are not stored between re-loads. As a result, if one wants to know the
    /// current setting values, one needs to ask the components directly.
    std::unordered_map<String, std::pair<String, ChangeableWithoutRestart>> changeable_settings = {
        {"max_server_memory_usage", {std::to_string(total_memory_tracker.getHardLimit()), ChangeableWithoutRestart::Yes}},
        {"allow_use_jemalloc_memory", {std::to_string(total_memory_tracker.getAllowUseJemallocMmemory()), ChangeableWithoutRestart::Yes}},

        {"max_table_size_to_drop", {std::to_string(context->getMaxTableSizeToDrop()), ChangeableWithoutRestart::Yes}},
        {"max_partition_size_to_drop", {std::to_string(context->getMaxPartitionSizeToDrop()), ChangeableWithoutRestart::Yes}},

        {"max_concurrent_queries", {std::to_string(context->getProcessList().getMaxSize()), ChangeableWithoutRestart::Yes}},
        {"max_concurrent_insert_queries", {std::to_string(context->getProcessList().getMaxInsertQueriesAmount()), ChangeableWithoutRestart::Yes}},
        {"max_concurrent_select_queries", {std::to_string(context->getProcessList().getMaxSelectQueriesAmount()), ChangeableWithoutRestart::Yes}},
        {"max_waiting_queries", {std::to_string(context->getProcessList().getMaxWaitingQueriesAmount()), ChangeableWithoutRestart::Yes}},

        {"background_buffer_flush_schedule_pool_size", {std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundBufferFlushSchedulePoolSize)), ChangeableWithoutRestart::IncreaseOnly}},
        {"background_schedule_pool_size", {std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundSchedulePoolSize)), ChangeableWithoutRestart::IncreaseOnly}},
        {"background_message_broker_schedule_pool_size", {std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundMessageBrokerSchedulePoolSize)), ChangeableWithoutRestart::IncreaseOnly}},
        {"background_distributed_schedule_pool_size", {std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundDistributedSchedulePoolSize)), ChangeableWithoutRestart::IncreaseOnly}},

        {"mark_cache_size", {std::to_string(context->getMarkCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
        {"uncompressed_cache_size", {std::to_string(context->getUncompressedCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
        {"index_mark_cache_size", {std::to_string(context->getIndexMarkCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
        {"index_uncompressed_cache_size", {std::to_string(context->getIndexUncompressedCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
        {"mmap_cache_size", {std::to_string(context->getMMappedFileCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},

        {"merge_workload", {context->getMergeWorkload(), ChangeableWithoutRestart::Yes}},
        {"mutation_workload", {context->getMutationWorkload(), ChangeableWithoutRestart::Yes}},
        {"config_reload_interval_ms", {std::to_string(context->getConfigReloaderInterval()), ChangeableWithoutRestart::Yes}}
    };

    if (context->areBackgroundExecutorsInitialized())
    {
        changeable_settings.insert({"background_pool_size", {std::to_string(context->getMergeMutateExecutor()->getMaxThreads()), ChangeableWithoutRestart::IncreaseOnly}});
        changeable_settings.insert({"background_move_pool_size", {std::to_string(context->getMovesExecutor()->getMaxThreads()), ChangeableWithoutRestart::IncreaseOnly}});
        changeable_settings.insert({"background_fetches_pool_size", {std::to_string(context->getFetchesExecutor()->getMaxThreads()), ChangeableWithoutRestart::IncreaseOnly}});
        changeable_settings.insert({"background_common_pool_size", {std::to_string(context->getCommonExecutor()->getMaxThreads()), ChangeableWithoutRestart::IncreaseOnly}});
    }

    const auto & config = context->getConfigRef();
    ServerSettings settings;
    settings.loadSettingsFromConfig(config);

    for (const auto & setting : settings.all())
    {
        const auto & setting_name = setting.getName();

        const auto & changeable_settings_it = changeable_settings.find(setting_name);
        const bool is_changeable = (changeable_settings_it != changeable_settings.end());

        res_columns[0]->insert(setting_name);
        res_columns[1]->insert(is_changeable ? changeable_settings_it->second.first : setting.getValueString());
        res_columns[2]->insert(setting.getDefaultValueString());
        res_columns[3]->insert(setting.isValueChanged());
        res_columns[4]->insert(setting.getDescription());
        res_columns[5]->insert(setting.getTypeName());
        res_columns[6]->insert(is_changeable ? changeable_settings_it->second.second : ChangeableWithoutRestart::No);
        res_columns[7]->insert(setting.isObsolete());
    }
}

}
