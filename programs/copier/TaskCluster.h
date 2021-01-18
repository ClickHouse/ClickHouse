#pragma once

#include "Aliases.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct TaskCluster
{
    TaskCluster(const String & task_zookeeper_path_, const String & default_local_database_)
            : task_zookeeper_path(task_zookeeper_path_), default_local_database(default_local_database_) {}

    void loadTasks(const Poco::Util::AbstractConfiguration & config, const String & base_key = "");

    /// Set (or update) settings and max_workers param
    void reloadSettings(const Poco::Util::AbstractConfiguration & config, const String & base_key = "");

    /// Base node for all tasks. Its structure:
    ///  workers/ - directory with active workers (amount of them is less or equal max_workers)
    ///  description - node with task configuration
    ///  table_table1/ - directories with per-partition copying status
    String task_zookeeper_path;

    /// Database used to create temporary Distributed tables
    String default_local_database;

    /// Limits number of simultaneous workers
    UInt64 max_workers = 0;

    /// Base settings for pull and push
    Settings settings_common;
    /// Settings used to fetch data
    Settings settings_pull;
    /// Settings used to insert data
    Settings settings_push;

    String clusters_prefix;

    /// Subtasks
    TasksTable table_tasks;

    std::random_device random_device;
    pcg64 random_engine;
};

inline void DB::TaskCluster::loadTasks(const Poco::Util::AbstractConfiguration & config, const String & base_key)
{
    String prefix = base_key.empty() ? "" : base_key + ".";

    clusters_prefix = prefix + "remote_servers";
    if (!config.has(clusters_prefix))
        throw Exception("You should specify list of clusters in " + clusters_prefix, ErrorCodes::BAD_ARGUMENTS);

    Poco::Util::AbstractConfiguration::Keys tables_keys;
    config.keys(prefix + "tables", tables_keys);

    for (const auto & table_key : tables_keys)
    {
        table_tasks.emplace_back(*this, config, prefix + "tables", table_key);
    }
}

inline void DB::TaskCluster::reloadSettings(const Poco::Util::AbstractConfiguration & config, const String & base_key)
{
    String prefix = base_key.empty() ? "" : base_key + ".";

    max_workers = config.getUInt64(prefix + "max_workers");

    settings_common = Settings();
    if (config.has(prefix + "settings"))
        settings_common.loadSettingsFromConfig(prefix + "settings", config);

    settings_pull = settings_common;
    if (config.has(prefix + "settings_pull"))
        settings_pull.loadSettingsFromConfig(prefix + "settings_pull", config);

    settings_push = settings_common;
    if (config.has(prefix + "settings_push"))
        settings_push.loadSettingsFromConfig(prefix + "settings_push", config);

    auto set_default_value = [] (auto && setting, auto && default_value)
    {
        setting = setting.changed ? setting.value : default_value;
    };

    /// Override important settings
    settings_pull.readonly = 1;
    settings_push.insert_distributed_sync = 1;
    set_default_value(settings_pull.load_balancing, LoadBalancing::NEAREST_HOSTNAME);
    set_default_value(settings_pull.max_threads, 1);
    set_default_value(settings_pull.max_block_size, 8192UL);
    set_default_value(settings_pull.preferred_block_size_bytes, 0);
    set_default_value(settings_push.insert_distributed_timeout, 0);
}

}
