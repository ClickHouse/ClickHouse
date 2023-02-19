#pragma once

#include "TaskTable.h"

#include <Core/Settings.h>
#include <base/types.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <random>

namespace DB
{

struct TaskCluster
{
    TaskCluster(const String & task_zookeeper_path_, const String & default_local_database_);

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

}
