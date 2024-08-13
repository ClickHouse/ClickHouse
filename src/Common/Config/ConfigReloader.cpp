#include "ConfigReloader.h"

#include <Poco/Util/Application.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include "ConfigProcessor.h"
#include <filesystem>
#include <Common/filesystemHelpers.h>


namespace fs = std::filesystem;

namespace DB
{

ConfigReloader::ConfigReloader(
        std::string_view config_path_,
        const std::vector<std::string>& extra_paths_,
        const std::string & preprocessed_dir_,
        zkutil::ZooKeeperNodeCache && zk_node_cache_,
        const zkutil::EventPtr & zk_changed_event_,
        Updater && updater_,
        bool already_loaded)
    : config_path(config_path_)
    , extra_paths(extra_paths_)
    , preprocessed_dir(preprocessed_dir_)
    , zk_node_cache(std::move(zk_node_cache_))
    , zk_changed_event(zk_changed_event_)
    , updater(std::move(updater_))
{
    if (!already_loaded)
        reloadIfNewer(/* force = */ true, /* throw_on_error = */ true, /* fallback_to_preprocessed = */ true, /* initial_loading = */ true);
}


void ConfigReloader::start()
{
    std::lock_guard lock(reload_mutex);
    if (!thread.joinable())
    {
        quit = false;
        thread = ThreadFromGlobalPool(&ConfigReloader::run, this);
    }
}


void ConfigReloader::stop()
{
    std::unique_lock lock(reload_mutex);
    if (!thread.joinable())
        return;
    quit = true;
    zk_changed_event->set();
    auto temp_thread = std::move(thread);
    lock.unlock();
    temp_thread.join();
}


ConfigReloader::~ConfigReloader()
{
    try
    {
        stop();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}


void ConfigReloader::run()
{
    setThreadName("ConfigReloader");

    while (true)
    {
        try
        {
            bool zk_changed = zk_changed_event->tryWait(std::chrono::milliseconds(reload_interval).count());
            if (quit)
                return;

            reloadIfNewer(zk_changed, /* throw_on_error = */ false, /* fallback_to_preprocessed = */ false, /* initial_loading = */ false);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            std::this_thread::sleep_for(reload_interval);
        }
    }
}

void ConfigReloader::reloadIfNewer(bool force, bool throw_on_error, bool fallback_to_preprocessed, bool initial_loading)
{
    std::lock_guard lock(reload_mutex);

    FilesChangesTracker new_files = getNewFileList();
    if (force || need_reload_from_zk || new_files.isDifferOrNewerThan(files))
    {
        ConfigProcessor config_processor(config_path);
        ConfigProcessor::LoadedConfig loaded_config;

        LOG_DEBUG(log, "Loading config '{}'", config_path);

        try
        {
            loaded_config = config_processor.loadConfig(/* allow_zk_includes = */ true);
            if (loaded_config.has_zk_includes)
                loaded_config = config_processor.loadConfigWithZooKeeperIncludes(
                    zk_node_cache, zk_changed_event, fallback_to_preprocessed);
        }
        catch (const Coordination::Exception & e)
        {
            if (Coordination::isHardwareError(e.code))
                need_reload_from_zk = true;

            if (throw_on_error)
                throw;

            tryLogCurrentException(log, "ZooKeeper error when loading config from '" + config_path + "'");
            return;
        }
        catch (...)
        {
            if (throw_on_error)
                throw;

            tryLogCurrentException(log, "Error loading config from '" + config_path + "'");
            return;
        }
        config_processor.savePreprocessedConfig(loaded_config, preprocessed_dir);

        /** We should remember last modification time if and only if config was successfully loaded
         * Otherwise a race condition could occur during config files update:
         *  File is contain raw (and non-valid) data, therefore config is not applied.
         *  When file has been written (and contain valid data), we don't load new data since modification time remains the same.
         */
        if (!loaded_config.loaded_from_preprocessed)
        {
            files = std::move(new_files);
            need_reload_from_zk = false;
        }

        LOG_DEBUG(log, "Loaded config '{}', performing update on configuration", config_path);

        try
        {
            updater(loaded_config.configuration, initial_loading);
        }
        catch (...)
        {
            if (throw_on_error)
                throw;
            tryLogCurrentException(log, "Error updating configuration from '" + config_path + "' config.");
            return;
        }

        LOG_DEBUG(log, "Loaded config '{}', performed update on configuration", config_path);
    }
}

struct ConfigReloader::FileWithTimestamp
{
    std::string path;
    fs::file_time_type modification_time;

    explicit FileWithTimestamp(const std::string & path_)
        : path(path_), modification_time(fs::last_write_time(path_)) {}

    bool operator < (const FileWithTimestamp & rhs) const
    {
        return path < rhs.path;
    }

    static bool isTheSame(const FileWithTimestamp & lhs, const FileWithTimestamp & rhs)
    {
        return (lhs.modification_time == rhs.modification_time) && (lhs.path == rhs.path);
    }
};


void ConfigReloader::FilesChangesTracker::addIfExists(const std::string & path_to_add)
{
    if (!path_to_add.empty() && fs::exists(path_to_add))
        files.emplace(path_to_add);
}

bool ConfigReloader::FilesChangesTracker::isDifferOrNewerThan(const FilesChangesTracker & rhs)
{
    return (files.size() != rhs.files.size()) ||
        !std::equal(files.begin(), files.end(), rhs.files.begin(), FileWithTimestamp::isTheSame);
}

ConfigReloader::FilesChangesTracker ConfigReloader::getNewFileList() const
{
    FilesChangesTracker file_list;

    file_list.addIfExists(config_path);
    for (const std::string& path : extra_paths)
        file_list.addIfExists(path);

    for (const auto & merge_path : ConfigProcessor::getConfigMergeFiles(config_path))
        file_list.addIfExists(merge_path);

    return file_list;
}

}
