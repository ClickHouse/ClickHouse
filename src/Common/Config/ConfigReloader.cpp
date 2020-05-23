#include "ConfigReloader.h"

#include <Poco/Util/Application.h>
#include <Poco/File.h>
#include <common/logger_useful.h>
#include <Common/setThreadName.h>
#include "ConfigProcessor.h"


namespace DB
{

constexpr decltype(ConfigReloader::reload_interval) ConfigReloader::reload_interval;

ConfigReloader::ConfigReloader(
        const std::string & path_,
        const std::string & include_from_path_,
        const std::string & preprocessed_dir_,
        zkutil::ZooKeeperNodeCache && zk_node_cache_,
        const zkutil::EventPtr & zk_changed_event_,
        Updater && updater_,
        bool already_loaded)
    : path(path_), include_from_path(include_from_path_)
    , preprocessed_dir(preprocessed_dir_)
    , zk_node_cache(std::move(zk_node_cache_))
    , zk_changed_event(zk_changed_event_)
    , updater(std::move(updater_))
{
    if (!already_loaded)
        reloadIfNewer(/* force = */ true, /* throw_on_error = */ true, /* fallback_to_preprocessed = */ true);
}


void ConfigReloader::start()
{
    thread = ThreadFromGlobalPool(&ConfigReloader::run, this);
}


ConfigReloader::~ConfigReloader()
{
    try
    {
        quit = true;
        zk_changed_event->set();

        if (thread.joinable())
            thread.join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
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

            reloadIfNewer(zk_changed, /* throw_on_error = */ false, /* fallback_to_preprocessed = */ false);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            std::this_thread::sleep_for(reload_interval);
        }
    }
}

void ConfigReloader::reloadIfNewer(bool force, bool throw_on_error, bool fallback_to_preprocessed)
{
    std::lock_guard lock(reload_mutex);

    FilesChangesTracker new_files = getNewFileList();
    if (force || need_reload_from_zk || new_files.isDifferOrNewerThan(files))
    {
        ConfigProcessor config_processor(path);
        ConfigProcessor::LoadedConfig loaded_config;
        try
        {
            LOG_DEBUG(log, "Loading config '{}'", path);

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

            tryLogCurrentException(log, "ZooKeeper error when loading config from '" + path + "'");
            return;
        }
        catch (...)
        {
            if (throw_on_error)
                throw;

            tryLogCurrentException(log, "Error loading config from '" + path + "'");
            return;
        }
        config_processor.savePreprocessedConfig(loaded_config, preprocessed_dir);

        /** We should remember last modification time if and only if config was sucessfully loaded
         * Otherwise a race condition could occur during config files update:
         *  File is contain raw (and non-valid) data, therefore config is not applied.
         *  When file has been written (and contain valid data), we don't load new data since modification time remains the same.
         */
        if (!loaded_config.loaded_from_preprocessed)
        {
            files = std::move(new_files);
            need_reload_from_zk = false;
        }

        try
        {
            updater(loaded_config.configuration);
        }
        catch (...)
        {
            if (throw_on_error)
                throw;
            tryLogCurrentException(log, "Error updating configuration from '" + path + "' config.");
        }
    }
}

struct ConfigReloader::FileWithTimestamp
{
    std::string path;
    time_t modification_time;

    FileWithTimestamp(const std::string & path_, time_t modification_time_)
        : path(path_), modification_time(modification_time_) {}

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
    if (!path_to_add.empty() && Poco::File(path_to_add).exists())
        files.emplace(path_to_add, Poco::File(path_to_add).getLastModified().epochTime());
}

bool ConfigReloader::FilesChangesTracker::isDifferOrNewerThan(const FilesChangesTracker & rhs)
{
    return (files.size() != rhs.files.size()) ||
        !std::equal(files.begin(), files.end(), rhs.files.begin(), FileWithTimestamp::isTheSame);
}

ConfigReloader::FilesChangesTracker ConfigReloader::getNewFileList() const
{
    FilesChangesTracker file_list;

    file_list.addIfExists(path);
    file_list.addIfExists(include_from_path);

    for (const auto & merge_path : ConfigProcessor::getConfigMergeFiles(path))
        file_list.addIfExists(merge_path);

    return file_list;
}

}
