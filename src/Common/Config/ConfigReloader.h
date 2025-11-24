#pragma once

#include <Common/Config/ConfigProcessor.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <time.h>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <list>


namespace Poco { class Logger; }

namespace DB
{

/** Every two seconds checks configuration files for update.
  * If configuration is changed, then config will be reloaded by ConfigProcessor
  *  and the reloaded config will be applied via Updater functor.
  * It doesn't take into account changes of --config-file and <users_config>.
  */
class ConfigReloader
{
public:
    static constexpr auto DEFAULT_RELOAD_INTERVAL = std::chrono::milliseconds(2000);

    using Updater = std::function<void(ConfigurationPtr, bool)>;

    ConfigReloader(
        std::string_view path_,
        const std::vector<std::string>& extra_paths_,
        const std::string & preprocessed_dir,
        zkutil::ZooKeeperNodeCache && zk_node_cache,
        const zkutil::EventPtr & zk_changed_event,
        Updater && updater);

    ~ConfigReloader();

    /// Starts periodic reloading in the background thread.
    void start();

    /// Stops periodic reloading reloading in the background thread.
    /// This function is automatically called by the destructor.
    void stop();

    /// Reload immediately. For SYSTEM RELOAD CONFIG query.
    void reload() { reloadIfNewer(/* force */ true, /* throw_on_error */ true, /* fallback_to_preprocessed */ false, /* initial_loading = */ false); }

private:
    void run();

    std::optional<ConfigProcessor::LoadedConfig> reloadIfNewer(bool force, bool throw_on_error, bool fallback_to_preprocessed, bool initial_loading);

    struct FileWithTimestamp;

    struct FilesChangesTracker
    {
        std::set<FileWithTimestamp> files;

        void addIfExists(const std::string & path_to_add);
        bool isDifferOrNewerThan(const FilesChangesTracker & rhs);
    };

    FilesChangesTracker getNewFileList() const;

    LoggerPtr log = getLogger("ConfigReloader");

    std::string config_path;
    std::vector<std::string> extra_paths;

    std::string preprocessed_dir;
    FilesChangesTracker files;
    zkutil::ZooKeeperNodeCache zk_node_cache;
    bool need_reload_from_zk = false;
    zkutil::EventPtr zk_changed_event = std::make_shared<Poco::Event>();

    Updater updater;

    std::atomic<bool> quit{false};
    ThreadFromGlobalPool thread;

    std::chrono::milliseconds reload_interval = DEFAULT_RELOAD_INTERVAL;

    /// Locked inside reloadIfNewer.
    std::mutex reload_mutex;
};

}
