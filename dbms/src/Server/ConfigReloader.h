#pragma once

#include <Common/ConfigProcessor/ConfigProcessor.h>
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

class Context;

/** Every two seconds checks configuration files for update.
  * If configuration is changed, then config will be reloaded by ConfigProcessor
  *  and the reloaded config will be applied via Updater functor.
  * It doesn't take into account changes of --config-file, <users_config> and <include_from> parameters.
  */
class ConfigReloader
{
public:
    using Updater = std::function<void(ConfigurationPtr)>;

    /** include_from_path is usually /etc/metrika.xml (i.e. value of <include_from> tag)
      */
    ConfigReloader(
            const std::string & path,
            const std::string & include_from_path,
            zkutil::ZooKeeperNodeCache && zk_node_cache,
            Updater && updater,
            bool already_loaded);

    ~ConfigReloader();

private:
    void run();

    void reloadIfNewer(bool force, bool throw_on_error, bool fallback_to_preprocessed);

    struct FileWithTimestamp;

    struct FilesChangesTracker
    {
        std::set<FileWithTimestamp> files;

        void addIfExists(const std::string & path);
        bool isDifferOrNewerThan(const FilesChangesTracker & rhs);
    };

    FilesChangesTracker getNewFileList() const;

private:

    static constexpr auto reload_interval = std::chrono::seconds(2);

    Poco::Logger * log = &Logger::get("ConfigReloader");

    std::string path;
    std::string include_from_path;
    FilesChangesTracker files;
    zkutil::ZooKeeperNodeCache zk_node_cache;

    Updater updater;

    std::atomic<bool> quit{false};
    std::thread thread;
};

}
