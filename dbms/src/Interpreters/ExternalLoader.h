#pragma once

#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/randomSeed.h>
#include <common/MultiVersion.h>
#include <common/logger_useful.h>
#include <Poco/Event.h>
#include <unistd.h>
#include <ctime>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <chrono>
#include <pcg_random.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/IExternalLoadable.h>


namespace DB
{

class Context;

struct ExternalLoaderUpdateSettings
{
    UInt64 check_period_sec = 5;
    UInt64 backoff_initial_sec = 5;
    /// 10 minutes
    UInt64 backoff_max_sec = 10 * 60;
};


/* External configuration structure.
 *
 * <external_group>
 *     <external_config>
 *         <external_name>name</external_name>
 *         ....
 *     </external_config>
 * </external_group>
 */
struct ExternalLoaderConfigSettings
{
    std::string external_config;
    std::string external_name;

    std::string path_setting_name;
};

/** Manages user-defined objects.
*    Monitors configuration file and automatically reloads objects in a separate thread.
*    The monitoring thread wakes up every @check_period_sec seconds and checks
*    modification time of objects' configuration file. If said time is greater than
*    @config_last_modified, the objects are created from scratch using configuration file,
*    possibly overriding currently existing objects with the same name (previous versions of
*    overridden objects will live as long as there are any users retaining them).
*
*    Apart from checking configuration file for modifications, each object
*    has a lifetime of its own and may be updated if it supportUpdates.
*    The time of next update is calculated by choosing uniformly a random number
*    distributed between lifetime.min_sec and lifetime.max_sec.
*    If either of lifetime.min_sec and lifetime.max_sec is zero, such object is never updated.
*/
class ExternalLoader
{
public:
    using Configuration = Poco::Util::AbstractConfiguration;
    using ObjectsMap = std::unordered_map<std::string, LoadableInfo>;

    /// Dictionaries will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    ExternalLoader(const Configuration & config,
                   const ExternalLoaderUpdateSettings & update_settings,
                   const ExternalLoaderConfigSettings & config_settings,
                   Logger * log, const std::string & loadable_object_name, bool throw_on_error);
    ~ExternalLoader();

    /// Forcibly reloads all loadable objects.
    void reload();

    /// Forcibly reloads specified loadable object.
    void reload(const std::string & name);

    LoadablePtr getLoadable(const std::string & name) const;

protected:
    virtual LoadablePtr create(const std::string & name, const Configuration & config,
                               const std::string & config_prefix) = 0;

    /// Direct access to objects.
    std::tuple<std::lock_guard<std::mutex>, const ObjectsMap &> getObjectsMap()
    {
        return std::make_tuple(std::lock_guard<std::mutex>(map_mutex), std::cref(loadable_objects));
    }

private:

    /// Protects only dictionaries map.
    mutable std::mutex map_mutex;

    /// Protects all data, currently used to avoid races between updating thread and SYSTEM queries
    mutable std::mutex all_mutex;

    using LoadablePtr = std::shared_ptr<IExternalLoadable>;
    struct LoadableInfo final
    {
        LoadablePtr loadable;
        std::string origin;
        std::exception_ptr exception;
    };

    struct FailedLoadableInfo final
    {
        std::unique_ptr<IExternalLoadable> loadable;
        std::chrono::system_clock::time_point next_attempt_time;
        UInt64 error_count;
    };

    /// name -> loadable.
    ObjectsMap loadable_objects;

    /** Here are loadable objects, that has been never loaded successfully.
      * They are also in 'loadable_objects', but with nullptr as 'loadable'.
      */
    std::unordered_map<std::string, FailedLoadableInfo> failed_loadable_objects;

    /** Both for dictionaries and failed_dictionaries.
      */
    std::unordered_map<std::string, std::chrono::system_clock::time_point> update_times;

    pcg64 rnd_engine{randomSeed()};

    const Configuration & config;
    const ExternalLoaderUpdateSettings & update_settings;
    const ExternalLoaderConfigSettings & config_settings;

    std::thread reloading_thread;
    Poco::Event destroy;

    Logger * log;
    /// Loadable object name to use in log messages.
    std::string object_name;

    std::unordered_map<std::string, Poco::Timestamp> last_modification_times;

    /// Check dictionaries definitions in config files and reload or/and add new ones if the definition is changed
    /// If loadable_name is not empty, load only loadable object with name loadable_name
    void reloadFromConfigFiles(bool throw_on_error, bool force_reload = false, const std::string & loadable_name = "");
    void reloadFromConfigFile(const std::string & config_path, bool throw_on_error, bool force_reload,
                              const std::string & loadable_name);

    /// Check config files and update expired loadable objects
    void reloadAndUpdate(bool throw_on_error = false);

    void reloadPeriodically();
};

}
