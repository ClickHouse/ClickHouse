#pragma once

#include <common/logger_useful.h>
#include <Poco/Event.h>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <tuple>
#include <Interpreters/IExternalLoadable.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Core/Types.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <Common/ThreadPool.h>


namespace DB
{

struct ExternalLoaderUpdateSettings
{
    UInt64 check_period_sec = 5;
    UInt64 backoff_initial_sec = 5;
    /// 10 minutes
    UInt64 backoff_max_sec = 10 * 60;

    ExternalLoaderUpdateSettings() = default;
    ExternalLoaderUpdateSettings(UInt64 check_period_sec, UInt64 backoff_initial_sec, UInt64 backoff_max_sec)
            : check_period_sec(check_period_sec),
              backoff_initial_sec(backoff_initial_sec),
              backoff_max_sec(backoff_max_sec) {}
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
*    The monitoring thread wakes up every 'check_period_sec' seconds and checks
*    modification time of objects' configuration file. If said time is greater than
*    'config_last_modified', the objects are created from scratch using configuration file,
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
    using LoadablePtr = std::shared_ptr<IExternalLoadable>;

private:
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

public:
    using Configuration = Poco::Util::AbstractConfiguration;
    using ObjectsMap = std::unordered_map<std::string, LoadableInfo>;

    /// Call init() after constructing the instance of any derived class.
    ExternalLoader(const Configuration & config_main,
                   const ExternalLoaderUpdateSettings & update_settings,
                   const ExternalLoaderConfigSettings & config_settings,
                   std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
                   Logger * log, const std::string & loadable_object_name);
    virtual ~ExternalLoader();

    /// Should be called after creating an instance of a derived class.
    /// Loads the objects immediately and starts a separate thread to update them once in each 'reload_period' seconds.
    /// This function does nothing if called again.
    void init(bool throw_on_error);

    /// Forcibly reloads all loadable objects.
    void reload();

    /// Forcibly reloads specified loadable object.
    void reload(const std::string & name);

    LoadablePtr getLoadable(const std::string & name) const;
    LoadablePtr tryGetLoadable(const std::string & name) const;

protected:
    virtual std::unique_ptr<IExternalLoadable> create(const std::string & name, const Configuration & config,
                                                      const std::string & config_prefix) const = 0;

    class LockedObjectsMap
    {
    public:
        LockedObjectsMap(std::mutex & mutex, const ObjectsMap & objects_map) : lock(mutex), objects_map(objects_map) {}
        const ObjectsMap & get() { return objects_map; }
    private:
        std::unique_lock<std::mutex> lock;
        const ObjectsMap & objects_map;
    };

    /// Direct access to objects.
    LockedObjectsMap getObjectsMap() const;

private:
    std::once_flag is_initialized_flag;

    /// Protects only objects map.
    /** Reading and assignment of "loadable" should be done under mutex.
      * Creating new versions of "loadable" should not be done under mutex.
      */
    mutable std::mutex map_mutex;

    /// Protects all data, currently used to avoid races between updating thread and SYSTEM queries.
    /// The mutex is recursive because creating of objects might be recursive, i.e.
    /// creating objects might cause creating other objects.
    mutable std::recursive_mutex all_mutex;

    /// name -> loadable.
    mutable ObjectsMap loadable_objects;

    struct LoadableCreationInfo
    {
        std::string name;
        Poco::AutoPtr<Poco::Util::AbstractConfiguration> config;
        std::string config_path;
        std::string config_prefix;
    };

    /// Objects which should be reloaded soon.
    mutable std::unordered_map<std::string, LoadableCreationInfo> objects_to_reload;

    /// Here are loadable objects, that has been never loaded successfully.
    /// They are also in 'loadable_objects', but with nullptr as 'loadable'.
    mutable std::unordered_map<std::string, FailedLoadableInfo> failed_loadable_objects;

    /// Both for loadable_objects and failed_loadable_objects.
    mutable std::unordered_map<std::string, std::chrono::system_clock::time_point> update_times;

    std::unordered_map<std::string, std::unordered_set<std::string>> loadable_objects_defined_in_config;

    mutable pcg64 rnd_engine{randomSeed()};

    const Configuration & config_main;
    const ExternalLoaderUpdateSettings & update_settings;
    const ExternalLoaderConfigSettings & config_settings;

    std::unique_ptr<IExternalLoaderConfigRepository> config_repository;

    ThreadFromGlobalPool reloading_thread;
    Poco::Event destroy;

    Logger * log;
    /// Loadable object name to use in log messages.
    std::string object_name;

    std::unordered_map<std::string, Poco::Timestamp> last_modification_times;

    void initImpl(bool throw_on_error);

    /// Check objects definitions in config files and reload or/and add new ones if the definition is changed
    /// If loadable_name is not empty, load only loadable object with name loadable_name
    void reloadFromConfigFiles(bool throw_on_error, bool force_reload = false, const std::string & loadable_name = "");
    void reloadFromConfigFile(const std::string & config_path, const bool force_reload, const std::string & loadable_name);

    /// Check config files and update expired loadable objects
    void reloadAndUpdate(bool throw_on_error = false);

    void reloadPeriodically();

    void finishReload(const std::string & loadable_name, bool throw_on_error) const;
    void finishAllReloads(bool throw_on_error) const;
    void finishReloadImpl(const LoadableCreationInfo & creation_info, bool throw_on_error) const;

    LoadablePtr getLoadableImpl(const std::string & name, bool throw_on_error) const;
};

}
