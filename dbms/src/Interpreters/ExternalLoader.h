#pragma once

#include <chrono>
#include <functional>
#include <unordered_map>
#include <Core/Types.h>
#include <Interpreters/IExternalLoadable.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <common/logger_useful.h>


namespace DB
{
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
  * Monitors configuration file and automatically reloads objects in separate threads.
  * The monitoring thread wakes up every 'check_period_sec' seconds and checks
  * modification time of objects' configuration file. If said time is greater than
  * 'config_last_modified', the objects are created from scratch using configuration file,
  * possibly overriding currently existing objects with the same name (previous versions of
  * overridden objects will live as long as there are any users retaining them).
  *
  * Apart from checking configuration file for modifications, each object
  * has a lifetime of its own and may be updated if it supportUpdates.
  * The time of next update is calculated by choosing uniformly a random number
  * distributed between lifetime.min_sec and lifetime.max_sec.
  * If either of lifetime.min_sec and lifetime.max_sec is zero, such object is never updated.
  */
class ExternalLoader
{
public:
    using LoadablePtr = std::shared_ptr<const IExternalLoadable>;
    using Loadables = std::vector<LoadablePtr>;

    enum class Status
    {
        NOT_LOADED, /// Object hasn't been tried to load. This is an initial state.
        LOADED, /// Object has been loaded successfully.
        FAILED, /// Object has been failed to load.
        LOADING, /// Object is being loaded right now for the first time.
        FAILED_AND_RELOADING, /// Object was failed to load before and it's being reloaded right now.
        LOADED_AND_RELOADING, /// Object was loaded successfully before and it's being reloaded right now.
        NOT_EXIST, /// Object with this name wasn't found in the configuration.
    };

    static std::vector<std::pair<String, Int8>> getStatusEnumAllPossibleValues();

    using Duration = std::chrono::milliseconds;
    using TimePoint = std::chrono::system_clock::time_point;

    struct LoadResult
    {
        LoadResult(Status status_) : status(status_) {}
        Status status;
        LoadablePtr object;
        String origin;
        TimePoint loading_start_time;
        Duration loading_duration;
        std::exception_ptr exception;
    };

    using LoadResults = std::vector<std::pair<String, LoadResult>>;

    ExternalLoader(const Poco::Util::AbstractConfiguration & main_config, const String & type_name_, Logger * log);
    virtual ~ExternalLoader();

    /// Adds a repository which will be used to read configurations from.
    void addConfigRepository(
        std::unique_ptr<IExternalLoaderConfigRepository> config_repository, const ExternalLoaderConfigSettings & config_settings);

    /// Sets whether all the objects from the configuration should be always loaded (even those which are never used).
    void enableAlwaysLoadEverything(bool enable);

    /// Sets whether the objects should be loaded asynchronously, each loading in a new thread (from the thread pool).
    void enableAsyncLoading(bool enable);

    /// Sets settings for periodic updates.
    void enablePeriodicUpdates(bool enable);

    /// Returns the status of the object.
    /// If the object has not been loaded yet then the function returns Status::NOT_LOADED.
    /// If the specified name isn't found in the configuration then the function returns Status::NOT_EXIST.
    Status getCurrentStatus(const String & name) const;

    /// Returns the result of loading the object.
    /// The function doesn't load anything, it just returns the current load result as is.
    LoadResult getCurrentLoadResult(const String & name) const;

    using FilterByNameFunction = std::function<bool(const String &)>;

    /// Returns all the load results as a map.
    /// The function doesn't load anything, it just returns the current load results as is.
    LoadResults getCurrentLoadResults() const;
    LoadResults getCurrentLoadResults(const FilterByNameFunction & filter_by_name) const;

    /// Returns all loaded objects as a map.
    /// The function doesn't load anything, it just returns the current load results as is.
    Loadables getCurrentlyLoadedObjects() const;
    Loadables getCurrentlyLoadedObjects(const FilterByNameFunction & filter_by_name) const;
    size_t getNumberOfCurrentlyLoadedObjects() const;

    /// Returns true if any object was loaded.
    bool hasCurrentlyLoadedObjects() const;

    static constexpr Duration NO_TIMEOUT = Duration::max();

    /// Starts loading of a specified object.
    void load(const String & name) const;

    /// Tries to finish loading of a specified object during the timeout.
    /// Returns nullptr if the loading is unsuccessful or if there is no such object.
    void load(const String & name, LoadablePtr & loaded_object, Duration timeout = NO_TIMEOUT) const;
    void load(const String & name, LoadResult & load_result, Duration timeout = NO_TIMEOUT) const;
    LoadablePtr loadAndGet(const String & name, Duration timeout = NO_TIMEOUT) const { LoadablePtr object; load(name, object, timeout); return object; }
    LoadablePtr tryGetLoadable(const String & name) const { return loadAndGet(name); }

    /// Tries to finish loading of a specified object during the timeout.
    /// Throws an exception if the loading is unsuccessful or if there is no such object.
    void loadStrict(const String & name, LoadablePtr & loaded_object) const;
    void loadStrict(const String & name, LoadResult & load_result) const;
    LoadablePtr loadAndGetStrict(const String & name) const { LoadablePtr object; loadStrict(name, object); return object; }
    LoadablePtr getLoadable(const String & name) const { return loadAndGetStrict(name); }

    /// Tries to start loading of the objects for which the specified function returns true.
    void load(const FilterByNameFunction & filter_by_name) const;

    /// Tries to finish loading of the objects for which the specified function returns true.
    void load(const FilterByNameFunction & filter_by_name, Loadables & loaded_objects, Duration timeout = NO_TIMEOUT) const;
    void load(const FilterByNameFunction & filter_by_name, LoadResults & load_results, Duration timeout = NO_TIMEOUT) const;
    Loadables loadAndGet(const FilterByNameFunction & filter_by_name, Duration timeout = NO_TIMEOUT) const { Loadables loaded_objects; load(filter_by_name, loaded_objects, timeout); return loaded_objects; }

    /// Starts loading of all the objects.
    void load() const;

    /// Tries to finish loading of all the objects during the timeout.
    void load(Loadables & loaded_objects, Duration timeout = NO_TIMEOUT) const;
    void load(LoadResults & load_results, Duration timeout = NO_TIMEOUT) const;

    /// Starts reloading of a specified object.
    /// `load_never_loading` specifies what to do if the object has never been loading before.
    /// The function can either skip it (false) or load for the first time (true).
    void reload(const String & name, bool load_never_loading = false);

    /// Starts reloading of the objects for which the specified function returns true.
    /// `load_never_loading` specifies what to do with the objects which have never been loading before.
    /// The function can either skip them (false) or load for the first time (true).
    void reload(const FilterByNameFunction & filter_by_name, bool load_never_loading = false);

    /// Starts reloading of all the objects.
    /// `load_never_loading` specifies what to do with the objects which have never been loading before.
    /// The function can either skip them (false) or load for the first time (true).
    void reload(bool load_never_loading = false);

protected:
    virtual LoadablePtr create(const String & name, const Poco::Util::AbstractConfiguration & config, const String & key_in_config) const = 0;

private:
    struct ObjectConfig;

    LoadablePtr createObject(const String & name, const ObjectConfig & config, bool config_changed, const LoadablePtr & previous_version) const;
    TimePoint calculateNextUpdateTime(const LoadablePtr & loaded_object, size_t error_count) const;

    class ConfigFilesReader;
    std::unique_ptr<ConfigFilesReader> config_files_reader;

    class LoadingDispatcher;
    std::unique_ptr<LoadingDispatcher> loading_dispatcher;

    class PeriodicUpdater;
    std::unique_ptr<PeriodicUpdater> periodic_updater;

    const String type_name;
};

String toString(ExternalLoader::Status status);
std::ostream & operator<<(std::ostream & out, ExternalLoader::Status status);

}
