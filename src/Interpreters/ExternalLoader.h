#pragma once

#include <chrono>
#include <functional>
#include <unordered_map>
#include <common/types.h>
#include <Interpreters/IExternalLoadable.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>
#include <Common/ExternalLoaderStatus.h>


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
    std::string external_database;
    std::string external_uuid;
};

/** Interface for manage user-defined objects.
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
    using Status = ExternalLoaderStatus;

    using Duration = std::chrono::milliseconds;
    using TimePoint = std::chrono::system_clock::time_point;

    struct ObjectConfig
    {
        Poco::AutoPtr<Poco::Util::AbstractConfiguration> config;
        String key_in_config;
        String repository_name;
        bool from_temp_repository = false;
        String path;
    };

    struct LoadResult
    {
        Status status = Status::NOT_EXIST;
        String name;
        LoadablePtr object;
        TimePoint loading_start_time;
        TimePoint last_successful_update_time;
        Duration loading_duration;
        std::exception_ptr exception;
        std::shared_ptr<const ObjectConfig> config;
    };

    using LoadResults = std::vector<LoadResult>;

    template <typename T>
    static constexpr bool is_scalar_load_result_type = std::is_same_v<T, LoadResult> || std::is_same_v<T, LoadablePtr>;

    template <typename T>
    static constexpr bool is_vector_load_result_type = std::is_same_v<T, LoadResults> || std::is_same_v<T, Loadables>;

    ExternalLoader(const String & type_name_, Poco::Logger * log);
    virtual ~ExternalLoader();

    /// Adds a repository which will be used to read configurations from.
    scope_guard addConfigRepository(std::unique_ptr<IExternalLoaderConfigRepository> config_repository) const;

    void setConfigSettings(const ExternalLoaderConfigSettings & settings_);

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
    template <typename ReturnType = LoadResult, typename = std::enable_if_t<is_scalar_load_result_type<ReturnType>, void>>
    ReturnType getLoadResult(const String & name) const;

    using FilterByNameFunction = std::function<bool(const String &)>;

    /// Returns all the load results as a map.
    /// The function doesn't load anything, it just returns the current load results as is.
    template <typename ReturnType = LoadResults, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType getLoadResults() const { return getLoadResults<ReturnType>(FilterByNameFunction{}); }

    template <typename ReturnType = LoadResults, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType getLoadResults(const FilterByNameFunction & filter) const;

    /// Returns all loaded objects as a map.
    /// The function doesn't load anything, it just returns the current load results as is.
    Loadables getLoadedObjects() const;
    Loadables getLoadedObjects(const FilterByNameFunction & filter) const;

    /// Returns true if any object was loaded.
    bool hasLoadedObjects() const;
    size_t getNumberOfLoadedObjects() const;

    /// Returns true if there is no object.
    bool hasObjects() const { return getNumberOfObjects() == 0; }

    /// Returns number of objects.
    size_t getNumberOfObjects() const;

    static constexpr Duration NO_WAIT = Duration::zero();
    static constexpr Duration WAIT = Duration::max();

    /// Loads a specified object.
    /// The function does nothing if it's already loaded.
    /// The function doesn't throw an exception if it's failed to load.
    template <typename ReturnType = LoadablePtr, typename = std::enable_if_t<is_scalar_load_result_type<ReturnType>, void>>
    ReturnType tryLoad(const String & name, Duration timeout = WAIT) const;

    /// Loads objects by filter.
    /// The function does nothing for already loaded objects, it just returns them.
    /// The function doesn't throw an exception if it's failed to load something.
    template <typename ReturnType = Loadables, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType tryLoad(const FilterByNameFunction & filter, Duration timeout = WAIT) const;

    /// Loads all objects.
    /// The function does nothing for already loaded objects, it just returns them.
    /// The function doesn't throw an exception if it's failed to load something.
    template <typename ReturnType = Loadables, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType tryLoadAll(Duration timeout = WAIT) const { return tryLoad<ReturnType>(FilterByNameFunction{}, timeout); }

    /// Loads a specified object.
    /// The function does nothing if it's already loaded.
    /// The function throws an exception if it's failed to load.
    template <typename ReturnType = LoadablePtr, typename = std::enable_if_t<is_scalar_load_result_type<ReturnType>, void>>
    ReturnType load(const String & name) const;

    /// Loads objects by filter.
    /// The function does nothing for already loaded objects, it just returns them.
    /// The function throws an exception if it's failed to load something.
    template <typename ReturnType = Loadables, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType load(const FilterByNameFunction & filter) const;

    /// Loads all objects. Not recommended to use.
    /// The function does nothing for already loaded objects, it just returns them.
    /// The function throws an exception if it's failed to load something.
    template <typename ReturnType = Loadables, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType loadAll() const { return load<ReturnType>(FilterByNameFunction{}); }

    /// Loads or reloads a specified object.
    /// The function reloads the object if it's already loaded.
    /// The function throws an exception if it's failed to load or reload.
    template <typename ReturnType = LoadablePtr, typename = std::enable_if_t<is_scalar_load_result_type<ReturnType>, void>>
    ReturnType loadOrReload(const String & name) const;

    /// Loads or reloads objects by filter.
    /// The function reloads the objects which are already loaded.
    /// The function throws an exception if it's failed to load or reload something.
    template <typename ReturnType = Loadables, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType loadOrReload(const FilterByNameFunction & filter) const;

    /// Load or reloads all objects. Not recommended to use.
    /// The function throws an exception if it's failed to load or reload something.
    template <typename ReturnType = Loadables, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType loadOrReloadAll() const { return loadOrReload<ReturnType>(FilterByNameFunction{}); }

    /// Reloads objects by filter which were tried to load before (successfully or not).
    /// The function throws an exception if it's failed to load or reload something.
    template <typename ReturnType = Loadables, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType reloadAllTriedToLoad() const;

    /// Check if object with name exists in configuration
    bool has(const String & name) const;

    /// Reloads all config repositories.
    void reloadConfig() const;

    /// Reloads only a specified config repository.
    void reloadConfig(const String & repository_name) const;

    /// Reload only a specified path in a specified config repository.
    void reloadConfig(const String & repository_name, const String & path) const;

protected:
    virtual LoadablePtr create(const String & name, const Poco::Util::AbstractConfiguration & config, const String & key_in_config, const String & repository_name) const = 0;

private:
    void checkLoaded(const LoadResult & result, bool check_no_errors) const;
    void checkLoaded(const LoadResults & results, bool check_no_errors) const;

    Strings getAllTriedToLoadNames() const;

    LoadablePtr createObject(const String & name, const ObjectConfig & config, const LoadablePtr & previous_version) const;

    class LoadablesConfigReader;
    std::unique_ptr<LoadablesConfigReader> config_files_reader;

    class LoadingDispatcher;
    std::unique_ptr<LoadingDispatcher> loading_dispatcher;

    class PeriodicUpdater;
    std::unique_ptr<PeriodicUpdater> periodic_updater;

    const String type_name;
    Poco::Logger * log;
};

}
