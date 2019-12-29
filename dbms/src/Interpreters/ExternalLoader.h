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
        Status status = Status::NOT_EXIST;
        String name;
        LoadablePtr object;
        String origin;
        TimePoint loading_start_time;
        Duration loading_duration;
        std::exception_ptr exception;
        std::string repository_name;
    };

    using LoadResults = std::vector<LoadResult>;

    template <typename T>
    static constexpr bool is_scalar_load_result_type = std::is_same_v<T, LoadResult> || std::is_same_v<T, LoadablePtr>;

    template <typename T>
    static constexpr bool is_vector_load_result_type = std::is_same_v<T, LoadResults> || std::is_same_v<T, Loadables>;

    ExternalLoader(const String & type_name_, Logger * log);
    virtual ~ExternalLoader();

    /// Adds a repository which will be used to read configurations from.
    void addConfigRepository(
        const std::string & repository_name,
        std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
        const ExternalLoaderConfigSettings & config_settings);

    /// Removes a repository which were used to read configurations.
    std::unique_ptr<IExternalLoaderConfigRepository> removeConfigRepository(const std::string & repository_name);

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
    ReturnType getCurrentLoadResult(const String & name) const;

    using FilterByNameFunction = std::function<bool(const String &)>;

    /// Returns all the load results as a map.
    /// The function doesn't load anything, it just returns the current load results as is.
    template <typename ReturnType = LoadResults, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType getCurrentLoadResults() const { return getCurrentLoadResults<ReturnType>(alwaysTrue); }

    template <typename ReturnType = LoadResults, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType getCurrentLoadResults(const FilterByNameFunction & filter) const;

    /// Returns all loaded objects as a map.
    /// The function doesn't load anything, it just returns the current load results as is.
    Loadables getCurrentlyLoadedObjects() const;
    Loadables getCurrentlyLoadedObjects(const FilterByNameFunction & filter) const;

    /// Returns true if any object was loaded.
    bool hasCurrentlyLoadedObjects() const;
    size_t getNumberOfCurrentlyLoadedObjects() const;

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
    ReturnType tryLoadAll(Duration timeout = WAIT) const { return tryLoad<ReturnType>(alwaysTrue, timeout); }

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
    ReturnType loadAll() const { return load<ReturnType>(alwaysTrue); }

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
    ReturnType loadOrReloadAll() const { return loadOrReload<ReturnType>(alwaysTrue); }

    /// Reloads objects by filter which were tried to load before (successfully or not).
    /// The function throws an exception if it's failed to load or reload something.
    template <typename ReturnType = Loadables, typename = std::enable_if_t<is_vector_load_result_type<ReturnType>, void>>
    ReturnType reloadAllTriedToLoad() const;

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

    static bool alwaysTrue(const String &) { return true; }
    Strings getAllTriedToLoadNames() const;

    struct ObjectConfig;
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

String toString(ExternalLoader::Status status);
std::ostream & operator<<(std::ostream & out, ExternalLoader::Status status);

}
