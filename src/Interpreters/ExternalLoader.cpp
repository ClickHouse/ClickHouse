#include "ExternalLoader.h"

#include <mutex>
#include <pcg_random.hpp>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ThreadPool.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <Common/StatusInfo.h>
#include <base/chrono_io.h>
#include <Common/scope_guard_safe.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <unordered_set>


namespace CurrentStatusInfo
{
    extern const Status DictionaryStatus;
}


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARIES_WAS_NOT_LOADED;
}


namespace
{
    template <typename ReturnType>
    ReturnType convertTo(ExternalLoader::LoadResult result)
    {
        if constexpr (std::is_same_v<ReturnType, ExternalLoader::LoadResult>)
            return result;
        else
        {
            static_assert(std::is_same_v<ReturnType, ExternalLoader::LoadablePtr>);
            return std::move(result.object);
        }
    }

    template <typename ReturnType>
    ReturnType convertTo(ExternalLoader::LoadResults results)
    {
        if constexpr (std::is_same_v<ReturnType, ExternalLoader::LoadResults>)
            return results;
        else
        {
            static_assert(std::is_same_v<ReturnType, ExternalLoader::Loadables>);
            ExternalLoader::Loadables objects;
            objects.reserve(results.size());
            for (auto && result : results)
            {
                if (auto object = std::move(result.object))
                    objects.push_back(std::move(object));
            }
            return objects;
        }
    }

    template <typename ReturnType>
    ReturnType notExists(const String & name)
    {
        if constexpr (std::is_same_v<ReturnType, ExternalLoader::LoadResult>)
        {
            ExternalLoader::LoadResult res;
            res.name = name;
            return res;
        }
        else
        {
            static_assert(std::is_same_v<ReturnType, ExternalLoader::LoadablePtr>);
            return nullptr;
        }
    }


    /// Lock mutex only in async mode
    /// In other case does nothing
    struct LoadingGuardForAsyncLoad
    {
        std::unique_lock<std::mutex> lock;
        LoadingGuardForAsyncLoad(bool async, std::mutex & mutex)
        {
            if (async)
                lock = std::unique_lock(mutex);
        }
    };
}


/** Reads configurations from configuration repository and parses it.
  */
class ExternalLoader::LoadablesConfigReader : private boost::noncopyable
{
public:
    LoadablesConfigReader(const String & type_name_, Poco::Logger * log_)
        : type_name(type_name_), log(log_)
    {
    }
    ~LoadablesConfigReader() = default;

    using Repository = IExternalLoaderConfigRepository;

    void addConfigRepository(std::unique_ptr<Repository> repository)
    {
        std::lock_guard lock{mutex};
        auto * ptr = repository.get();
        repositories.emplace(ptr, RepositoryInfo{std::move(repository), {}});
        need_collect_object_configs = true;
    }

    void removeConfigRepository(Repository * repository)
    {
        std::lock_guard lock{mutex};
        auto it = repositories.find(repository);
        if (it == repositories.end())
            return;
        repositories.erase(it);
        need_collect_object_configs = true;
    }

    void setConfigSettings(const ExternalLoaderConfigSettings & settings_)
    {
        std::lock_guard lock{mutex};
        settings = settings_;
    }

    struct ObjectConfigs
    {
        std::unordered_map<String /* object's name */, std::shared_ptr<const ObjectConfig>> configs_by_name;
        size_t counter = 0;
    };

    using ObjectConfigsPtr = std::shared_ptr<const ObjectConfigs>;

    /// Reads all repositories.
    ObjectConfigsPtr read()
    {
        std::lock_guard lock(mutex);
        readRepositories();
        collectObjectConfigs();
        return object_configs;
    }

    /// Reads only a specified repository.
    /// This functions checks only a specified repository but returns configs from all repositories.
    ObjectConfigsPtr read(const String & repository_name)
    {
        std::lock_guard lock(mutex);
        readRepositories(repository_name);
        collectObjectConfigs();
        return object_configs;
    }

    /// Reads only a specified path from a specified repository.
    /// This functions checks only a specified repository but returns configs from all repositories.
    ObjectConfigsPtr read(const String & repository_name, const String & path)
    {
        std::lock_guard lock(mutex);
        readRepositories(repository_name, path);
        collectObjectConfigs();
        return object_configs;
    }

private:
    struct FileInfo
    {
        Poco::Timestamp last_update_time = 0;
        bool in_use = true; // Whether the `FileInfo` should be destroyed because the correspondent file is deleted.
        Poco::AutoPtr<Poco::Util::AbstractConfiguration> file_contents; // Parsed contents of the file.
        std::unordered_map<String /* object name */, String /* key in file_contents */> objects;
    };

    struct RepositoryInfo
    {
        std::unique_ptr<Repository> repository;
        std::unordered_map<String /* path */, FileInfo> files;
    };

    /// Reads the repositories.
    /// Checks last modification times of files and read those files which are new or changed.
    void readRepositories(const std::optional<String> & only_repository_name = {}, const std::optional<String> & only_path = {})
    {
        for (auto & [repository, repository_info] : repositories)
        {
            if (only_repository_name && (repository->getName() != *only_repository_name))
                continue;

            for (auto & file_info : repository_info.files | boost::adaptors::map_values)
                file_info.in_use = false;

            Strings existing_paths;
            if (only_path)
            {
                if (repository->exists(*only_path))
                    existing_paths.push_back(*only_path);
            }
            else
                boost::copy(repository->getAllLoadablesDefinitionNames(), std::back_inserter(existing_paths));

            for (const auto & path : existing_paths)
            {
                auto it = repository_info.files.find(path);
                if (it != repository_info.files.end())
                {
                    FileInfo & file_info = it->second;
                    if (readFileInfo(file_info, *repository, path))
                        need_collect_object_configs = true;
                }
                else
                {
                    FileInfo file_info;
                    if (readFileInfo(file_info, *repository, path))
                    {
                        repository_info.files.emplace(path, std::move(file_info));
                        need_collect_object_configs = true;
                    }
                }
            }

            Strings deleted_paths;
            for (auto & [path, file_info] : repository_info.files)
            {
                if (file_info.in_use)
                    continue;

                if (only_path && (*only_path != path))
                    continue;

                deleted_paths.emplace_back(path);
            }

            if (!deleted_paths.empty())
            {
                for (const String & deleted_path : deleted_paths)
                    repository_info.files.erase(deleted_path);
                need_collect_object_configs = true;
            }
        }
    }

    /// Reads a file, returns true if the file is new or changed.
    bool readFileInfo(
        FileInfo & file_info,
        IExternalLoaderConfigRepository & repository,
        const String & path) const
    {
        try
        {
            if (path.empty() || !repository.exists(path))
            {
                LOG_WARNING(log, "Config file '{}' does not exist", path);
                return false;
            }

            auto update_time_from_repository = repository.getUpdateTime(path);

            // We can't count on that the mtime increases or that it has
            // a particular relation to system time, so just check for strict
            // equality.
            // Note that on 1.x versions on Poco, the granularity of update
            // time is one second, so the window where we can miss the changes
            // is that wide (i.e. when we read the file and after that it
            // is updated, but in the same second).
            // The solution to this is probably switching to std::filesystem
            // -- the work is underway to do so.
            if (update_time_from_repository == file_info.last_update_time)
            {
                file_info.in_use = true;
                return false;
            }

            LOG_TRACE(log, "Loading config file '{}'.", path);
            file_info.file_contents = repository.load(path);
            auto & file_contents = *file_info.file_contents;

            /// get all objects' definitions
            Poco::Util::AbstractConfiguration::Keys keys;
            file_contents.keys(keys);

            /// for each object defined in repositories
            std::unordered_map<String, String> objects;
            for (const auto & key : keys)
            {
                if (!startsWith(key, settings.external_config))
                {
                    if (!startsWith(key, "comment") && !startsWith(key, "include_from"))
                        LOG_WARNING(log, "{}: file contains unknown node '{}', expected '{}'", path, key, settings.external_config);
                    continue;
                }

                /// Use uuid as name if possible
                String object_uuid = file_contents.getString(key + "." + settings.external_uuid, "");
                String object_name;
                if (object_uuid.empty())
                    object_name = file_contents.getString(key + "." + settings.external_name);
                else
                    object_name = object_uuid;
                if (object_name.empty())
                {
                    LOG_WARNING(log, "{}: node '{}' defines {} with an empty name. It's not allowed", path, key, type_name);
                    continue;
                }

                if (object_uuid.empty())
                {
                    String database;
                    if (!settings.external_database.empty())
                        database = file_contents.getString(key + "." + settings.external_database, "");
                    if (!database.empty())
                        object_name = database + "." + object_name;
                }

                objects.emplace(object_name, key);
            }

            file_info.objects = std::move(objects);
            file_info.last_update_time = update_time_from_repository;
            file_info.in_use = true;
            return true;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to load config file '" + path + "'");
            return false;
        }
    }

    /// Builds a map of current configurations of objects.
    void collectObjectConfigs()
    {
        if (!need_collect_object_configs)
            return;
        need_collect_object_configs = false;

        // Generate new result.
        auto new_configs = std::make_shared<ObjectConfigs>();

        for (const auto & [repository, repository_info] : repositories)
        {
            for (const auto & [path, file_info] : repository_info.files)
            {
                for (const auto & [object_name, key_in_config] : file_info.objects)
                {
                    auto already_added_it = new_configs->configs_by_name.find(object_name);
                    if (already_added_it == new_configs->configs_by_name.end())
                    {
                        auto new_config = std::make_shared<ObjectConfig>();
                        new_config->config = file_info.file_contents;
                        new_config->key_in_config = key_in_config;
                        new_config->repository_name = repository->getName();
                        new_config->from_temp_repository = repository->isTemporary();
                        new_config->path = path;
                        new_configs->configs_by_name.emplace(object_name, std::move(new_config));
                    }
                    else
                    {
                        const auto & already_added = already_added_it->second;
                        if (!already_added->from_temp_repository && !repository->isTemporary())
                        {
                            if (path == already_added->path && repository->getName() == already_added->repository_name)
                                LOG_WARNING(log, "{} '{}' is found twice in the same file '{}'",
                                    type_name, object_name, path);
                            else
                                LOG_WARNING(log, "{} '{}' is found both in file '{}' and '{}'",
                                    type_name, object_name, already_added->path, path);
                        }
                    }
                }
            }
        }

        new_configs->counter = counter++;
        object_configs = new_configs;
    }

    const String type_name;
    Poco::Logger * log;

    std::mutex mutex;
    ExternalLoaderConfigSettings settings;
    std::unordered_map<Repository *, RepositoryInfo> repositories;
    ObjectConfigsPtr object_configs;
    bool need_collect_object_configs = false;
    size_t counter = 0;
};


/** Manages loading and reloading objects. Uses configurations from the class LoadablesConfigReader.
  * Supports parallel loading.
  */
class ExternalLoader::LoadingDispatcher : private boost::noncopyable
{
public:
    /// Called to load or reload an object.
    using CreateObjectFunction = std::function<LoadablePtr(
        const String & /* name */, const ObjectConfig & /* config */, const LoadablePtr & /* previous_version */)>;

    LoadingDispatcher(
        const CreateObjectFunction & create_object_function_,
        const String & type_name_,
        Poco::Logger * log_)
        : create_object(create_object_function_)
        , type_name(type_name_)
        , log(log_)
    {
    }

    ~LoadingDispatcher()
    {
        std::unique_lock lock{mutex};
        infos.clear(); /// We clear this map to tell the threads that we don't want any load results anymore.

        /// Wait for all the threads to finish.
        while (!loading_threads.empty())
        {
            auto it = loading_threads.begin();
            auto thread = std::move(it->second);
            loading_threads.erase(it);
            lock.unlock();
            event.notify_all();
            thread.join();
            lock.lock();
        }
    }

    using ObjectConfigsPtr = LoadablesConfigReader::ObjectConfigsPtr;

    /// Sets new configurations for all the objects.
    void setConfiguration(const ObjectConfigsPtr & new_configs)
    {
        std::lock_guard lock{mutex};
        if (configs == new_configs)
            return;

        /// The following check prevents a race when two threads are trying to update configuration
        /// at almost the same time:
        /// 1) first thread reads a configuration (for example as a part of periodic updates)
        /// 2) second thread sets a new configuration (for example after executing CREATE DICTIONARY)
        /// 3) first thread sets the configuration it read in 1) and thus discards the changes made in 2).
        /// So we use `counter` here to ensure we exchange the current configuration only for a newer one.
        if (configs && (configs->counter >= new_configs->counter))
            return;

        configs = new_configs;

        std::vector<String> removed_names;
        for (auto & [name, info] : infos)
        {
            auto new_config_it = new_configs->configs_by_name.find(name);
            if (new_config_it == new_configs->configs_by_name.end())
            {
                removed_names.emplace_back(name);
            }
            else
            {
                const auto & new_config = new_config_it->second;
                bool config_is_same = isSameConfiguration(*info.config->config, info.config->key_in_config, *new_config->config, new_config->key_in_config);
                info.config = new_config;
                if (!config_is_same)
                {
                    if (info.triedToLoad())
                    {
                        /// The object has been tried to load before, so it is currently in use or was in use
                        /// and we should try to reload it with the new config.
                        LOG_TRACE(log, "Will reload '{}' because its configuration has been changed and there were attempts to load it before", name);
                        startLoading(info, true);
                    }
                }
            }
        }

        /// Insert to the map those objects which added to the new configuration.
        for (const auto & [name, config] : new_configs->configs_by_name)
        {
            if (infos.find(name) == infos.end())
            {
                Info & info = infos.emplace(name, Info{name, config}).first->second;
                if (always_load_everything)
                {
                    LOG_TRACE(log, "Will load '{}' because always_load_everything flag is set.", name);
                    startLoading(info);
                }
            }
        }

        /// Remove from the map those objects which were removed from the configuration.
        for (const String & name : removed_names)
        {
            if (auto it = infos.find(name); it != infos.end())
            {
                const auto & info = it->second;
                if (info.loaded() || info.isLoading())
                    LOG_TRACE(log, "Unloading '{}' because its configuration has been removed or detached", name);
                infos.erase(it);
            }
        }

        /// Maybe we have just added new objects which require to be loaded
        /// or maybe we have just removed object which were been loaded,
        /// so we should notify `event` to recheck conditions in load() and loadAll() now.
        event.notify_all();
    }

    /// Sets whether all the objects from the configuration should be always loaded (even if they aren't used).
    void enableAlwaysLoadEverything(bool enable)
    {
        std::lock_guard lock{mutex};
        if (always_load_everything == enable)
            return;

        always_load_everything = enable;

        if (enable)
        {
            /// Start loading all the objects which were not loaded yet.
            for (auto & [name, info] : infos)
                if (!info.triedToLoad())
                    startLoading(info);
        }
    }

    /// Sets whether the objects should be loaded asynchronously, each loading in a new thread (from the thread pool).
    void enableAsyncLoading(bool enable)
    {
        enable_async_loading = enable;
    }

    /// Returns the status of the object.
    /// If the object has not been loaded yet then the function returns Status::NOT_LOADED.
    /// If the specified name isn't found in the configuration then the function returns Status::NOT_EXIST.
    Status getCurrentStatus(const String & name) const
    {
        std::lock_guard lock{mutex};
        const Info * info = getInfo(name);
        if (!info)
            return Status::NOT_EXIST;
        return info->status();
    }

    /// Returns the load result of the object.
    template <typename ReturnType>
    ReturnType getLoadResult(const String & name) const
    {
        std::lock_guard lock{mutex};
        const Info * info = getInfo(name);
        if (!info)
            return notExists<ReturnType>(name);
        return info->getLoadResult<ReturnType>();
    }

    /// Returns all the load results as a map.
    /// The function doesn't load anything, it just returns the current load results as is.
    template <typename ReturnType>
    ReturnType getLoadResults(const FilterByNameFunction & filter) const
    {
        std::lock_guard lock{mutex};
        return collectLoadResults<ReturnType>(filter);
    }

    size_t getNumberOfLoadedObjects() const
    {
        std::lock_guard lock{mutex};
        size_t count = 0;
        for (const auto & name_and_info : infos)
        {
            const auto & info = name_and_info.second;
            if (info.loaded())
                ++count;
        }
        return count;
    }

    bool hasLoadedObjects() const
    {
        std::lock_guard lock{mutex};
        for (const auto & name_info : infos)
            if (name_info.second.loaded())
                return true;
        return false;
    }

    Strings getAllTriedToLoadNames() const
    {
        std::lock_guard lock{mutex};
        Strings names;
        for (const auto & [name, info] : infos)
            if (info.triedToLoad())
                names.push_back(name);
        return names;
    }

    size_t getNumberOfObjects() const
    {
        std::lock_guard lock{mutex};
        return infos.size();
    }

    /// Tries to load a specified object during the timeout.
    template <typename ReturnType>
    ReturnType tryLoad(const String & name, Duration timeout)
    {
        std::unique_lock lock{mutex};
        Info * info = loadImpl(name, timeout, false, lock);
        if (!info)
            return notExists<ReturnType>(name);
        return info->getLoadResult<ReturnType>();
    }

    template <typename ReturnType>
    ReturnType tryLoad(const FilterByNameFunction & filter, Duration timeout)
    {
        std::unique_lock lock{mutex};
        loadImpl(filter, timeout, false, lock);
        return collectLoadResults<ReturnType>(filter);
    }

    /// Tries to load or reload a specified object.
    template <typename ReturnType>
    ReturnType tryLoadOrReload(const String & name, Duration timeout)
    {
        std::unique_lock lock{mutex};
        Info * info = loadImpl(name, timeout, true, lock);
        if (!info)
            return notExists<ReturnType>(name);
        return info->getLoadResult<ReturnType>();
    }

    template <typename ReturnType>
    ReturnType tryLoadOrReload(const FilterByNameFunction & filter, Duration timeout)
    {
        std::unique_lock lock{mutex};
        loadImpl(filter, timeout, true, lock);
        return collectLoadResults<ReturnType>(filter);
    }

    bool has(const String & name) const
    {
        std::lock_guard lock{mutex};
        return infos.contains(name);
    }

    /// Starts reloading all the object which update time is earlier than now.
    /// The function doesn't touch the objects which were never tried to load.
    void reloadOutdated()
    {
        /// Iterate through all the objects and find loaded ones which should be checked if they need update.
        std::unordered_map<LoadablePtr, bool> should_update_map;
        {
            std::lock_guard lock{mutex};
            TimePoint now = std::chrono::system_clock::now();
            for (const auto & name_and_info : infos)
            {
                const auto & info = name_and_info.second;
                if ((now >= info.next_update_time) && !info.isLoading() && info.loaded())
                    should_update_map.emplace(info.object, info.failedToReload());
            }
        }

        /// Find out which of the loaded objects were modified.
        /// We couldn't perform these checks while we were building `should_update_map` because
        /// the `mutex` should be unlocked while we're calling the function object->isModified()
        for (auto & [object, should_update_flag] : should_update_map)
        {
            try
            {
                /// Maybe already true, if we have an exception
                if (!should_update_flag)
                    should_update_flag = object->isModified();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Could not check if " + type_name + " '" + object->getLoadableName() + "' was modified");
                /// Cannot check isModified, so update
                should_update_flag = true;
            }
        }

        /// Iterate through all the objects again and either start loading or just set `next_update_time`.
        {
            std::lock_guard lock{mutex};
            TimePoint now = std::chrono::system_clock::now();
            for (auto & [name, info] : infos)
            {
                if ((now >= info.next_update_time) && !info.isLoading())
                {
                    if (info.loaded())
                    {
                        auto it = should_update_map.find(info.object);
                        if (it == should_update_map.end())
                            continue; /// Object has been just loaded (it wasn't loaded while we were building the map `should_update_map`), so we don't have to reload it right now.

                        bool should_update_flag = it->second;
                        if (!should_update_flag)
                        {
                            info.next_update_time = calculateNextUpdateTime(info.object, info.error_count);
                            LOG_TRACE(log, "Object '{}' not modified, will not reload. Next update at {}", info.name, to_string(info.next_update_time));
                            continue;
                        }

                        /// Object was modified or it was failed to reload last time, so it should be reloaded.
                        startLoading(info);
                    }
                    else if (info.failed())
                    {
                        /// Object was never loaded successfully and should be reloaded.
                        startLoading(info);
                    }
                    LOG_TRACE(log, "Object '{}' is neither loaded nor failed, so it will not be reloaded as outdated.", info.name);
                }
            }
        }
    }

private:
    struct Info
    {
        Info(const String & name_, const std::shared_ptr<const ObjectConfig> & config_) : name(name_), config(config_) {}

        bool loaded() const { return object != nullptr; }
        bool failed() const { return !object && exception; }
        bool loadedOrFailed() const { return loaded() || failed(); }
        bool triedToLoad() const { return loaded() || failed() || isLoading(); }
        bool failedToReload() const { return loaded() && exception != nullptr; }
        bool isLoading() const { return loading_id > state_id; }

        Status status() const
        {
            if (object)
                return isLoading() ? Status::LOADED_AND_RELOADING : Status::LOADED;
            else if (exception)
                return isLoading() ? Status::FAILED_AND_RELOADING : Status::FAILED;
            else
                return isLoading() ? Status::LOADING : Status::NOT_LOADED;
        }

        Duration loadingDuration() const
        {
            if (isLoading())
                return std::chrono::duration_cast<Duration>(std::chrono::system_clock::now() - loading_start_time);
            return std::chrono::duration_cast<Duration>(loading_end_time - loading_start_time);
        }

        template <typename ReturnType>
        ReturnType getLoadResult() const
        {
            if constexpr (std::is_same_v<ReturnType, LoadResult>)
            {
                LoadResult result;
                result.name = name;
                result.status = status();
                result.object = object;
                result.exception = exception;
                result.loading_start_time = loading_start_time;
                result.last_successful_update_time = last_successful_update_time;
                result.loading_duration = loadingDuration();
                result.config = config;
                return result;
            }
            else
            {
                static_assert(std::is_same_v<ReturnType, ExternalLoader::LoadablePtr>);
                return object;
            }
        }

        String name;
        LoadablePtr object;
        std::shared_ptr<const ObjectConfig> config;
        TimePoint loading_start_time;
        TimePoint loading_end_time;
        TimePoint last_successful_update_time;
        size_t state_id = 0; /// Index of the current state of this `info`, this index is incremented every loading.
        size_t loading_id = 0; /// The value which will be stored in `state_id` after finishing the current loading.
        size_t error_count = 0; /// Numbers of errors since last successful loading.
        std::exception_ptr exception; /// Last error occurred.
        TimePoint next_update_time = TimePoint::max(); /// Time of the next update, `TimePoint::max()` means "never".
    };

    Info * getInfo(const String & name)
    {
        auto it = infos.find(name);
        if (it == infos.end())
            return nullptr;
        return &it->second;
    }

    const Info * getInfo(const String & name) const
    {
        auto it = infos.find(name);
        if (it == infos.end())
            return nullptr;
        return &it->second;
    }

    template <typename ReturnType>
    ReturnType collectLoadResults(const FilterByNameFunction & filter) const
    {
        ReturnType results;
        results.reserve(infos.size());
        for (const auto & [name, info] : infos)
        {
            if (!filter || filter(name))
            {
                auto result = info.template getLoadResult<typename ReturnType::value_type>();
                if constexpr (std::is_same_v<typename ReturnType::value_type, LoadablePtr>)
                {
                    if (!result)
                        continue;
                }
                results.emplace_back(std::move(result));
            }
        }
        return results;
    }

    Info * loadImpl(const String & name, Duration timeout, bool forced_to_reload, std::unique_lock<std::mutex> & lock)
    {
        std::optional<size_t> min_id;
        Info * info = nullptr;
        auto pred = [&]
        {
            info = getInfo(name);
            if (!info)
                return true; /// stop

            if (!min_id)
                min_id = getMinIDToFinishLoading(forced_to_reload);

            if (info->loading_id < min_id)
                startLoading(*info, forced_to_reload, *min_id);

            /// Wait for the next event if loading wasn't completed, or stop otherwise.
            return (info->state_id >= min_id);
        };

        if (timeout == WAIT)
            event.wait(lock, pred);
        else
            event.wait_for(lock, timeout, pred);

        return info;
    }

    void loadImpl(const FilterByNameFunction & filter, Duration timeout, bool forced_to_reload, std::unique_lock<std::mutex> & lock)
    {
        std::optional<size_t> min_id;
        auto pred = [&]
        {
            if (!min_id)
                min_id = getMinIDToFinishLoading(forced_to_reload);

            bool all_ready = true;
            for (auto & [name, info] : infos)
            {
                if (filter && !filter(name))
                    continue;

                if (info.loading_id < min_id)
                    startLoading(info, forced_to_reload, *min_id);

                all_ready &= (info.state_id >= min_id);
            }
            return all_ready;
        };

        if (timeout == WAIT)
            event.wait(lock, pred);
        else
            event.wait_for(lock, timeout, pred);
    }

    /// When state_id >= getMinIDToFinishLoading() the loading is considered as finished.
    size_t getMinIDToFinishLoading(bool forced_to_reload) const
    {
        if (forced_to_reload)
        {
            /// We need to force reloading, that's why we return next_id_counter here
            /// (because info.state_id < next_id_counter for any info).
            return next_id_counter;
        }

        /// The loading of an object can cause the loading of another object.
        /// We use the same "min_id" in this case to allows reloading multiple objects at once
        /// taking into account their dependencies.
        auto it = min_id_to_finish_loading_dependencies.find(std::this_thread::get_id());
        if (it != min_id_to_finish_loading_dependencies.end())
            return it->second;

        /// We just need the first loading to be finished, that's why we return 1 here
        /// (because info.state_id >= 1 since the first loading is finished, successfully or not).
        return 1;
    }

    void startLoading(Info & info, bool forced_to_reload = false, size_t min_id_to_finish_loading_dependencies_ = 1)
    {
        if (info.isLoading())
        {
            LOG_TRACE(log, "The object '{}' is already being loaded, force = {}.", info.name, forced_to_reload);

            if (!forced_to_reload)
            {
                return;
            }

            cancelLoading(info);
        }

        putBackFinishedThreadsToPool();

        /// All loadings have unique loading IDs.
        size_t loading_id = next_id_counter++;
        info.loading_id = loading_id;
        info.loading_start_time = std::chrono::system_clock::now();
        info.loading_end_time = TimePoint{};

        LOG_TRACE(log, "Will load the object '{}' {}, force = {}, loading_id = {}", info.name, (enable_async_loading ? std::string("in background") : "immediately"), forced_to_reload, info.loading_id);

        if (enable_async_loading)
        {
            /// Put a job to the thread pool for the loading.
            auto thread = ThreadFromGlobalPool{&LoadingDispatcher::doLoading, this, info.name, loading_id, forced_to_reload, min_id_to_finish_loading_dependencies_, true, CurrentThread::getGroup()};
            loading_threads.try_emplace(loading_id, std::move(thread));
        }
        else
        {
            /// Perform the loading immediately.
            doLoading(info.name, loading_id, forced_to_reload, min_id_to_finish_loading_dependencies_, false);
        }
    }

    void putBackFinishedThreadsToPool()
    {
        for (auto loading_id : recently_finished_loadings)
        {
            auto it = loading_threads.find(loading_id);
            if (it != loading_threads.end())
            {
                auto thread = std::move(it->second);
                loading_threads.erase(it);
                thread.join(); /// It's very likely that `thread` has already finished.
            }
        }
        recently_finished_loadings.clear();
    }

    static void cancelLoading(Info & info)
    {
        if (!info.isLoading())
            return;

        /// In fact we cannot actually CANCEL the loading (because it's possibly already being performed in another thread).
        /// But we can reset the `loading_id` and doLoading() will understand it as a signal to stop loading.
        info.loading_id = info.state_id;
        info.loading_end_time = std::chrono::system_clock::now();
    }

    /// Does the loading, possibly in the separate thread.
    void doLoading(const String & name, size_t loading_id, bool forced_to_reload, size_t min_id_to_finish_loading_dependencies_, bool async, ThreadGroupStatusPtr thread_group = {})
    {
        SCOPE_EXIT_SAFE(
            if (thread_group)
                CurrentThread::detachQueryIfNotDetached();
        );

        if (thread_group)
            CurrentThread::attachTo(thread_group);

        LOG_TRACE(log, "Start loading object '{}'", name);
        try
        {
            /// Prepare for loading.
            std::optional<Info> info;
            {
                LoadingGuardForAsyncLoad lock(async, mutex);
                info = prepareToLoadSingleObject(name, loading_id, min_id_to_finish_loading_dependencies_, lock);
                if (!info)
                {
                    LOG_TRACE(log, "Could not lock object '{}' for loading", name);
                    return;
                }
            }

            /// Previous version can be used as the base for new loading, enabling loading only part of data.
            auto previous_version_as_base_for_loading = info->object;
            if (forced_to_reload)
                previous_version_as_base_for_loading = nullptr; /// Need complete reloading, cannot use the previous version.

            /// Loading.
            auto [new_object, new_exception] = loadSingleObject(name, *info->config, previous_version_as_base_for_loading);
            if (!new_object && !new_exception)
                throw Exception("No object created and no exception raised for " + type_name, ErrorCodes::LOGICAL_ERROR);

            /// Saving the result of the loading.
            {
                LoadingGuardForAsyncLoad lock(async, mutex);
                saveResultOfLoadingSingleObject(name, loading_id, info->object, new_object, new_exception, info->error_count, lock);
                finishLoadingSingleObject(name, loading_id, lock);
            }
            event.notify_all();
        }
        catch (...)
        {
            LoadingGuardForAsyncLoad lock(async, mutex);
            finishLoadingSingleObject(name, loading_id, lock);
            throw;
        }
    }

    /// Returns single object info, checks loading_id and name.
    std::optional<Info> prepareToLoadSingleObject(
        const String & name, size_t loading_id, size_t min_id_to_finish_loading_dependencies_, const LoadingGuardForAsyncLoad &)
    {
        Info * info = getInfo(name);
        /// We check here if this is exactly the same loading as we planned to perform.
        /// This check is necessary because the object could be removed or load with another config before this thread even starts.
        if (!info || !info->isLoading() || (info->loading_id != loading_id))
            return {};

        min_id_to_finish_loading_dependencies[std::this_thread::get_id()] = min_id_to_finish_loading_dependencies_;
        return *info;
    }

    /// Load one object, returns object ptr or exception.
    std::pair<LoadablePtr, std::exception_ptr>
    loadSingleObject(const String & name, const ObjectConfig & config, LoadablePtr previous_version)
    {
        /// Use `create_function` to perform the actual loading.
        /// It's much better to do it with `mutex` unlocked because the loading can take a lot of time
        /// and require access to other objects.
        LoadablePtr new_object;
        std::exception_ptr new_exception;
        try
        {
            new_object = create_object(name, config, previous_version);
        }
        catch (...)
        {
            new_exception = std::current_exception();
        }
        return std::make_pair(new_object, new_exception);
    }

    /// Saves the result of the loading, calculates the time of the next update, and handles errors.
    void saveResultOfLoadingSingleObject(
        const String & name,
        size_t loading_id,
        LoadablePtr previous_version,
        LoadablePtr new_object,
        std::exception_ptr new_exception,
        size_t error_count,
        const LoadingGuardForAsyncLoad &)
    {
        /// Calculate a new update time.
        TimePoint next_update_time;
        try
        {
            if (new_exception)
                ++error_count;
            else
                error_count = 0;

            LoadablePtr object = previous_version;
            if (new_object)
                object = new_object;

            next_update_time = calculateNextUpdateTime(object, error_count);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot find out when the " + type_name + " '" + name + "' should be updated");
            next_update_time = TimePoint::max();
        }


        Info * info = getInfo(name);

        /// We should check if this is still the same loading as we were doing.
        /// This is necessary because the object could be removed or load with another config while the `mutex` was unlocked.
        if (!info)
        {
            LOG_TRACE(log, "Next update time for '{}' will not be set because this object was not found.", name);
            return;
        }
        if (!info->isLoading())
        {
            LOG_TRACE(log, "Next update time for '{}' will not be set because this object is not currently loading.", name);
            return;
        }
        if (info->loading_id != loading_id)
        {
            LOG_TRACE(log, "Next update time for '{}' will not be set because this object's current loading_id {} is different from the specified {}.", name, info->loading_id, loading_id);
            return;
        }

        if (new_exception)
        {
            auto next_update_time_description = [next_update_time]
            {
                if (next_update_time == TimePoint::max())
                    return String();
                return ", next update is scheduled at " + to_string(next_update_time);
            };
            if (previous_version)
                tryLogException(new_exception, log, "Could not update " + type_name + " '" + name + "'"
                                ", leaving the previous version" + next_update_time_description());
            else
                tryLogException(new_exception, log, "Could not load " + type_name + " '" + name + "'" + next_update_time_description());
        }

        if (new_object)
            info->object = new_object;

        info->exception = new_exception;
        info->error_count = error_count;
        const auto current_time = std::chrono::system_clock::now();
        info->loading_end_time = current_time;
        if (!info->exception)
            info->last_successful_update_time = current_time;
        info->state_id = info->loading_id;
        info->next_update_time = next_update_time;
        LOG_TRACE(log, "Next update time for '{}' was set to {}", info->name, to_string(next_update_time));
    }

    /// Removes the references to the loading thread from the maps.
    void finishLoadingSingleObject(const String & name, size_t loading_id, const LoadingGuardForAsyncLoad &)
    {
        Info * info = getInfo(name);
        if (info && (info->loading_id == loading_id))
        {
            info->loading_id = info->state_id;
            CurrentStatusInfo::set(CurrentStatusInfo::DictionaryStatus, name, static_cast<Int8>(info->status()));
        }
        min_id_to_finish_loading_dependencies.erase(std::this_thread::get_id());

        /// Add `loading_id` to the list of recently finished loadings.
        /// This list is used to later put the threads which finished loading back to the thread pool.
        /// (We can't put the loading thread back to the thread pool immediately here because at this point
        /// the loading thread is about to finish but it's not finished yet right now.)
        recently_finished_loadings.push_back(loading_id);
    }

    /// Calculate next update time for loaded_object. Can be called without mutex locking,
    /// because single loadable can be loaded in single thread only.
    TimePoint calculateNextUpdateTime(const LoadablePtr & loaded_object, size_t error_count) const
    {
        static constexpr auto never = TimePoint::max();

        if (loaded_object)
        {
            if (!loaded_object->supportUpdates())
            {
                LOG_TRACE(log, "Supposed update time for '{}' is never (loaded, does not support updates)", loaded_object->getLoadableName());

                return never;
            }

            /// do not update loadable objects with zero as lifetime
            const auto & lifetime = loaded_object->getLifetime();
            if (lifetime.min_sec == 0 && lifetime.max_sec == 0)
            {
                LOG_TRACE(log, "Supposed update time for '{}' is never (loaded, lifetime 0)", loaded_object->getLoadableName());
                return never;
            }

            if (!error_count)
            {
                std::uniform_int_distribution<UInt64> distribution{lifetime.min_sec, lifetime.max_sec};
                auto result = std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)};
                LOG_TRACE(log, "Supposed update time for '{}' is {} (loaded, lifetime [{}, {}], no errors)",
                    loaded_object->getLoadableName(), to_string(result), lifetime.min_sec, lifetime.max_sec);
                return result;
            }

            auto result = std::chrono::system_clock::now() + std::chrono::seconds(calculateDurationWithBackoff(rnd_engine, error_count));
            LOG_TRACE(log, "Supposed update time for '{}' is {} (backoff, {} errors)", loaded_object->getLoadableName(), to_string(result), error_count);
            return result;
        }
        else
        {
            auto result = std::chrono::system_clock::now() + std::chrono::seconds(calculateDurationWithBackoff(rnd_engine, error_count));
            LOG_TRACE(log, "Supposed update time for unspecified object is {} (backoff, {} errors.", to_string(result), error_count);
            return result;
        }
    }

    const CreateObjectFunction create_object;
    const String type_name;
    Poco::Logger * log;

    mutable std::mutex mutex;
    std::condition_variable event;
    ObjectConfigsPtr configs;
    std::unordered_map<String, Info> infos;
    bool always_load_everything = false;
    std::atomic<bool> enable_async_loading = false;
    std::unordered_map<size_t, ThreadFromGlobalPool> loading_threads;
    std::vector<size_t> recently_finished_loadings;
    std::unordered_map<std::thread::id, size_t> min_id_to_finish_loading_dependencies;
    size_t next_id_counter = 1; /// should always be > 0
    mutable pcg64 rnd_engine{randomSeed()};
};


class ExternalLoader::PeriodicUpdater : private boost::noncopyable
{
public:
    static constexpr UInt64 check_period_sec = 5;

    PeriodicUpdater(LoadablesConfigReader & config_files_reader_, LoadingDispatcher & loading_dispatcher_)
        : config_files_reader(config_files_reader_), loading_dispatcher(loading_dispatcher_)
    {
    }

    ~PeriodicUpdater() { enable(false); }

    void enable(bool enable_)
    {
        std::unique_lock lock{mutex};
        enabled = enable_;

        if (enable_)
        {
            if (!thread.joinable())
            {
                /// Starts the thread which will do periodic updates.
                thread = ThreadFromGlobalPool{&PeriodicUpdater::doPeriodicUpdates, this};
            }
        }
        else
        {
            if (thread.joinable())
            {
                /// Wait for the thread to finish.
                auto temp_thread = std::move(thread);
                lock.unlock();
                event.notify_one();
                temp_thread.join();
            }
        }
    }


private:
    void doPeriodicUpdates()
    {
        setThreadName("ExterLdrReload");

        std::unique_lock lock{mutex};
        auto pred = [this] { return !enabled; };
        while (!event.wait_for(lock, std::chrono::seconds(check_period_sec), pred))
        {
            lock.unlock();
            loading_dispatcher.setConfiguration(config_files_reader.read());
            loading_dispatcher.reloadOutdated();
            lock.lock();
        }
    }

    LoadablesConfigReader & config_files_reader;
    LoadingDispatcher & loading_dispatcher;
    mutable std::mutex mutex;
    bool enabled = false;
    ThreadFromGlobalPool thread;
    std::condition_variable event;
};


ExternalLoader::ExternalLoader(const String & type_name_, Poco::Logger * log_)
    : config_files_reader(std::make_unique<LoadablesConfigReader>(type_name_, log_))
    , loading_dispatcher(std::make_unique<LoadingDispatcher>(
          [this](auto && a, auto && b, auto && c) { return createObject(a, b, c); },
          type_name_,
          log_))
    , periodic_updater(std::make_unique<PeriodicUpdater>(*config_files_reader, *loading_dispatcher))
    , type_name(type_name_)
    , log(log_)
{
}

ExternalLoader::~ExternalLoader() = default;

scope_guard ExternalLoader::addConfigRepository(std::unique_ptr<IExternalLoaderConfigRepository> repository) const
{
    auto * ptr = repository.get();
    String name = ptr->getName();

    config_files_reader->addConfigRepository(std::move(repository));
    reloadConfig(name);

    return [this, ptr, name]()
    {
        config_files_reader->removeConfigRepository(ptr);
        reloadConfig(name);
    };
}

void ExternalLoader::setConfigSettings(const ExternalLoaderConfigSettings & settings)
{
    config_files_reader->setConfigSettings(settings);
}

void ExternalLoader::enableAlwaysLoadEverything(bool enable)
{
    loading_dispatcher->enableAlwaysLoadEverything(enable);
}

void ExternalLoader::enableAsyncLoading(bool enable)
{
    loading_dispatcher->enableAsyncLoading(enable);
}

void ExternalLoader::enablePeriodicUpdates(bool enable_)
{
    periodic_updater->enable(enable_);
}

bool ExternalLoader::hasLoadedObjects() const
{
    return loading_dispatcher->hasLoadedObjects();
}

ExternalLoader::Status ExternalLoader::getCurrentStatus(const String & name) const
{
    return loading_dispatcher->getCurrentStatus(name);
}

template <typename ReturnType, typename>
ReturnType ExternalLoader::getLoadResult(const String & name) const
{
    return loading_dispatcher->getLoadResult<ReturnType>(name);
}

template <typename ReturnType, typename>
ReturnType ExternalLoader::getLoadResults(const FilterByNameFunction & filter) const
{
    return loading_dispatcher->getLoadResults<ReturnType>(filter);
}

ExternalLoader::Loadables ExternalLoader::getLoadedObjects() const
{
    return getLoadResults<Loadables>();
}

ExternalLoader::Loadables ExternalLoader::getLoadedObjects(const FilterByNameFunction & filter) const
{
    return getLoadResults<Loadables>(filter);
}

size_t ExternalLoader::getNumberOfLoadedObjects() const
{
    return loading_dispatcher->getNumberOfLoadedObjects();
}

size_t ExternalLoader::getNumberOfObjects() const
{
    return loading_dispatcher->getNumberOfObjects();
}

template <typename ReturnType, typename>
ReturnType ExternalLoader::tryLoad(const String & name, Duration timeout) const
{
    return loading_dispatcher->tryLoad<ReturnType>(name, timeout);
}

template <typename ReturnType, typename>
ReturnType ExternalLoader::tryLoad(const FilterByNameFunction & filter, Duration timeout) const
{
    return loading_dispatcher->tryLoad<ReturnType>(filter, timeout);
}

template <typename ReturnType, typename>
ReturnType ExternalLoader::load(const String & name) const
{
    auto result = tryLoad<LoadResult>(name);
    checkLoaded(result, false);
    return convertTo<ReturnType>(result);
}

template <typename ReturnType, typename>
ReturnType ExternalLoader::load(const FilterByNameFunction & filter) const
{
    auto results = tryLoad<LoadResults>(filter);
    checkLoaded(results, false);
    return convertTo<ReturnType>(results);
}

template <typename ReturnType, typename>
ReturnType ExternalLoader::loadOrReload(const String & name) const
{
    loading_dispatcher->setConfiguration(config_files_reader->read());
    auto result = loading_dispatcher->tryLoadOrReload<LoadResult>(name, WAIT);
    checkLoaded(result, true);
    return convertTo<ReturnType>(result);
}

template <typename ReturnType, typename>
ReturnType ExternalLoader::loadOrReload(const FilterByNameFunction & filter) const
{
    loading_dispatcher->setConfiguration(config_files_reader->read());
    auto results = loading_dispatcher->tryLoadOrReload<LoadResults>(filter, WAIT);
    checkLoaded(results, true);
    return convertTo<ReturnType>(results);
}

template <typename ReturnType, typename>
ReturnType ExternalLoader::reloadAllTriedToLoad() const
{
    std::unordered_set<String> names;
    boost::range::copy(getAllTriedToLoadNames(), std::inserter(names, names.end()));
    return loadOrReload<ReturnType>([&names](const String & name) { return names.count(name); });
}

bool ExternalLoader::has(const String & name) const
{
    return loading_dispatcher->has(name);
}

Strings ExternalLoader::getAllTriedToLoadNames() const
{
    return loading_dispatcher->getAllTriedToLoadNames();
}


void ExternalLoader::checkLoaded(const ExternalLoader::LoadResult & result,
                                 bool check_no_errors) const
{
    if (result.object && (!check_no_errors || !result.exception))
        return;
    if (result.status == ExternalLoader::Status::LOADING)
        throw Exception(type_name + " '" + result.name + "' is still loading", ErrorCodes::BAD_ARGUMENTS);
    if (result.exception)
    {
        // Exception is shared for multiple threads.
        // Don't just rethrow it, because sharing the same exception object
        // between multiple threads can lead to weird effects if they decide to
        // modify it, for example, by adding some error context.
        try
        {
            std::rethrow_exception(result.exception);
        }
        catch (const Poco::Exception & e)
        {
            /// This will create a copy for Poco::Exception and DB::Exception
            e.rethrow();
        }
        catch (...)
        {
            throw DB::Exception(ErrorCodes::DICTIONARIES_WAS_NOT_LOADED,
                                "Failed to load dictionary '{}': {}",
                                result.name,
                                getCurrentExceptionMessage(true /*with stack trace*/,
                                                           true /*check embedded stack trace*/));
        }
    }
    if (result.status == ExternalLoader::Status::NOT_EXIST)
        throw Exception(type_name + " '" + result.name + "' not found", ErrorCodes::BAD_ARGUMENTS);
    if (result.status == ExternalLoader::Status::NOT_LOADED)
        throw Exception(type_name + " '" + result.name + "' not tried to load", ErrorCodes::BAD_ARGUMENTS);
}

void ExternalLoader::checkLoaded(const ExternalLoader::LoadResults & results,
                                 bool check_no_errors) const
{
    std::exception_ptr exception;
    for (const auto & result : results)
    {
        try
        {
            checkLoaded(result, check_no_errors);
        }
        catch (...)
        {
            if (!exception)
                exception = std::current_exception();
            else
                tryLogCurrentException(log);
        }
    }

    if (exception)
        std::rethrow_exception(exception);
}


void ExternalLoader::reloadConfig() const
{
    loading_dispatcher->setConfiguration(config_files_reader->read());
}

void ExternalLoader::reloadConfig(const String & repository_name) const
{
    loading_dispatcher->setConfiguration(config_files_reader->read(repository_name));
}

void ExternalLoader::reloadConfig(const String & repository_name, const String & path) const
{
    loading_dispatcher->setConfiguration(config_files_reader->read(repository_name, path));
}

ExternalLoader::LoadablePtr ExternalLoader::createObject(
    const String & name, const ObjectConfig & config, const LoadablePtr & previous_version) const
{
    if (previous_version)
        return previous_version->clone();

    return create(name, *config.config, config.key_in_config, config.repository_name);
}

template ExternalLoader::LoadablePtr ExternalLoader::getLoadResult<ExternalLoader::LoadablePtr>(const String &) const;
template ExternalLoader::LoadResult ExternalLoader::getLoadResult<ExternalLoader::LoadResult>(const String &) const;
template ExternalLoader::Loadables ExternalLoader::getLoadResults<ExternalLoader::Loadables>(const FilterByNameFunction &) const;
template ExternalLoader::LoadResults ExternalLoader::getLoadResults<ExternalLoader::LoadResults>(const FilterByNameFunction &) const;

template ExternalLoader::LoadablePtr ExternalLoader::tryLoad<ExternalLoader::LoadablePtr>(const String &, Duration) const;
template ExternalLoader::LoadResult ExternalLoader::tryLoad<ExternalLoader::LoadResult>(const String &, Duration) const;
template ExternalLoader::Loadables ExternalLoader::tryLoad<ExternalLoader::Loadables>(const FilterByNameFunction &, Duration) const;
template ExternalLoader::LoadResults ExternalLoader::tryLoad<ExternalLoader::LoadResults>(const FilterByNameFunction &, Duration) const;

template ExternalLoader::LoadablePtr ExternalLoader::load<ExternalLoader::LoadablePtr>(const String &) const;
template ExternalLoader::LoadResult ExternalLoader::load<ExternalLoader::LoadResult>(const String &) const;
template ExternalLoader::Loadables ExternalLoader::load<ExternalLoader::Loadables>(const FilterByNameFunction &) const;
template ExternalLoader::LoadResults ExternalLoader::load<ExternalLoader::LoadResults>(const FilterByNameFunction &) const;

template ExternalLoader::LoadablePtr ExternalLoader::loadOrReload<ExternalLoader::LoadablePtr>(const String &) const;
template ExternalLoader::LoadResult ExternalLoader::loadOrReload<ExternalLoader::LoadResult>(const String &) const;
template ExternalLoader::Loadables ExternalLoader::loadOrReload<ExternalLoader::Loadables>(const FilterByNameFunction &) const;
template ExternalLoader::LoadResults ExternalLoader::loadOrReload<ExternalLoader::LoadResults>(const FilterByNameFunction &) const;

template ExternalLoader::Loadables ExternalLoader::reloadAllTriedToLoad<ExternalLoader::Loadables>() const;
template ExternalLoader::LoadResults ExternalLoader::reloadAllTriedToLoad<ExternalLoader::LoadResults>() const;
}
