#include "ExternalLoader.h"

#include <mutex>
#include <pcg_random.hpp>
#include <common/DateLUT.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ThreadPool.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <ext/scope_guard.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


struct ExternalLoader::ObjectConfig
{
    String config_path;
    Poco::AutoPtr<Poco::Util::AbstractConfiguration> config;
    String key_in_config;
};


/** Reads configurations from configuration repository and parses it.
  */
class ExternalLoader::LoadablesConfigReader : private boost::noncopyable
{
public:
    LoadablesConfigReader(const String & type_name_, Logger * log_)
        : type_name(type_name_), log(log_)
    {
    }
    ~LoadablesConfigReader() = default;

    void addConfigRepository(std::unique_ptr<IExternalLoaderConfigRepository> repository, const ExternalLoaderConfigSettings & settings)
    {
        std::lock_guard lock{mutex};
        repositories.emplace_back(std::move(repository), std::move(settings));
    }

    using ObjectConfigsPtr = std::shared_ptr<const std::unordered_map<String /* object's name */, ObjectConfig>>;

    /// Reads configuration files.
    ObjectConfigsPtr read()
    {
        std::lock_guard lock{mutex};

        // Check last modification times of files and read those files which are new or changed.
        if (!readLoadablesInfos())
            return configs; // Nothing changed, so we can return the previous result.

        // Generate new result.
        auto new_configs = std::make_shared<std::unordered_map<String /* object's name */, ObjectConfig>>();
        for (const auto & [path, loadable_info] :  loadables_infos)
        {
            for (const auto & [name, config] : loadable_info.configs)
            {
                auto already_added_it = new_configs->find(name);
                if (already_added_it != new_configs->end())
                {
                    const auto & already_added = already_added_it->second;
                    LOG_WARNING(log, path << ": " << type_name << " '" << name << "' is found "
                                          << ((path == already_added.config_path)
                                                  ? ("twice in the same file")
                                                  : ("both in file '" + already_added.config_path + "' and '" + path + "'")));
                    continue;
                }
                new_configs->emplace(name, config);
            }
        }

        configs = new_configs;
        return configs;
    }

private:
    struct  LoadablesInfos
    {
        Poco::Timestamp last_update_time = 0;
        std::vector<std::pair<String, ObjectConfig>> configs; // Parsed file's contents.
        bool in_use = true; // Whether the ` LoadablesInfos` should be destroyed because the correspondent file is deleted.
    };

    /// Read files and store them to the map ` loadables_infos`.
    bool readLoadablesInfos()
    {
        bool changed = false;

        for (auto & name_and_loadable_info : loadables_infos)
        {
            LoadablesInfos & loadable_info = name_and_loadable_info.second;
            loadable_info.in_use = false;
        }

        for (const auto & [repository, settings] : repositories)
        {
            const auto names = repository->getAllLoadablesDefinitionNames();
            for (const auto & name : names)
            {
                auto it =  loadables_infos.find(name);
                if (it !=  loadables_infos.end())
                {
                    LoadablesInfos & loadable_info = it->second;
                    if (readLoadablesInfo(*repository, name, settings, loadable_info))
                        changed = true;
                }
                else
                {
                    LoadablesInfos loadable_info;
                    if (readLoadablesInfo(*repository, name, settings, loadable_info))
                    {
                        loadables_infos.emplace(name, std::move(loadable_info));
                        changed = true;
                    }
                }
            }
        }

        std::vector<String> deleted_files;
        for (auto & [path, loadable_info] : loadables_infos)
            if (!loadable_info.in_use)
                deleted_files.emplace_back(path);
        if (!deleted_files.empty())
        {
            for (const String & deleted_file : deleted_files)
                loadables_infos.erase(deleted_file);
            changed = true;
        }
        return changed;
    }

    bool readLoadablesInfo(
        IExternalLoaderConfigRepository & repository,
        const String & path,
        const ExternalLoaderConfigSettings & settings,
        LoadablesInfos & loadable_info) const
    {
        try
        {
            if (path.empty() || !repository.exists(path))
            {
                LOG_WARNING(log, "config file '" + path + "' does not exist");
                return false;
            }

            auto update_time_from_repository = repository.getUpdateTime(path);

            /// Actually it can't be less, but for sure we check less or equal
            if (update_time_from_repository <= loadable_info.last_update_time)
            {
                loadable_info.in_use = true;
                return false;
            }

            auto file_contents = repository.load(path);

            /// get all objects' definitions
            Poco::Util::AbstractConfiguration::Keys keys;
            file_contents->keys(keys);

            /// for each object defined in xml config
            std::vector<std::pair<String, ObjectConfig>> configs_from_file;
            for (const auto & key : keys)
            {
                if (!startsWith(key, settings.external_config))
                {
                    if (!startsWith(key, "comment") && !startsWith(key, "include_from"))
                        LOG_WARNING(log, path << ": file contains unknown node '" << key << "', expected '" << settings.external_config << "'");
                    continue;
                }

                String name = file_contents->getString(key + "." + settings.external_name);
                if (name.empty())
                {
                    LOG_WARNING(log, path << ": node '" << key << "' defines " << type_name << " with an empty name. It's not allowed");
                    continue;
                }

                configs_from_file.emplace_back(name, ObjectConfig{path, file_contents, key});
            }

            loadable_info.configs = std::move(configs_from_file);
            loadable_info.last_update_time = update_time_from_repository;
            loadable_info.in_use = true;
            return true;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to read config file '" + path + "'");
            return false;
        }
    }

    const String type_name;
    Logger * log;

    std::mutex mutex;
    std::vector<std::pair<std::unique_ptr<IExternalLoaderConfigRepository>, ExternalLoaderConfigSettings>> repositories;
    ObjectConfigsPtr configs;
    std::unordered_map<String /* config path */, LoadablesInfos>  loadables_infos;
};


/** Manages loading and reloading objects. Uses configurations from the class LoadablesConfigReader.
  * Supports parallel loading.
  */
class ExternalLoader::LoadingDispatcher : private boost::noncopyable
{
public:
    /// Called to load or reload an object.
    using CreateObjectFunction = std::function<LoadablePtr(
        const String & /* name */, const ObjectConfig & /* config */, bool config_changed, const LoadablePtr & /* previous_version */)>;

    LoadingDispatcher(
        const CreateObjectFunction & create_object_function_,
        const String & type_name_,
        Logger * log_)
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
        while (!loading_ids.empty())
        {
            auto it = loading_ids.begin();
            auto thread = std::move(it->second);
            loading_ids.erase(it);
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

        configs = new_configs;

        std::vector<String> removed_names;
        for (auto & [name, info] : infos)
        {
            auto new_config_it = new_configs->find(name);
            if (new_config_it == new_configs->end())
                removed_names.emplace_back(name);
            else
            {
                const auto & new_config = new_config_it->second;
                if (!isSameConfiguration(*info.config.config, info.config.key_in_config, *new_config.config, new_config.key_in_config))
                {
                    /// Configuration has been changed.
                    info.config = new_config;
                    info.config_changed = true;

                    if (info.wasLoading())
                    {
                        /// The object has been tried to load before, so it is currently in use or was in use
                        /// and we should try to reload it with the new config.
                        cancelLoading(info);
                        startLoading(name, info);
                    }
                }
            }
        }

        /// Insert to the map those objects which added to the new configuration.
        for (const auto & [name, config] : *new_configs)
        {
            if (infos.find(name) == infos.end())
            {
                Info & info = infos.emplace(name, Info{config}).first->second;
                if (always_load_everything)
                    startLoading(name, info);
            }
        }

        /// Remove from the map those objects which were removed from the configuration.
        for (const String & name : removed_names)
            infos.erase(name);

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
                if (!info.wasLoading())
                    startLoading(name, info);
        }
    }

    /// Sets whether the objects should be loaded asynchronously, each loading in a new thread (from the thread pool).
    void enableAsyncLoading(bool enable)
    {
        std::lock_guard lock{mutex};
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
    LoadResult getCurrentLoadResult(const String & name) const
    {
        std::lock_guard lock{mutex};
        const Info * info = getInfo(name);
        if (!info)
            return {Status::NOT_EXIST};
        return info->loadResult();
    }

    /// Returns all the load results as a map.
    /// The function doesn't load anything, it just returns the current load results as is.
    LoadResults getCurrentLoadResults(const FilterByNameFunction & filter_by_name) const
    {
        std::lock_guard lock{mutex};
        return collectLoadResults(filter_by_name);
    }

    LoadResults getCurrentLoadResults() const { return getCurrentLoadResults(allNames); }

    /// Returns all the loaded objects as a map.
    /// The function doesn't load anything, it just returns the current load results as is.
    Loadables getCurrentlyLoadedObjects(const FilterByNameFunction & filter_by_name) const
    {
        std::lock_guard lock{mutex};
        return collectLoadedObjects(filter_by_name);
    }

    Loadables getCurrentlyLoadedObjects() const { return getCurrentlyLoadedObjects(allNames); }

    size_t getNumberOfCurrentlyLoadedObjects() const
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

    bool hasCurrentlyLoadedObjects() const
    {
        std::lock_guard lock{mutex};
        for (auto & name_info : infos)
            if (name_info.second.loaded())
                return true;
        return false;
    }

    /// Tries to load a specified object during the timeout.
    /// Returns nullptr if the loading is unsuccessful or if there is no such object.
    void load(const String & name, LoadablePtr & loaded_object, Duration timeout = NO_TIMEOUT)
    {
        std::unique_lock lock{mutex};
        Info * info = loadImpl(name, timeout, lock);
        loaded_object = (info ? info->object : nullptr);
    }

    /// Tries to finish loading of a specified object during the timeout.
    /// Returns nullptr if the loading is unsuccessful or if there is no such object.
    void loadStrict(const String & name, LoadablePtr & loaded_object)
    {
        std::unique_lock lock{mutex};
        Info * info = loadImpl(name, NO_TIMEOUT, lock);
        if (!info)
            throw Exception("No such " + type_name + " '" + name + "'.", ErrorCodes::BAD_ARGUMENTS);
        checkLoaded(name, *info);
        loaded_object = info->object;
    }

    /// Tries to start loading of the objects for which the specified functor returns true.
    void load(const FilterByNameFunction & filter_by_name)
    {
        std::lock_guard lock{mutex};
        for (auto & [name, info] : infos)
            if (!info.wasLoading() && filter_by_name(name))
                startLoading(name, info);
    }

    /// Tries to finish loading of the objects for which the specified function returns true.
    void load(const FilterByNameFunction & filter_by_name, Loadables & loaded_objects, Duration timeout = NO_TIMEOUT)
    {
        std::unique_lock lock{mutex};
        loadImpl(filter_by_name, timeout, lock);
        loaded_objects = collectLoadedObjects(filter_by_name);
    }

    /// Tries to finish loading of all the objects during the timeout.
    void load(Loadables & loaded_objects, Duration timeout = NO_TIMEOUT) { load(allNames, loaded_objects, timeout); }

    /// Starts reloading a specified object.
    void reload(const String & name, bool load_never_loading = false)
    {
        std::lock_guard lock{mutex};
        Info * info = getInfo(name);
        if (!info)
            return;

        if (info->wasLoading() || load_never_loading)
        {
            cancelLoading(*info);
            info->forced_to_reload = true;
            startLoading(name, *info);
        }
    }

    /// Starts reloading of the objects which `filter_by_name` returns true for.
    void reload(const FilterByNameFunction & filter_by_name, bool load_never_loading = false)
    {
        std::lock_guard lock{mutex};
        for (auto & [name, info] : infos)
        {
            if ((info.wasLoading() || load_never_loading) && filter_by_name(name))
            {
                cancelLoading(info);
                info.forced_to_reload = true;
                startLoading(name, info);
            }
        }
    }

    /// Starts reloading of all the objects.
    void reload(bool load_never_loading = false) { reload(allNames, load_never_loading); }

    /// Starts reloading all the object which update time is earlier than now.
    /// The function doesn't touch the objects which were never tried to load.
    void reloadOutdated()
    {
        /// Iterate through all the objects and find loaded ones which should be checked if they were modified.
        std::unordered_map<LoadablePtr, bool> is_modified_map;
        {
            std::lock_guard lock{mutex};
            TimePoint now = std::chrono::system_clock::now();
            for (const auto & name_and_info : infos)
            {
                const auto & info = name_and_info.second;
                if ((now >= info.next_update_time) && !info.loading() && info.loaded())
                    is_modified_map.emplace(info.object, true);
            }
        }

        /// Find out which of the loaded objects were modified.
        /// We couldn't perform these checks while we were building `is_modified_map` because
        /// the `mutex` should be unlocked while we're calling the function object->isModified()
        for (auto & [object, is_modified_flag] : is_modified_map)
        {
            try
            {
                is_modified_flag = object->isModified();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Could not check if " + type_name + " '" + object->getName() + "' was modified");
            }
        }

        /// Iterate through all the objects again and either start loading or just set `next_update_time`.
        {
            std::lock_guard lock{mutex};
            TimePoint now = std::chrono::system_clock::now();
            for (auto & [name, info] : infos)
            {
                if ((now >= info.next_update_time) && !info.loading())
                {
                    if (info.loaded())
                    {
                        auto it = is_modified_map.find(info.object);
                        if (it == is_modified_map.end())
                            continue; /// Object has been just loaded (it wasn't loaded while we were building the map `is_modified_map`), so we don't have to reload it right now.

                        bool is_modified_flag = it->second;
                        if (!is_modified_flag)
                        {
                            /// Object wasn't modified so we only have to set `next_update_time`.
                            info.next_update_time = calculateNextUpdateTime(info.object, info.error_count);
                            continue;
                        }

                        /// Object was modified and should be reloaded.
                        startLoading(name, info);
                    }
                    else if (info.failed())
                    {
                        /// Object was never loaded successfully and should be reloaded.
                        startLoading(name, info);
                    }
                }
            }
        }
    }

private:
    struct Info
    {
        Info(const ObjectConfig & config_) : config(config_) {}

        bool loaded() const { return object != nullptr; }
        bool failed() const { return !object && exception; }
        bool loading() const { return loading_id != 0; }
        bool wasLoading() const { return loaded() || failed() || loading(); }
        bool ready() const { return (loaded() || failed()) && !forced_to_reload; }

        Status status() const
        {
            if (object)
                return loading() ? Status::LOADED_AND_RELOADING : Status::LOADED;
            else if (exception)
                return loading() ? Status::FAILED_AND_RELOADING : Status::FAILED;
            else
                return loading() ? Status::LOADING : Status::NOT_LOADED;
        }

        Duration loadingDuration() const
        {
            if (loading())
                return std::chrono::duration_cast<Duration>(std::chrono::system_clock::now() - loading_start_time);
            return std::chrono::duration_cast<Duration>(loading_end_time - loading_start_time);
        }

        LoadResult loadResult() const
        {
            LoadResult result{status()};
            result.object = object;
            result.exception = exception;
            result.loading_start_time = loading_start_time;
            result.loading_duration = loadingDuration();
            result.origin = config.config_path;
            return result;
        }

        ObjectConfig config;
        LoadablePtr object;
        TimePoint loading_start_time;
        TimePoint loading_end_time;
        size_t loading_id = 0; /// Non-zero if it's loading right now.
        size_t error_count = 0; /// Numbers of errors since last successful loading.
        std::exception_ptr exception; /// Last error occurred.
        bool config_changed = false; /// Whether the config has been change since last successful loading.
        bool forced_to_reload = false; /// Whether the current reloading is forced, i.e. caused by user's direction. For periodic reloading and reloading due to a config's change `forced_to_reload == false`.
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

    Loadables collectLoadedObjects(const FilterByNameFunction & filter_by_name) const
    {
        Loadables objects;
        objects.reserve(infos.size());
        for (const auto & [name, info] : infos)
            if (info.loaded() && filter_by_name(name))
                objects.emplace_back(info.object);
        return objects;
    }

    LoadResults collectLoadResults(const FilterByNameFunction & filter_by_name) const
    {
        LoadResults load_results;
        load_results.reserve(infos.size());
        for (const auto & [name, info] : infos)
            if (filter_by_name(name))
                load_results.emplace_back(name, info.loadResult());
        return load_results;
    }

    Info * loadImpl(const String & name, Duration timeout, std::unique_lock<std::mutex> & lock)
    {
        Info * info;
        auto pred = [&]()
        {
            info = getInfo(name);
            if (!info || info->ready())
                return true;
            if (!info->loading())
                startLoading(name, *info);
            return info->ready();
        };

        if (timeout == NO_TIMEOUT)
            event.wait(lock, pred);
        else
            event.wait_for(lock, timeout, pred);

        return info;
    }

    void loadImpl(const FilterByNameFunction & filter_by_name, Duration timeout, std::unique_lock<std::mutex> & lock)
    {
        auto pred = [&]()
        {
            bool all_ready = true;
            for (auto & [name, info] : infos)
            {
                if (info.ready() || !filter_by_name(name))
                    continue;
                if (!info.loading())
                    startLoading(name, info);
                if (!info.ready())
                    all_ready = false;
            }
            return all_ready;
        };

        if (timeout == NO_TIMEOUT)
            event.wait(lock, pred);
        else
            event.wait_for(lock, timeout, pred);
    }

    void startLoading(const String & name, Info & info)
    {
        if (info.loading())
            return;

        /// All loadings have unique loading IDs.
        size_t loading_id = next_loading_id++;
        info.loading_id = loading_id;
        info.loading_start_time = std::chrono::system_clock::now();
        info.loading_end_time = TimePoint{};

        if (enable_async_loading)
        {
            /// Put a job to the thread pool for the loading.
            auto thread = ThreadFromGlobalPool{&LoadingDispatcher::doLoading, this, name, loading_id, true};
            loading_ids.try_emplace(loading_id, std::move(thread));
        }
        else
        {
            /// Perform the loading immediately.
            doLoading(name, loading_id, false);
        }
    }

    /// Does the loading, possibly in the separate thread.
    void doLoading(const String & name, size_t loading_id, bool async)
    {
        std::unique_lock<std::mutex> lock;
        if (async)
        {
            setThreadName("ExterLdrJob");
            lock = std::unique_lock{mutex}; /// If `async == false` the mutex is already locked.
        }

        SCOPE_EXIT({
            if (async)
            {
                if (!lock.owns_lock())
                    lock.lock();
                /// Remove the information about the thread after it finishes.
                auto it = loading_ids.find(loading_id);
                if (it != loading_ids.end())
                {
                    it->second.detach();
                    loading_ids.erase(it);
                }
            }
        });

        /// We check here if this is exactly the same loading as we planned to perform.
        /// This check is necessary because the object could be removed or load with another config before this thread even starts.
        Info * info = getInfo(name);
        if (!info || !info->loading() || (info->loading_id != loading_id))
            return;

        ObjectConfig config = info->config;
        bool config_changed = info->config_changed;
        LoadablePtr previous_version = info->object;
        size_t error_count = info->error_count;

        /// Use `create_function` to perform the actual loading.
        /// It's much better to do it with `mutex` unlocked because the loading can take a lot of time
        /// and require access to other objects.
        if (async)
            lock.unlock();

        LoadablePtr new_object;
        std::exception_ptr new_exception;
        try
        {
            new_object = create_object(name, config, config_changed, previous_version);
        }
        catch (...)
        {
            new_exception = std::current_exception();
        }

        if (!new_object && !new_exception)
            throw Exception("No object created and no exception raised for " + type_name, ErrorCodes::LOGICAL_ERROR);

        /// Calculate a new update time.
        TimePoint next_update_time;
        try
        {
            if (new_exception)
                ++error_count;
            else
                error_count = 0;
            next_update_time = calculateNextUpdateTime(new_object, error_count);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot find out when the " + type_name + " '" + name + "' should be updated");
            next_update_time = TimePoint::max();
        }

        /// Lock the mutex again to store the changes.
        if (async)
            lock.lock();
        info = getInfo(name);

        /// And again we should check if this is still the same loading as we were doing.
        /// This is necessary because the object could be removed or load with another config while the `mutex` was unlocked.
        if (!info || !info->loading() || (info->loading_id != loading_id))
            return;

        if (new_exception)
        {
            auto next_update_time_description = [next_update_time]
            {
                if (next_update_time == TimePoint::max())
                    return String();
                return ", next update is scheduled at "
                    + DateLUT::instance().timeToString(std::chrono::system_clock::to_time_t(next_update_time));
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
        info->loading_end_time = std::chrono::system_clock::now();
        info->loading_id = 0;
        info->next_update_time = next_update_time;

        info->forced_to_reload = false;
        if (new_object)
            info->config_changed = false;

        /// Notify `event` to recheck conditions in loadImpl() now.
        if (async)
            lock.unlock();
        event.notify_all();
    }

    void cancelLoading(const String & name)
    {
        Info * info = getInfo(name);
        if (info)
            cancelLoading(*info);
    }

    void cancelLoading(Info & info)
    {
        if (!info.loading())
            return;

        /// In fact we cannot actually CANCEL the loading (because it's possibly already being performed in another thread).
        /// But we can reset the `loading_id` and doLoading() will understand it as a signal to stop loading.
        info.loading_id = 0;
        info.loading_end_time = std::chrono::system_clock::now();
    }

    void checkLoaded(const String & name, const Info & info)
    {
        if (info.loaded())
            return;
        if (info.loading())
            throw Exception(type_name + " '" + name + "' is still loading.", ErrorCodes::BAD_ARGUMENTS);
        if (info.failed())
            std::rethrow_exception(info.exception);
    }

    /// Filter by name which matches everything.
    static bool allNames(const String &) { return true; }

    /// Calculate next update time for loaded_object. Can be called without mutex locking,
    /// because single loadable can be loaded in single thread only.
    TimePoint calculateNextUpdateTime(const LoadablePtr & loaded_object, size_t error_count) const
    {
        static constexpr auto never = TimePoint::max();
        if (!error_count)
        {
            if (!loaded_object->supportUpdates())
                return never;

            /// do not update loadable objects with zero as lifetime
            const auto & lifetime = loaded_object->getLifetime();
            if (lifetime.min_sec == 0 || lifetime.max_sec == 0)
                return never;

            std::uniform_int_distribution<UInt64> distribution{lifetime.min_sec, lifetime.max_sec};
            return std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)};
        }

        return std::chrono::system_clock::now() + std::chrono::seconds(calculateDurationWithBackoff(rnd_engine, error_count));
    }

    const CreateObjectFunction create_object;
    const String type_name;
    Logger * log;

    mutable std::mutex mutex;
    std::condition_variable event;
    ObjectConfigsPtr configs;
    std::unordered_map<String, Info> infos;
    bool always_load_everything = false;
    bool enable_async_loading = false;
    std::unordered_map<size_t, ThreadFromGlobalPool> loading_ids;
    size_t next_loading_id = 1; /// should always be > 0
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


ExternalLoader::ExternalLoader(const String & type_name_, Logger * log)
    : config_files_reader(std::make_unique<LoadablesConfigReader>(type_name_, log))
    , loading_dispatcher(std::make_unique<LoadingDispatcher>(
          std::bind(&ExternalLoader::createObject, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4),
          type_name_,
          log))
    , periodic_updater(std::make_unique<PeriodicUpdater>(*config_files_reader, *loading_dispatcher))
    , type_name(type_name_)
{
}

ExternalLoader::~ExternalLoader() = default;

void ExternalLoader::addConfigRepository(
    std::unique_ptr<IExternalLoaderConfigRepository> config_repository, const ExternalLoaderConfigSettings & config_settings)
{
    config_files_reader->addConfigRepository(std::move(config_repository), config_settings);
    loading_dispatcher->setConfiguration(config_files_reader->read());
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

bool ExternalLoader::hasCurrentlyLoadedObjects() const
{
    return loading_dispatcher->hasCurrentlyLoadedObjects();
}

ExternalLoader::Status ExternalLoader::getCurrentStatus(const String & name) const
{
    return loading_dispatcher->getCurrentStatus(name);
}

ExternalLoader::LoadResult ExternalLoader::getCurrentLoadResult(const String & name) const
{
    return loading_dispatcher->getCurrentLoadResult(name);
}

ExternalLoader::LoadResults ExternalLoader::getCurrentLoadResults() const
{
    return loading_dispatcher->getCurrentLoadResults();
}

ExternalLoader::LoadResults ExternalLoader::getCurrentLoadResults(const FilterByNameFunction & filter_by_name) const
{
    return loading_dispatcher->getCurrentLoadResults(filter_by_name);
}

ExternalLoader::Loadables ExternalLoader::getCurrentlyLoadedObjects() const
{
    return loading_dispatcher->getCurrentlyLoadedObjects();
}

ExternalLoader::Loadables ExternalLoader::getCurrentlyLoadedObjects(const FilterByNameFunction & filter_by_name) const
{
    return loading_dispatcher->getCurrentlyLoadedObjects(filter_by_name);
}

size_t ExternalLoader::getNumberOfCurrentlyLoadedObjects() const
{
    return loading_dispatcher->getNumberOfCurrentlyLoadedObjects();
}

void ExternalLoader::load(const String & name, LoadablePtr & loaded_object, Duration timeout) const
{
    loading_dispatcher->load(name, loaded_object, timeout);
}

void ExternalLoader::loadStrict(const String & name, LoadablePtr & loaded_object) const
{
    loading_dispatcher->loadStrict(name, loaded_object);
}

void ExternalLoader::load(const FilterByNameFunction & filter_by_name, Loadables & loaded_objects, Duration timeout) const
{
    if (filter_by_name)
        loading_dispatcher->load(filter_by_name, loaded_objects, timeout);
    else
        loading_dispatcher->load(loaded_objects, timeout);
}

void ExternalLoader::load(Loadables & loaded_objects, Duration timeout) const
{
    return loading_dispatcher->load(loaded_objects, timeout);
}

void ExternalLoader::reload(const String & name, bool load_never_loading)
{
    loading_dispatcher->setConfiguration(config_files_reader->read());
    loading_dispatcher->reload(name, load_never_loading);
}

void ExternalLoader::reload(bool load_never_loading)
{
    loading_dispatcher->setConfiguration(config_files_reader->read());
    loading_dispatcher->reload(load_never_loading);
}

ExternalLoader::LoadablePtr ExternalLoader::createObject(
    const String & name, const ObjectConfig & config, bool config_changed, const LoadablePtr & previous_version) const
{
    if (previous_version && !config_changed)
        return previous_version->clone();

    return create(name, *config.config, config.key_in_config);
}

std::vector<std::pair<String, Int8>> ExternalLoader::getStatusEnumAllPossibleValues()
{
    return std::vector<std::pair<String, Int8>>{
        {toString(Status::NOT_LOADED), static_cast<Int8>(Status::NOT_LOADED)},
        {toString(Status::LOADED), static_cast<Int8>(Status::LOADED)},
        {toString(Status::FAILED), static_cast<Int8>(Status::FAILED)},
        {toString(Status::LOADING), static_cast<Int8>(Status::LOADING)},
        {toString(Status::LOADED_AND_RELOADING), static_cast<Int8>(Status::LOADED_AND_RELOADING)},
        {toString(Status::FAILED_AND_RELOADING), static_cast<Int8>(Status::FAILED_AND_RELOADING)},
        {toString(Status::NOT_EXIST), static_cast<Int8>(Status::NOT_EXIST)},
    };
}


String toString(ExternalLoader::Status status)
{
    using Status = ExternalLoader::Status;
    switch (status)
    {
        case Status::NOT_LOADED: return "NOT_LOADED";
        case Status::LOADED: return "LOADED";
        case Status::FAILED: return "FAILED";
        case Status::LOADING: return "LOADING";
        case Status::FAILED_AND_RELOADING: return "FAILED_AND_RELOADING";
        case Status::LOADED_AND_RELOADING: return "LOADED_AND_RELOADING";
        case Status::NOT_EXIST: return "NOT_EXIST";
    }
    __builtin_unreachable();
}


std::ostream & operator<<(std::ostream & out, ExternalLoader::Status status)
{
    return out << toString(status);
}

}
