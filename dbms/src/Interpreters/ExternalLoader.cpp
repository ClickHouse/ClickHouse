#include <Interpreters/ExternalLoader.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/MemoryTracker.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <ext/scope_guard.h>
#include <Poco/Util/Application.h>
#include <cmath>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int EXTERNAL_LOADABLE_ALREADY_EXISTS;
}


ExternalLoadableLifetime::ExternalLoadableLifetime(const Poco::Util::AbstractConfiguration & config,
                                                   const std::string & config_prefix)
{
    const auto & lifetime_min_key = config_prefix + ".min";
    const auto has_min = config.has(lifetime_min_key);

    min_sec = has_min ? config.getUInt64(lifetime_min_key) : config.getUInt64(config_prefix);
    max_sec = has_min ? config.getUInt64(config_prefix + ".max") : min_sec;
}

void ExternalLoader::reloadPeriodically()
{
    setThreadName("ExterLdrReload");

    while (true)
    {
        if (destroy.tryWait(update_settings.check_period_sec * 1000))
            return;

        reloadAndUpdate();
    }
}


ExternalLoader::ExternalLoader(const Poco::Util::AbstractConfiguration & config,
                               const ExternalLoaderUpdateSettings & update_settings,
                               const ExternalLoaderConfigSettings & config_settings,
                               std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
                               Logger * log, const std::string & loadable_object_name)
        : config(config)
        , update_settings(update_settings)
        , config_settings(config_settings)
        , config_repository(std::move(config_repository))
        , log(log)
        , object_name(loadable_object_name)
{
}

void ExternalLoader::init(bool throw_on_error)
{
    if (is_initialized)
        return;

    is_initialized = true;

    {
        /// During synchronous loading of external dictionaries at moment of query execution,
        /// we should not use per query memory limit.
        auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

        reloadAndUpdate(throw_on_error);
    }

    reloading_thread = std::thread{&ExternalLoader::reloadPeriodically, this};
}


ExternalLoader::~ExternalLoader()
{
    destroy.set();
    reloading_thread.join();
}


void ExternalLoader::reloadAndUpdate(bool throw_on_error)
{
    reloadFromConfigFiles(throw_on_error);

    /// list of recreated loadable objects to perform delayed removal from unordered_map
    std::list<std::string> recreated_failed_loadable_objects;

    std::unique_lock<std::mutex> all_lock(all_mutex);

    /// retry loading failed loadable objects
    for (auto & failed_loadable_object : failed_loadable_objects)
    {
        if (std::chrono::system_clock::now() < failed_loadable_object.second.next_attempt_time)
            continue;

        const auto & name = failed_loadable_object.first;

        try
        {
            auto loadable_ptr = failed_loadable_object.second.loadable->clone();
            if (const auto exception_ptr = loadable_ptr->getCreationException())
            {
                /// recalculate next attempt time
                std::uniform_int_distribution<UInt64> distribution(
                        0, static_cast<UInt64>(std::exp2(failed_loadable_object.second.error_count)));

                std::chrono::seconds delay(std::min<UInt64>(
                        update_settings.backoff_max_sec,
                        update_settings.backoff_initial_sec + distribution(rnd_engine)));
                failed_loadable_object.second.next_attempt_time = std::chrono::system_clock::now() + delay;

                ++failed_loadable_object.second.error_count;

                std::rethrow_exception(exception_ptr);
            }
            else
            {
                const std::lock_guard<std::mutex> lock{map_mutex};

                const auto & lifetime = loadable_ptr->getLifetime();
                std::uniform_int_distribution<UInt64> distribution{lifetime.min_sec, lifetime.max_sec};
                update_times[name] = std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)};

                const auto dict_it = loadable_objects.find(name);

                dict_it->second.loadable.reset();
                dict_it->second.loadable = std::move(loadable_ptr);

                /// clear stored exception on success
                dict_it->second.exception = std::exception_ptr{};

                recreated_failed_loadable_objects.push_back(name);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed reloading '" + name + "' " + object_name);

            if (throw_on_error)
                throw;
        }
    }

    /// do not undertake further attempts to recreate these loadable objects
    for (const auto & name : recreated_failed_loadable_objects)
        failed_loadable_objects.erase(name);

    /// periodic update
    for (auto & loadable_object : loadable_objects)
    {
        const auto & name = loadable_object.first;

        try
        {
            /// If the loadable objects failed to load or even failed to initialize from the config.
            if (!loadable_object.second.loadable)
                continue;

            auto current = loadable_object.second.loadable;
            const auto & lifetime = current->getLifetime();

            /// do not update loadable objects with zero as lifetime
            if (lifetime.min_sec == 0 || lifetime.max_sec == 0)
                continue;

            if (current->supportUpdates())
            {
                auto & update_time = update_times[current->getName()];

                /// check that timeout has passed
                if (std::chrono::system_clock::now() < update_time)
                    continue;

                SCOPE_EXIT({
                        /// calculate next update time
                        std::uniform_int_distribution<UInt64> distribution{lifetime.min_sec, lifetime.max_sec};
                        update_time = std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)};
                           });

                /// check source modified
                if (current->isModified())
                {
                    /// create new version of loadable object
                    auto new_version = current->clone();

                    if (const auto exception_ptr = new_version->getCreationException())
                        std::rethrow_exception(exception_ptr);

                    loadable_object.second.loadable.reset();
                    loadable_object.second.loadable = std::move(new_version);
                }
            }

            /// erase stored exception on success
            loadable_object.second.exception = std::exception_ptr{};
        }
        catch (...)
        {
            loadable_object.second.exception = std::current_exception();

            tryLogCurrentException(log, "Cannot update " + object_name + " '" + name + "', leaving old version");

            if (throw_on_error)
                throw;
        }
    }
}

void ExternalLoader::reloadFromConfigFiles(const bool throw_on_error, const bool force_reload, const std::string & only_dictionary)
{
    const auto config_paths = config_repository->list(config, config_settings.path_setting_name);

    for (const auto & config_path : config_paths)
    {
        try
        {
            reloadFromConfigFile(config_path, throw_on_error, force_reload, only_dictionary);
        }
        catch (...)
        {
            tryLogCurrentException(log, "reloadFromConfigFile has thrown while reading from " + config_path);

            if (throw_on_error)
                throw;
        }
    }

    /// erase removed from config loadable objects
    std::list<std::string> removed_loadable_objects;
    for (const auto & loadable : loadable_objects)
    {
        const auto & current_config = loadable_objects_defined_in_config[loadable.second.origin];
        if (current_config.find(loadable.first) == std::end(current_config))
            removed_loadable_objects.emplace_back(loadable.first);
    }
    for(const auto & name : removed_loadable_objects)
        loadable_objects.erase(name);
}

void ExternalLoader::reloadFromConfigFile(const std::string & config_path, const bool throw_on_error,
                                          const bool force_reload, const std::string & loadable_name)
{
    if (config_path.empty() || !config_repository->exists(config_path))
    {
        LOG_WARNING(log, "config file '" + config_path + "' does not exist");
    }
    else
    {
        std::unique_lock<std::mutex> all_lock(all_mutex);

        auto modification_time_it = last_modification_times.find(config_path);
        if (modification_time_it == std::end(last_modification_times))
            modification_time_it = last_modification_times.emplace(config_path, Poco::Timestamp{0}).first;
        auto & config_last_modified = modification_time_it->second;

        const auto last_modified = config_repository->getLastModificationTime(config_path);
        if (force_reload || last_modified > config_last_modified)
        {
            auto loaded_config = config_repository->load(config_path);

            loadable_objects_defined_in_config[config_path].clear();

            /// Definitions of loadable objects may have changed, recreate all of them

            /// If we need update only one object, don't update modification time: might be other objects in the config file
            if (loadable_name.empty())
                config_last_modified = last_modified;

            /// get all objects' definitions
            Poco::Util::AbstractConfiguration::Keys keys;
            loaded_config->keys(keys);

            /// for each loadable object defined in xml config
            for (const auto & key : keys)
            {
                std::string name;

                if (!startsWith(key, config_settings.external_config))
                {
                    if (!startsWith(key, "comment") && !startsWith(key, "include_from"))
                        LOG_WARNING(log, config_path << ": unknown node in file: '" << key
                                                     << "', expected '" << config_settings.external_config << "'");
                    continue;
                }

                try
                {
                    name = loaded_config->getString(key + "." + config_settings.external_name);
                    if (name.empty())
                    {
                        LOG_WARNING(log, config_path << ": " + config_settings.external_name + " name cannot be empty");
                        continue;
                    }

                    loadable_objects_defined_in_config[config_path].emplace(name);
                    if (!loadable_name.empty() && name != loadable_name)
                        continue;

                    decltype(loadable_objects.begin()) object_it;
                    {
                        std::lock_guard<std::mutex> lock{map_mutex};
                        object_it = loadable_objects.find(name);
                    }

                    /// Object with the same name was declared in other config file.
                    if (object_it != std::end(loadable_objects) && object_it->second.origin != config_path)
                        throw Exception(object_name + " '" + name + "' from file " + config_path
                                        + " already declared in file " + object_it->second.origin,
                                        ErrorCodes::EXTERNAL_LOADABLE_ALREADY_EXISTS);

                    auto object_ptr = create(name, *loaded_config, key);

                    /// If the object could not be loaded.
                    if (const auto exception_ptr = object_ptr->getCreationException())
                    {
                        std::chrono::seconds delay(update_settings.backoff_initial_sec);
                        const auto failed_dict_it = failed_loadable_objects.find(name);
                        FailedLoadableInfo info{std::move(object_ptr), std::chrono::system_clock::now() + delay, 0};
                        if (failed_dict_it != std::end(failed_loadable_objects))
                            (*failed_dict_it).second = std::move(info);
                        else
                            failed_loadable_objects.emplace(name, std::move(info));

                        std::rethrow_exception(exception_ptr);
                    }
                    else if (object_ptr->supportUpdates())
                    {
                        const auto & lifetime = object_ptr->getLifetime();
                        if (lifetime.min_sec != 0 && lifetime.max_sec != 0)
                        {
                            std::uniform_int_distribution<UInt64> distribution(lifetime.min_sec, lifetime.max_sec);

                            update_times[name] = std::chrono::system_clock::now() +
                                                 std::chrono::seconds{distribution(rnd_engine)};
                        }
                    }

                    const std::lock_guard<std::mutex> lock{map_mutex};

                    /// add new loadable object or update an existing version
                    if (object_it == std::end(loadable_objects))
                        loadable_objects.emplace(name, LoadableInfo{std::move(object_ptr), config_path, {}});
                    else
                    {
                        if (object_it->second.loadable)
                            object_it->second.loadable.reset();
                        object_it->second.loadable = std::move(object_ptr);

                        /// erase stored exception on success
                        object_it->second.exception = std::exception_ptr{};
                        failed_loadable_objects.erase(name);
                    }
                }
                catch (...)
                {
                    if (!name.empty())
                    {
                        /// If the loadable object could not load data or even failed to initialize from the config.
                        /// - all the same we insert information into the `loadable_objects`, with the zero pointer `loadable`.

                        const std::lock_guard<std::mutex> lock{map_mutex};

                        const auto exception_ptr = std::current_exception();
                        const auto loadable_it = loadable_objects.find(name);
                        if (loadable_it == std::end(loadable_objects))
                            loadable_objects.emplace(name, LoadableInfo{nullptr, config_path, exception_ptr});
                        else
                            loadable_it->second.exception = exception_ptr;
                    }

                    tryLogCurrentException(log, "Cannot create " + object_name + " '"
                                                + name + "' from config path " + config_path);

                    /// propagate exception
                    if (throw_on_error)
                        throw;
                }
            }
        }
    }
}

void ExternalLoader::reload()
{
    reloadFromConfigFiles(true, true);
}

void ExternalLoader::reload(const std::string & name)
{
    reloadFromConfigFiles(true, true, name);

    /// Check that specified object was loaded
    const std::lock_guard<std::mutex> lock{map_mutex};
    if (!loadable_objects.count(name))
        throw Exception("Failed to load " + object_name + " '" + name + "' during the reload process", ErrorCodes::BAD_ARGUMENTS);
}

ExternalLoader::LoadablePtr ExternalLoader::getLoadableImpl(const std::string & name, bool throw_on_error) const
{
    const std::lock_guard<std::mutex> lock{map_mutex};

    const auto it = loadable_objects.find(name);
    if (it == std::end(loadable_objects))
    {
        if (throw_on_error)
            throw Exception("No such " + object_name + ": " + name, ErrorCodes::BAD_ARGUMENTS);
        return nullptr;
    }

    if (!it->second.loadable && throw_on_error)
        it->second.exception ? std::rethrow_exception(it->second.exception)
                             : throw Exception{object_name + " '" + name + "' is not loaded", ErrorCodes::LOGICAL_ERROR};

    return it->second.loadable;
}

ExternalLoader::LoadablePtr ExternalLoader::getLoadable(const std::string & name) const
{
    return getLoadableImpl(name, true);
}

ExternalLoader::LoadablePtr ExternalLoader::tryGetLoadable(const std::string & name) const
{
    return getLoadableImpl(name, false);
}

ExternalLoader::LockedObjectsMap ExternalLoader::getObjectsMap() const
{
    return LockedObjectsMap(map_mutex, loadable_objects);
}

}
