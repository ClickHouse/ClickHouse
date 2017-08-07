#include <Interpreters/ExternalDictionaries.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionarySource.h>
#include <Common/StringUtils.h>
#include <Common/MemoryTracker.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <ext/scope_guard.h>
#include <Poco/Util/Application.h>
#include <Poco/Glob.h>
#include <Poco/File.h>


namespace
{
    const auto check_period_sec = 5;
    const auto backoff_initial_sec = 5;
    /// 10 minutes
    const auto backoff_max_sec = 10 * 60;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


void ExternalDictionaries::reloadPeriodically()
{
    setThreadName("ExterDictReload");

    while (true)
    {
        if (destroy.tryWait(check_period_sec * 1000))
            return;

        reloadImpl();
    }
}


ExternalDictionaries::ExternalDictionaries(Context & context, const bool throw_on_error)
    : context(context), log(&Logger::get("ExternalDictionaries"))
{
    {
        /** During synchronous loading of external dictionaries at moment of query execution,
            *  we should not use per query memory limit.
            */
        TemporarilyDisableMemoryTracker temporarily_disable_memory_tracker;

        reloadImpl(throw_on_error);
    }

    reloading_thread = std::thread{&ExternalDictionaries::reloadPeriodically, this};
}


ExternalDictionaries::~ExternalDictionaries()
{
    destroy.set();
    reloading_thread.join();
}

namespace
{
std::set<std::string> getDictionariesConfigPaths(const Poco::Util::AbstractConfiguration & config)
{
    std::set<std::string> files;
    auto patterns = getMultipleValuesFromConfig(config, "", "dictionaries_config");
    for (auto & pattern : patterns)
    {
        if (pattern.empty())
            continue;

        if (pattern[0] != '/')
        {
            const auto app_config_path = config.getString("config-file", "config.xml");
            const auto config_dir = Poco::Path{app_config_path}.parent().toString();
            const auto absolute_path = config_dir + pattern;
            Poco::Glob::glob(absolute_path, files, 0);
            if (!files.empty())
                continue;
        }

        Poco::Glob::glob(pattern, files, 0);
    }

    return files;
}
}

void ExternalDictionaries::reloadImpl(const bool throw_on_error)
{
    const auto config_paths = getDictionariesConfigPaths(Poco::Util::Application::instance().config());

    for (const auto & config_path : config_paths)
    {
        try
        {
            reloadFromFile(config_path, throw_on_error);
        }
        catch (...)
        {
            tryLogCurrentException(log, "reloadFromFile has thrown while reading from " + config_path);

            if (throw_on_error)
                throw;
        }
    }

    /// list of recreated dictionaries to perform delayed removal from unordered_map
    std::list<std::string> recreated_failed_dictionaries;

    /// retry loading failed dictionaries
    for (auto & failed_dictionary : failed_dictionaries)
    {
        if (std::chrono::system_clock::now() < failed_dictionary.second.next_attempt_time)
            continue;

        const auto & name = failed_dictionary.first;

        try
        {
            auto dict_ptr = failed_dictionary.second.dict->clone();
            if (const auto exception_ptr = dict_ptr->getCreationException())
            {
                /// recalculate next attempt time
                std::uniform_int_distribution<UInt64> distribution(
                    0, std::exp2(failed_dictionary.second.error_count));

                failed_dictionary.second.next_attempt_time = std::chrono::system_clock::now() +
                    std::chrono::seconds{
                        std::min<UInt64>(backoff_max_sec, backoff_initial_sec + distribution(rnd_engine))};

                ++failed_dictionary.second.error_count;

                std::rethrow_exception(exception_ptr);
            }
            else
            {
                const std::lock_guard<std::mutex> lock{dictionaries_mutex};

                const auto & lifetime = dict_ptr->getLifetime();
                std::uniform_int_distribution<UInt64> distribution{lifetime.min_sec, lifetime.max_sec};
                update_times[name] = std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)};

                const auto dict_it = dictionaries.find(name);
                if (dict_it->second.dict)
                    dict_it->second.dict->set(dict_ptr.release());
                else
                    dict_it->second.dict = std::make_shared<MultiVersion<IDictionaryBase>>(dict_ptr.release());

                /// erase stored exception on success
                dict_it->second.exception = std::exception_ptr{};

                recreated_failed_dictionaries.push_back(name);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed reloading '" + name + "' dictionary");

            if (throw_on_error)
                throw;
        }
    }

    /// do not undertake further attempts to recreate these dictionaries
    for (const auto & name : recreated_failed_dictionaries)
        failed_dictionaries.erase(name);

    /// periodic update
    for (auto & dictionary : dictionaries)
    {
        const auto & name = dictionary.first;

        try
        {
            /// If the dictionary failed to load or even failed to initialize from the config.
            if (!dictionary.second.dict)
                continue;

            auto current = dictionary.second.dict->get();
            const auto & lifetime = current->getLifetime();

            /// do not update dictionaries with zero as lifetime
            if (lifetime.min_sec == 0 || lifetime.max_sec == 0)
                continue;

            /// update only non-cached dictionaries
            if (!current->isCached())
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
                if (current->getSource()->isModified())
                {
                    /// create new version of dictionary
                    auto new_version = current->clone();

                    if (const auto exception_ptr = new_version->getCreationException())
                        std::rethrow_exception(exception_ptr);

                    dictionary.second.dict->set(new_version.release());
                }
            }

            /// erase stored exception on success
            dictionary.second.exception = std::exception_ptr{};
        }
        catch (...)
        {
            dictionary.second.exception = std::current_exception();

            tryLogCurrentException(log, "Cannot update external dictionary '" + name + "', leaving old version");

            if (throw_on_error)
                throw;
        }
    }
}

void ExternalDictionaries::reloadFromFile(const std::string & config_path, const bool throw_on_error)
{
    const Poco::File config_file{config_path};

    if (config_path.empty() || !config_file.exists())
    {
        LOG_WARNING(log, "config file '" + config_path + "' does not exist");
    }
    else
    {
        auto modification_time_it = last_modification_times.find(config_path);
        if (modification_time_it == std::end(last_modification_times))
            modification_time_it = last_modification_times.emplace(config_path, Poco::Timestamp{0}).first;
        auto & config_last_modified = modification_time_it->second;

        const auto last_modified = config_file.getLastModified();
        if (last_modified > config_last_modified)
        {
            Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(config_path);

            /// definitions of dictionaries may have changed, recreate all of them
            config_last_modified = last_modified;

            /// get all dictionaries' definitions
            Poco::Util::AbstractConfiguration::Keys keys;
            config->keys(keys);

            /// for each dictionary defined in xml config
            for (const auto & key : keys)
            {
                std::string name;

                if (!startsWith(key, "dictionary"))
                {
                    if (!startsWith(key.data(), "comment"))
                        LOG_WARNING(log,
                            config_path << ": unknown node in dictionaries file: '" << key + "', 'dictionary'");

                    continue;
                }

                try
                {
                    name = config->getString(key + ".name");
                    if (name.empty())
                    {
                        LOG_WARNING(log, config_path << ": dictionary name cannot be empty");
                        continue;
                    }

                    const auto dict_it = dictionaries.find(name);
                    if (dict_it != std::end(dictionaries))
                        if (dict_it->second.origin != config_path)
                            throw std::runtime_error{"Overriding dictionary from file " + dict_it->second.origin};

                    auto dict_ptr = DictionaryFactory::instance().create(name, *config, key, context);

                    /// If the dictionary could not be loaded.
                    if (const auto exception_ptr = dict_ptr->getCreationException())
                    {
                        const auto failed_dict_it = failed_dictionaries.find(name);
                        if (failed_dict_it != std::end(failed_dictionaries))
                        {
                            failed_dict_it->second = FailedDictionaryInfo{
                                std::move(dict_ptr),
                                std::chrono::system_clock::now() + std::chrono::seconds{backoff_initial_sec}
                            };
                        }
                        else
                            failed_dictionaries.emplace(name, FailedDictionaryInfo{
                                std::move(dict_ptr),
                                std::chrono::system_clock::now() + std::chrono::seconds{backoff_initial_sec}
                            });

                        std::rethrow_exception(exception_ptr);
                    }

                    if (!dict_ptr->isCached())
                    {
                        const auto & lifetime = dict_ptr->getLifetime();
                        if (lifetime.min_sec != 0 && lifetime.max_sec != 0)
                        {
                            std::uniform_int_distribution<UInt64> distribution{
                                lifetime.min_sec,
                                lifetime.max_sec
                            };
                            update_times[name] = std::chrono::system_clock::now() +
                                std::chrono::seconds{distribution(rnd_engine)};
                        }
                    }

                    const std::lock_guard<std::mutex> lock{dictionaries_mutex};

                    /// add new dictionary or update an existing version
                    if (dict_it == std::end(dictionaries))
                        dictionaries.emplace(name, DictionaryInfo{
                            std::make_shared<MultiVersion<IDictionaryBase>>(dict_ptr.release()),
                            config_path
                        });
                    else
                    {
                        if (dict_it->second.dict)
                            dict_it->second.dict->set(dict_ptr.release());
                        else
                            dict_it->second.dict = std::make_shared<MultiVersion<IDictionaryBase>>(dict_ptr.release());

                        /// erase stored exception on success
                        dict_it->second.exception = std::exception_ptr{};
                        failed_dictionaries.erase(name);
                    }
                }
                catch (...)
                {
                    if (!name.empty())
                    {
                        /// If the dictionary could not load data or even failed to initialize from the config.
                        /// - all the same we insert information into the `dictionaries`, with the zero pointer `dict`.

                        const std::lock_guard<std::mutex> lock{dictionaries_mutex};

                        const auto exception_ptr = std::current_exception();
                        const auto dict_it = dictionaries.find(name);
                        if (dict_it == std::end(dictionaries))
                            dictionaries.emplace(name, DictionaryInfo{nullptr, config_path, exception_ptr});
                        else
                            dict_it->second.exception = exception_ptr;
                    }

                    tryLogCurrentException(log, "Cannot create external dictionary '" + name + "' from config path " + config_path);

                    /// propagate exception
                    if (throw_on_error)
                        throw;
                }
            }
        }
    }
}

MultiVersion<IDictionaryBase>::Version ExternalDictionaries::getDictionary(const std::string & name) const
{
    const std::lock_guard<std::mutex> lock{dictionaries_mutex};

    const auto it = dictionaries.find(name);
    if (it == std::end(dictionaries))
        throw Exception{
            "No such dictionary: " + name,
            ErrorCodes::BAD_ARGUMENTS
        };

    if (!it->second.dict)
        it->second.exception ? std::rethrow_exception(it->second.exception) :
            throw Exception{"No dictionary", ErrorCodes::LOGICAL_ERROR};

    return it->second.dict->get();
}

}
