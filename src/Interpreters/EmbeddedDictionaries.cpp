#include <Dictionaries/Embedded/RegionsHierarchies.h>
#include <Dictionaries/Embedded/RegionsNames.h>
#include <Dictionaries/Embedded/GeoDictionariesLoader.h>
#include <Interpreters/Context.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <base/logger_useful.h>
#include <Poco/Util/Application.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNFINISHED;
}

void EmbeddedDictionaries::handleException(const bool throw_on_error) const
{
    const auto exception_ptr = std::current_exception();

    tryLogCurrentException(log, "Cannot load dictionary! You must resolve this manually.");

    if (throw_on_error)
        std::rethrow_exception(exception_ptr);
}


template <typename Dictionary>
bool EmbeddedDictionaries::reloadDictionary(
    MultiVersion<Dictionary> & dictionary,
    DictionaryReloader<Dictionary> reload_dictionary,
    const bool throw_on_error,
    const bool force_reload)
{
    const auto & config = getContext()->getConfigRef();

    bool not_initialized = dictionary.get() == nullptr;

    if (force_reload || !is_fast_start_stage || not_initialized)
    {
        try
        {
            auto new_dictionary = reload_dictionary(config);
            if (new_dictionary)
                dictionary.set(std::move(new_dictionary));
        }
        catch (...)
        {
            handleException(throw_on_error);
            return false;
        }
    }

    return true;
}


bool EmbeddedDictionaries::reloadImpl(const bool throw_on_error, const bool force_reload)
{
    std::unique_lock lock(mutex);

    /** If you can not update the directories, then despite this, do not throw an exception (use the old directories).
      * If there are no old correct directories, then when using functions that depend on them,
      *  will throw an exception.
      * An attempt is made to load each directory separately.
      */

    LOG_INFO(log, "Loading dictionaries.");

    bool was_exception = false;

    DictionaryReloader<RegionsHierarchies> reload_regions_hierarchies = [=, this] (const Poco::Util::AbstractConfiguration & config)
    {
        return geo_dictionaries_loader->reloadRegionsHierarchies(config);
    };

    if (!reloadDictionary<RegionsHierarchies>(regions_hierarchies, std::move(reload_regions_hierarchies), throw_on_error, force_reload))
        was_exception = true;

    DictionaryReloader<RegionsNames> reload_regions_names = [=, this] (const Poco::Util::AbstractConfiguration & config)
    {
        return geo_dictionaries_loader->reloadRegionsNames(config);
    };

    if (!reloadDictionary<RegionsNames>(regions_names, std::move(reload_regions_names), throw_on_error, force_reload))
        was_exception = true;

    if (!was_exception)
        LOG_INFO(log, "Loaded dictionaries.");

    return !was_exception;
}


void EmbeddedDictionaries::reloadPeriodically()
{
    setThreadName("DictReload");

    while (true)
    {
        if (destroy.tryWait(cur_reload_period * 1000))
            return;

        if (reloadImpl(false))
        {
            /// Success
            cur_reload_period = reload_period;
            is_fast_start_stage = false;
        }

        if (is_fast_start_stage)
        {
            cur_reload_period = std::min(reload_period, 2 * cur_reload_period); /// exponentially increase delay
            is_fast_start_stage = cur_reload_period < reload_period; /// leave fast start state
        }
    }
}


EmbeddedDictionaries::EmbeddedDictionaries(
    std::unique_ptr<GeoDictionariesLoader> geo_dictionaries_loader_,
    ContextPtr context_,
    const bool throw_on_error)
    : WithContext(context_)
    , log(&Poco::Logger::get("EmbeddedDictionaries"))
    , geo_dictionaries_loader(std::move(geo_dictionaries_loader_))
    , reload_period(getContext()->getConfigRef().getInt("builtin_dictionaries_reload_interval", 3600))
{
    reloadImpl(throw_on_error);
    reloading_thread = ThreadFromGlobalPool([this] { reloadPeriodically(); });
}


EmbeddedDictionaries::~EmbeddedDictionaries()
{
    destroy.set();
    reloading_thread.join();
}

void EmbeddedDictionaries::reload()
{
    if (!reloadImpl(true, true))
        throw Exception("Some embedded dictionaries were not successfully reloaded", ErrorCodes::UNFINISHED);
}


}
