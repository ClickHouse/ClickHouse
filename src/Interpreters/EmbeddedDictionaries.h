#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/MultiVersion.h>
#include <Common/ThreadPool.h>

#include <Poco/Event.h>

#include <thread>
#include <functional>


namespace Poco { class Logger; namespace Util { class AbstractConfiguration; } }

namespace DB
{

class RegionsHierarchies;
class RegionsNames;
class GeoDictionariesLoader;

/// Metrica's Dictionaries which can be used in functions.

class EmbeddedDictionaries : WithContext
{
private:
    LoggerPtr log;

    MultiVersion<RegionsHierarchies> regions_hierarchies;
    MultiVersion<RegionsNames> regions_names;

    std::unique_ptr<GeoDictionariesLoader> geo_dictionaries_loader;

    /// Directories' updating periodicity (in seconds).
    int reload_period;
    int cur_reload_period = 1;
    bool is_fast_start_stage = true;

    mutable std::mutex mutex;

    ThreadFromGlobalPool reloading_thread;
    Poco::Event destroy;


    void handleException(bool throw_on_error) const;

    /** Updates directories (dictionaries) every reload_period seconds.
     * If all dictionaries are not loaded at least once, try reload them with exponential delay (1, 2, ... reload_period).
     * If all dictionaries are loaded, update them using constant reload_period delay.
     */
    void reloadPeriodically();

    /// Updates dictionaries.
    bool reloadImpl(bool throw_on_error, bool force_reload = false);

    template <typename Dictionary>
    using DictionaryReloader = std::function<std::unique_ptr<Dictionary>(const Poco::Util::AbstractConfiguration & config)>;

    template <typename Dictionary>
    bool reloadDictionary(
        MultiVersion<Dictionary> & dictionary,
        DictionaryReloader<Dictionary> reload_dictionary,
        bool throw_on_error,
        bool force_reload);

public:
    /// Every reload_period seconds directories are updated inside a separate thread.
    EmbeddedDictionaries(
        std::unique_ptr<GeoDictionariesLoader> geo_dictionaries_loader,
        ContextPtr context,
        bool throw_on_error);

    /// Forcibly reloads all dictionaries.
    void reload();

    ~EmbeddedDictionaries();


    MultiVersion<RegionsHierarchies>::Version getRegionsHierarchies() const
    {
        return regions_hierarchies.get();
    }

    MultiVersion<RegionsNames>::Version getRegionsNames() const
    {
        return regions_names.get();
    }
};

}
