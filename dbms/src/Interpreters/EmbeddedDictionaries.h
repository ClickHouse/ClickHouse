#pragma once

#include <thread>
#include <common/MultiVersion.h>
#include <Poco/Event.h>


namespace Poco { class Logger; }

class RegionsHierarchies;
class TechDataHierarchy;
class RegionsNames;


namespace DB
{

class Context;


/// Metrica's Dictionaries which can be used in functions.

class EmbeddedDictionaries
{
private:
    MultiVersion<RegionsHierarchies> regions_hierarchies;
    MultiVersion<TechDataHierarchy> tech_data_hierarchy;
    MultiVersion<RegionsNames> regions_names;

    /// Directories' updating periodicity (in seconds).
    int reload_period;
    int cur_reload_period = 1;
    bool is_fast_start_stage = true;

    std::thread reloading_thread;
    Poco::Event destroy;

    Poco::Logger * log;


    void handleException(const bool throw_on_error) const;

    /// Updates dictionaries.
    bool reloadImpl(const bool throw_on_error);

    /** Updates directories (dictionaries) every reload_period seconds.
     * If all dictionaries are not loaded at least once, try reload them with exponential delay (1, 2, ... reload_period).
     * If all dictionaries are loaded, update them using constant reload_period delay.
     */
    void reloadPeriodically();

    template <typename Dictionary>
    bool reloadDictionary(MultiVersion<Dictionary> & dictionary, const bool throw_on_error);

public:
    /// Every reload_period seconds directories are updated inside a separate thread.
    EmbeddedDictionaries(const bool throw_on_error, const int reload_period_);

    EmbeddedDictionaries(const bool throw_on_error);

    ~EmbeddedDictionaries();


    MultiVersion<RegionsHierarchies>::Version getRegionsHierarchies() const
    {
        return regions_hierarchies.get();
    }

    MultiVersion<TechDataHierarchy>::Version getTechDataHierarchy() const
    {
        return tech_data_hierarchy.get();
    }

    MultiVersion<RegionsNames>::Version getRegionsNames() const
    {
        return regions_names.get();
    }
};

}
