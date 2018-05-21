#include <Dictionaries/Embedded/RegionsHierarchy.h>
#include <Dictionaries/Embedded/GeodataProviders/IHierarchiesProvider.h>

#include <Poco/Util/Application.h>
#include <Poco/Exception.h>

#include <common/logger_useful.h>
#include <ext/singleton.h>

#include <IO/WriteHelpers.h>


RegionsHierarchy::RegionsHierarchy(IRegionsHierarchyDataSourcePtr data_source_)
    : data_source(data_source_)
{
}


void RegionsHierarchy::reload()
{
    Logger * log = &Logger::get("RegionsHierarchy");

    if (!data_source->isModified())
        return;

    LOG_DEBUG(log, "Reloading regions hierarchy");

    const size_t initial_size = 10000;
    const size_t max_size = 15000000;

    RegionParents new_parents(initial_size);
    RegionParents new_city(initial_size);
    RegionParents new_country(initial_size);
    RegionParents new_area(initial_size);
    RegionParents new_district(initial_size);
    RegionParents new_continent(initial_size);
    RegionParents new_top_continent(initial_size);
    RegionPopulations new_populations(initial_size);
    RegionDepths  new_depths(initial_size);
    RegionTypes types(initial_size);

    RegionID max_region_id = 0;


    auto regions_reader = data_source->createReader();

    RegionEntry region_entry;
    while (regions_reader->readNext(region_entry))
    {
        if (region_entry.id > max_region_id)
        {
            if (region_entry.id > max_size)
                throw DB::Exception("Region id is too large: " + DB::toString(region_entry.id) + ", should be not more than " + DB::toString(max_size));

            max_region_id = region_entry.id;

            while (region_entry.id >= new_parents.size())
            {
                new_parents.resize(new_parents.size() * 2);
                new_populations.resize(new_parents.size());
                types.resize(new_parents.size());
            }
        }

        new_parents[region_entry.id] = region_entry.parent_id;
        new_populations[region_entry.id] = region_entry.population;
        types[region_entry.id] = region_entry.type;
    }

    new_parents      .resize(max_region_id + 1);
    new_city         .resize(max_region_id + 1);
    new_country      .resize(max_region_id + 1);
    new_area         .resize(max_region_id + 1);
    new_district     .resize(max_region_id + 1);
    new_continent    .resize(max_region_id + 1);
    new_top_continent.resize(max_region_id + 1);
    new_populations  .resize(max_region_id + 1);
    new_depths       .resize(max_region_id + 1);
    types            .resize(max_region_id + 1);

    /// prescribe the cities and countries for the regions
    for (RegionID i = 0; i <= max_region_id; ++i)
    {
        if (types[i] == RegionType::City)
            new_city[i] = i;

        if (types[i] == RegionType::Area)
            new_area[i] = i;

        if (types[i] == RegionType::District)
            new_district[i] = i;

        if (types[i] == RegionType::Country)
            new_country[i] = i;

        if (types[i] == RegionType::Continent)
        {
            new_continent[i] = i;
            new_top_continent[i] = i;
        }

        RegionDepth depth = 0;
        RegionID current = i;
        while (true)
        {
            ++depth;

            if (depth == std::numeric_limits<RegionDepth>::max())
                throw Poco::Exception("Logical error in regions hierarchy: region " + DB::toString(current) + " possible is inside infinite loop");

            current = new_parents[current];
            if (current == 0)
                break;

            if (current > max_region_id)
                throw Poco::Exception("Logical error in regions hierarchy: region " + DB::toString(current) + " (specified as parent) doesn't exist");

            if (types[current] == RegionType::City)
                new_city[i] = current;

            if (types[current] == RegionType::Area)
                new_area[i] = current;

            if (types[current] == RegionType::District)
                new_district[i] = current;

            if (types[current] == RegionType::Country)
                new_country[i] = current;

            if (types[current] == RegionType::Continent)
            {
                if (!new_continent[i])
                    new_continent[i] = current;
                new_top_continent[i] = current;
            }
        }

        new_depths[i] = depth;
    }

    parents.swap(new_parents);
    country.swap(new_country);
    city.swap(new_city);
    area.swap(new_area);
    district.swap(new_district);
    continent.swap(new_continent);
    top_continent.swap(new_top_continent);
    populations.swap(new_populations);
    depths.swap(new_depths);
}
