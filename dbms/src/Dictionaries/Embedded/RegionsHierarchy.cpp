#include <Dictionaries/Embedded/RegionsHierarchy.h>

#include <Poco/Util/Application.h>
#include <Poco/Exception.h>
#include <Poco/File.h>

#include <common/logger_useful.h>
#include <ext/singleton.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


static constexpr auto config_key = "path_to_regions_hierarchy_file";


RegionsHierarchy::RegionsHierarchy()
{
    path = Poco::Util::Application::instance().config().getString(config_key);
}

RegionsHierarchy::RegionsHierarchy(const std::string & path_)
{
    path = path_;
}


void RegionsHierarchy::reload()
{
    Logger * log = &Logger::get("RegionsHierarchy");

    time_t new_modification_time = Poco::File(path).getLastModified().epochTime();
    if (new_modification_time <= file_modification_time)
        return;
    file_modification_time = new_modification_time;

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

    DB::ReadBufferFromFile in(path);

    RegionID max_region_id = 0;
    while (!in.eof())
    {
        /** Our internal geobase has negative numbers,
            *  that means "this is garbage, ignore this row".
            */
        Int32 read_region_id = 0;
        Int32 read_parent_id = 0;
        Int8 read_type = 0;

        DB::readIntText(read_region_id, in);
        DB::assertChar('\t', in);
        DB::readIntText(read_parent_id, in);
        DB::assertChar('\t', in);
        DB::readIntText(read_type, in);

        /** Then there can be a newline (old version)
            *  or tab, the region's population, line feed (new version).
            */
        RegionPopulation population = 0;
        if (!in.eof() && *in.position() == '\t')
        {
            ++in.position();
            UInt64 population_big = 0;
            DB::readIntText(population_big, in);
            population = population_big > std::numeric_limits<RegionPopulation>::max()
                ? std::numeric_limits<RegionPopulation>::max()
                : population_big;
        }
        DB::assertChar('\n', in);

        if (read_region_id <= 0 || read_type < 0)
            continue;

        RegionID region_id = read_region_id;
        RegionID parent_id = 0;

        if (read_parent_id >= 0)
            parent_id = read_parent_id;

        RegionType type = read_type;

        if (region_id > max_region_id)
        {
            if (region_id > max_size)
                throw DB::Exception("Region id is too large: " + DB::toString(region_id) + ", should be not more than " + DB::toString(max_size));

            max_region_id = region_id;

            while (region_id >= new_parents.size())
            {
                new_parents.resize(new_parents.size() * 2);
                new_populations.resize(new_parents.size());
                types.resize(new_parents.size());
            }
        }

        new_parents[region_id] = parent_id;
        new_populations[region_id] = population;
        types[region_id] = type;
    }

    new_parents        .resize(max_region_id + 1);
    new_city        .resize(max_region_id + 1);
    new_country        .resize(max_region_id + 1);
    new_area        .resize(max_region_id + 1);
    new_district    .resize(max_region_id + 1);
    new_continent    .resize(max_region_id + 1);
    new_top_continent.resize(max_region_id + 1);
    new_populations .resize(max_region_id + 1);
    new_depths        .resize(max_region_id + 1);
    types            .resize(max_region_id + 1);

    /// prescribe the cities and countries for the regions
    for (RegionID i = 0; i <= max_region_id; ++i)
    {
        if (types[i] == REGION_TYPE_CITY)
            new_city[i] = i;

        if (types[i] == REGION_TYPE_AREA)
            new_area[i] = i;

        if (types[i] == REGION_TYPE_DISTRICT)
            new_district[i] = i;

        if (types[i] == REGION_TYPE_COUNTRY)
            new_country[i] = i;

        if (types[i] == REGION_TYPE_CONTINENT)
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

            if (types[current] == REGION_TYPE_CITY)
                new_city[i] = current;

            if (types[current] == REGION_TYPE_AREA)
                new_area[i] = current;

            if (types[current] == REGION_TYPE_DISTRICT)
                new_district[i] = current;

            if (types[current] == REGION_TYPE_COUNTRY)
                new_country[i] = current;

            if (types[current] == REGION_TYPE_CONTINENT)
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
