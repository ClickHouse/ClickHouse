#include <Dictionaries/Embedded/GeodataProviders/HierarchiesProvider.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Poco/Util/Application.h>
#include <Poco/Exception.h>
#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>


class RegionsHierarchyFileReader : public IRegionsHierarchyReader
{
private:
    DB::ReadBufferFromFile in;

public:
    RegionsHierarchyFileReader(const std::string & path)
        : in(path)
    {}

    bool readNext(RegionEntry & entry) override;
};

bool RegionsHierarchyFileReader::readNext(RegionEntry & entry)
{
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

        RegionType type = static_cast<RegionType>(read_type);

        entry.id = region_id;
        entry.parent_id = parent_id;
        entry.type = type;
        entry.population = population;
        return true;
    }

    return false;
}


bool RegionsHierarchyDataSource::isModified() const
{
    return updates_tracker.isModified();
}

IRegionsHierarchyReaderPtr RegionsHierarchyDataSource::createReader()
{
    updates_tracker.fixCurrentVersion();
    return std::make_unique<RegionsHierarchyFileReader>(path);
}


RegionsHierarchiesDataProvider::RegionsHierarchiesDataProvider(const std::string & path)
    : path(path)
{
    discoverFilesWithCustomHierarchies();
}

void RegionsHierarchiesDataProvider::discoverFilesWithCustomHierarchies()
{
    std::string basename = Poco::Path(path).getBaseName();

    Poco::Path dir_path = Poco::Path(path).absolute().parent();

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(dir_path); dir_it != dir_end; ++dir_it)
    {
        std::string candidate_basename = dir_it.path().getBaseName();

        if ((0 == candidate_basename.compare(0, basename.size(), basename)) &&
            (candidate_basename.size() > basename.size() + 1) &&
            (candidate_basename[basename.size()] == '_'))
        {
            const std::string suffix = candidate_basename.substr(basename.size() + 1);
            hierarchy_files.emplace(suffix, dir_it->path());
        }
    }
}

std::vector<std::string> RegionsHierarchiesDataProvider::listCustomHierarchies() const
{
    std::vector<std::string> names;
    names.reserve(hierarchy_files.size());
    for (const auto & it : hierarchy_files)
        names.push_back(it.first);
    return names;
}

IRegionsHierarchyDataSourcePtr RegionsHierarchiesDataProvider::getDefaultHierarchySource() const
{
    return std::make_shared<RegionsHierarchyDataSource>(path);
}

IRegionsHierarchyDataSourcePtr RegionsHierarchiesDataProvider::getHierarchySource(const std::string & name) const
{
    auto found = hierarchy_files.find(name);

    if (found != hierarchy_files.end())
    {
        const auto & hierarchy_file_path = found->second;
        return std::make_shared<RegionsHierarchyDataSource>(hierarchy_file_path);
    }

    throw Poco::Exception("Regions hierarchy `" + name + "` not found");
}
