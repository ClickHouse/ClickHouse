#include <Dictionaries/Embedded/GeodataProviders/NamesProvider.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


class LanguageRegionsNamesFileReader : public ILanguageRegionsNamesReader
{
private:
    DB::ReadBufferFromFile in;

public:
    LanguageRegionsNamesFileReader(const std::string & path)
        : in(path)
    {}

    bool readNext(RegionNameEntry & entry) override;
};

bool LanguageRegionsNamesFileReader::readNext(RegionNameEntry & entry)
{
    while (!in.eof())
    {
        Int32 read_region_id;
        std::string region_name;

        DB::readIntText(read_region_id, in);
        DB::assertChar('\t', in);
        DB::readString(region_name, in);
        DB::assertChar('\n', in);

        if (read_region_id <= 0)
            continue;

        entry.id = read_region_id;
        entry.name = region_name;
        return true;
    }

    return false;
}


bool LanguageRegionsNamesDataSource::isModified() const
{
    return updates_tracker.isModified();
}

size_t LanguageRegionsNamesDataSource::estimateTotalSize() const
{
    return Poco::File(path).getSize();
}

ILanguageRegionsNamesReaderPtr LanguageRegionsNamesDataSource::createReader()
{
    updates_tracker.fixCurrentVersion();
    return std::make_unique<LanguageRegionsNamesFileReader>(path);
}

std::string LanguageRegionsNamesDataSource::getLanguage() const
{
    return language;
}

std::string LanguageRegionsNamesDataSource::getSourceName() const
{
    return path;
}


RegionsNamesDataProvider::RegionsNamesDataProvider(const std::string & directory_)
    : directory(directory_)
{}

ILanguageRegionsNamesDataSourcePtr RegionsNamesDataProvider::getLanguageRegionsNamesSource(
    const std::string & language) const
{
    const auto data_file = getDataFilePath(language);
    return std::make_unique<LanguageRegionsNamesDataSource>(data_file, language);
}

std::string RegionsNamesDataProvider::getDataFilePath(const std::string & language) const
{
    return directory + "/regions_names_" + language + ".txt";
}
