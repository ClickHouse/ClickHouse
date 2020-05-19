#include "NamesProvider.h"

#include <IO/ReadBufferFromFile.h>
#include "NamesFormatReader.h"


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
    auto file_reader = std::make_shared<DB::ReadBufferFromFile>(path);
    return std::make_unique<LanguageRegionsNamesFormatReader>(std::move(file_reader));
}

std::string LanguageRegionsNamesDataSource::getLanguage() const
{
    return language;
}

std::string LanguageRegionsNamesDataSource::getSourceName() const
{
    return path;
}


RegionsNamesDataProvider::RegionsNamesDataProvider(const std::string & directory_) : directory(directory_)
{
}

ILanguageRegionsNamesDataSourcePtr RegionsNamesDataProvider::getLanguageRegionsNamesSource(const std::string & language) const
{
    const auto data_file = getDataFilePath(language);
    if (Poco::File(data_file).exists())
        return std::make_unique<LanguageRegionsNamesDataSource>(data_file, language);
    else
        return {};
}

std::string RegionsNamesDataProvider::getDataFilePath(const std::string & language) const
{
    return directory + "/regions_names_" + language + ".txt";
}
