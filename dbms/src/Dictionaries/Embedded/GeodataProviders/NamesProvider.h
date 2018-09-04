#pragma once

#include <Dictionaries/Embedded/GeodataProviders/INamesProvider.h>

#include <Common/FileUpdatesTracker.h>


// Represents local file with list of regions ids / names
class LanguageRegionsNamesDataSource : public ILanguageRegionsNamesDataSource
{
private:
    std::string path;
    FileUpdatesTracker updates_tracker;
    std::string language;

public:
    LanguageRegionsNamesDataSource(const std::string & path_, const std::string & language_)
        : path(path_)
        , updates_tracker(path_)
        , language(language_)
    {}

    bool isModified() const override;

    size_t estimateTotalSize() const override;

    ILanguageRegionsNamesReaderPtr createReader() override;

    std::string getLanguage() const override;

    std::string getSourceName() const override;
};

using ILanguageRegionsNamesDataSourcePtr = std::unique_ptr<ILanguageRegionsNamesDataSource>;


// Provides access to directory with multiple data source files: one file per language
class RegionsNamesDataProvider : public IRegionsNamesDataProvider
{
private:
    std::string directory;

public:
    RegionsNamesDataProvider(const std::string & directory_);

    ILanguageRegionsNamesDataSourcePtr getLanguageRegionsNamesSource(
        const std::string & language) const override;

private:
    std::string getDataFilePath(const std::string & language) const;
};
