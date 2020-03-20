#pragma once

#include <Common/FileUpdatesTracker.h>
#include "INamesProvider.h"


// Represents local file with list of regions ids / names
class LanguageRegionsNamesDataSource : public ILanguageRegionsNamesDataSource
{
private:
    std::string path;
    FileUpdatesTracker updates_tracker;
    std::string language;

public:
    LanguageRegionsNamesDataSource(std::string path_, std::string language_)
        : path(std::move(path_)), updates_tracker(path), language(std::move(language_))
    {
    }

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
    RegionsNamesDataProvider(std::string directory_);

    ILanguageRegionsNamesDataSourcePtr getLanguageRegionsNamesSource(const std::string & language) const override;

private:
    std::string getDataFilePath(const std::string & language) const;
};
