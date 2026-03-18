#pragma once

#include "IHierarchiesProvider.h"

#include <unordered_map>
#include <Common/FileUpdatesTracker.h>

namespace DB
{

// Represents local file with regions hierarchy dump
class RegionsHierarchyDataSource : public IRegionsHierarchyDataSource
{
private:
    std::string path;
    FileUpdatesTracker updates_tracker;

public:
    explicit RegionsHierarchyDataSource(const std::string & path_) : path(path_), updates_tracker(path_) {}

    bool isModified() const override;

    IRegionsHierarchyReaderPtr createReader() override;
};


// Provides access to directory with multiple data source files: one file per regions hierarchy
class RegionsHierarchiesDataProvider : public IRegionsHierarchiesDataProvider
{
private:
    // path to file with default regions hierarchy
    std::string path;

    using HierarchyFiles = std::unordered_map<std::string, std::string>;
    HierarchyFiles hierarchy_files;

public:
    /** path must point to the file with the hierarchy of regions "by default". It will be accessible by an empty key.
      * In addition, a number of files are searched for, the name of which (before the extension, if any) is added arbitrary _suffix.
      * Such files are loaded, and the hierarchy of regions is put on the `suffix` key.
      *
      * For example, if /opt/geo/regions_hierarchy.txt is specified,
      *  then the /opt/geo/regions_hierarchy_ua.txt file will also be loaded, if any, it will be accessible by the `ua` key.
      */
    explicit RegionsHierarchiesDataProvider(const std::string & path_);

    std::vector<std::string> listCustomHierarchies() const override;

    IRegionsHierarchyDataSourcePtr getDefaultHierarchySource() const override;
    IRegionsHierarchyDataSourcePtr getHierarchySource(const std::string & name) const override;

private:
    void discoverFilesWithCustomHierarchies();
};

}
