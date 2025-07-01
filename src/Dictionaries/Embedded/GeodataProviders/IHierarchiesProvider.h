#pragma once

#include <memory>
#include <string>
#include <vector>
#include <Dictionaries/Embedded/GeodataProviders/Entries.h>

namespace DB
{

// Iterates over all regions in data source
class IRegionsHierarchyReader
{
public:
    virtual bool readNext(RegionEntry & entry) = 0;

    virtual ~IRegionsHierarchyReader() = default;
};

using IRegionsHierarchyReaderPtr = std::unique_ptr<IRegionsHierarchyReader>;


// Data source for single regions hierarchy
class IRegionsHierarchyDataSource
{
public:
    // data modified since last createReader invocation
    virtual bool isModified() const = 0;

    virtual IRegionsHierarchyReaderPtr createReader() = 0;

    virtual ~IRegionsHierarchyDataSource() = default;
};

using IRegionsHierarchyDataSourcePtr = std::shared_ptr<IRegionsHierarchyDataSource>;


// Provides data sources for different regions hierarchies
class IRegionsHierarchiesDataProvider
{
public:
    virtual std::vector<std::string> listCustomHierarchies() const = 0;

    virtual IRegionsHierarchyDataSourcePtr getDefaultHierarchySource() const = 0;
    virtual IRegionsHierarchyDataSourcePtr getHierarchySource(const std::string & name) const = 0;

    virtual ~IRegionsHierarchiesDataProvider() = default;
};

using IRegionsHierarchiesDataProviderPtr = std::shared_ptr<IRegionsHierarchiesDataProvider>;

}
