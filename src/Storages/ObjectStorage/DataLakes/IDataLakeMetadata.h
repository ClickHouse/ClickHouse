#pragma once
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <boost/noncopyable.hpp>
#include "PartitionColumns.h"

#include "Storages/ObjectStorage/DataFileInfo.h"

namespace DB
{
class IDataLakeMetadata : boost::noncopyable
{
public:
    virtual ~IDataLakeMetadata() = default;
    virtual std::vector<DataFileInfo> getDataFileInfos() const = 0;
    virtual NamesAndTypesList getTableSchema() const = 0;
    virtual bool operator==(const IDataLakeMetadata & other) const = 0;
    virtual const DataLakePartitionColumns & getPartitionColumns() const = 0;
    virtual const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const = 0;
};
using DataLakeMetadataPtr = std::unique_ptr<IDataLakeMetadata>;

}
