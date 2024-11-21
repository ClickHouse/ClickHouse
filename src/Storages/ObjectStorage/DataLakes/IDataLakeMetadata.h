#pragma once
#include <boost/noncopyable.hpp>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include "Interpreters/ActionsDAG.h"
#include "PartitionColumns.h"

namespace DB
{

class IDataLakeMetadata : boost::noncopyable
{
public:
    virtual ~IDataLakeMetadata() = default;
    virtual Strings getDataFiles() const = 0;
    virtual NamesAndTypesList getTableSchema() const = 0;
    virtual bool operator==(const IDataLakeMetadata & other) const = 0;
    virtual const DataLakePartitionColumns & getPartitionColumns() const = 0;
    virtual const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const = 0;
    virtual bool supportsPartitionPruning() { return false; }
    virtual Strings makePartitionPruning(const ActionsDAG &) { return {}; }
};
using DataLakeMetadataPtr = std::unique_ptr<IDataLakeMetadata>;

}
