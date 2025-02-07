#pragma once
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <boost/noncopyable.hpp>
#include "Interpreters/ActionsDAG.h"
#include "PartitionColumns.h"

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}

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
    virtual Strings makePartitionPruning(const ActionsDAG &)
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Partition pruning is not supported by the metadata type");
    }
    virtual std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String &) const { return {}; }
    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String &) const { return {}; }
    virtual bool supportsExternalMetadataChange() const { return false; }
    virtual bool supportsUpdate() const { return false; }
    virtual bool update(const ContextPtr &) { return false; }
};
using DataLakeMetadataPtr = std::unique_ptr<IDataLakeMetadata>;

}
