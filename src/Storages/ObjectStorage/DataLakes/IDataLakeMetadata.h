#pragma once
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <boost/noncopyable.hpp>
#include "Interpreters/ActionsDAG.h"
#include "PartitionColumns.h"
#include "Storages/ObjectStorage/StorageObjectStorage.h"

namespace DB
{

struct DataFileMeta : StorageObjectStorage::Configuration::PathMetadata
{
    enum class DataFileType : uint8_t
    {
        DATA_FILE,

        // Note: this types useful only for iceberg
        ICEBERG_POSITIONAL_DELETE,
    };
    DataFileType type;
    int64_t sequence_number;
};

using DataFileInfo = StorageObjectStorage::Configuration::Path;
using DataFileInfos = std::vector<DataFileInfo>;

class IDataLakeMetadata : boost::noncopyable
{
public:
    virtual ~IDataLakeMetadata() = default;
    virtual DataFileInfos getDataFiles() const = 0;
    virtual NamesAndTypesList getTableSchema() const = 0;
    virtual bool operator==(const IDataLakeMetadata & other) const = 0;
    virtual const DataLakePartitionColumns & getPartitionColumns() const = 0;
    virtual const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const = 0;
    virtual std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String &) const { return {}; }
    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String &) const { return {}; }
    virtual bool supportsExternalMetadataChange() const { return false; }
    virtual bool supportsUpdate() const { return false; }
    virtual bool update(const ContextPtr &) { return false; }
};
using DataLakeMetadataPtr = std::unique_ptr<IDataLakeMetadata>;

}
