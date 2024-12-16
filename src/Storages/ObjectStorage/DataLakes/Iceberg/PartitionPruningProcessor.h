#pragma once

#include "Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h"
#include "config.h"

#if USE_AVRO


// #    include <Core/Types.h>
// #    include <Disks/ObjectStorages/IObjectStorage.h>
// #    include <Interpreters/Context_fwd.h>
// #    include "Columns/ColumnsDateTime.h"
// #    include "DataTypes/DataTypeNullable.h"

// #    include <Poco/JSON/Array.h>
// #    include <Poco/JSON/Object.h>
// #    include <Poco/JSON/Parser.h>

#    include <Interpreters/ActionsDAG.h>
#    include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h"


#    include <unordered_map>

namespace DB
{

using Iceberg::ManifestFileEntry;
using Iceberg::SpecificSchemaPartitionInfo;

class PartitionPruningProcessor
{
public:
    SpecificSchemaPartitionInfo
    getSpecificPartitionInfo(const ManifestFileEntry & manifest_file, Int32 schema_version, const IcebergSchemaProcessor & processor) const;

    std::vector<bool> getPruningMask(
        const String & manifest_file,
        const SpecificSchemaPartitionInfo & specific_info,
        const ActionsDAG * filter_dag,
        ContextPtr context) const;

    Strings getDataFiles(
        const std::vector<ManifestFileEntry> & manifest_partitions_infos,
        const std::vector<SpecificSchemaPartitionInfo> & specific_infos,
        const ActionsDAG * filter_dag,
        ContextPtr context,
        const std::string & common_path) const;

private:
};


} // namespace DB

#endif
