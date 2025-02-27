#pragma once
#include "config.h"

#if USE_AVRO

#    include <Core/Types.h>
#    include <Disks/ObjectStorages/IObjectStorage.h>
#    include <Interpreters/Context_fwd.h>
#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/StorageObjectStorage.h>

#    include <Poco/JSON/Array.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Parser.h>

#    include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h"

#    include <unordered_map>

namespace DB
{

struct IcebergTableState
{
    Int32 current_metadata_version;
    Int32 format_version;
    Int32 current_schema_id;
    std::optional<Iceberg::IcebergSnapshot> current_snapshot;
};


class MetadataResolver
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;

    Iceberg::IcebergSnapshot getRelevantIcebergSnapshot() const
    {
        if (!current_snapshot.has_value())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No snapshot found");
        }
        return current_snapshot.value();
    }

private:
    const ConfigurationObserverPtr configuration;
    const ObjectStoragePtr object_storage;
    const std::string path;
};


}

#endif
