#pragma once
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include "config.h"

#if USE_AVRO

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Types.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>

#include <Common/SharedMutex.h>
#include <tuple>
#include <optional>
#include <base/defines.h>

#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#    include <Storages/ObjectStorage/StorageObjectStorage.h>


namespace DB {


struct IcebergDataObjectInfo : public RelativePathWithMetadata
{
    explicit IcebergDataObjectInfo(Iceberg::ManifestFileEntry data_manifest_file_entry_, const std::vector<Iceberg::ManifestFileEntry> & position_deletes_);

    String data_object_file_path_key;
    String data_object_file_path; // Full path to the data object file
    Int32 read_schema_id;
    std::span<const Iceberg::ManifestFileEntry> position_deletes_objects;

    bool operator<(const IcebergDataObjectInfo & other) const
    {
        return std::tie(data_object_file_path_key) < std::tie(other.data_object_file_path_key);
    }
};

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

}


#endif
