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
    explicit IcebergDataObjectInfo(Iceberg::ManifestFileEntry data_manifest_file_entry_, const std::vector<Iceberg::ManifestFileEntry> & position_deletes_, const String& format);
    explicit IcebergDataObjectInfo(
        String data_object_file_path_,
        String data_object_file_path_key_,
        Int32 read_schema_id_,
        std::pair<size_t, size_t> position_deletes_objects_range_);


    String data_object_file_path_key; // Full path to the data object file
    Int32 read_schema_id;
    std::pair<size_t, size_t> position_deletes_objects_range;
};

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

}


#endif
