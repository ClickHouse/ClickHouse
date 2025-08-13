
#include "config.h"
#if USE_AVRO

#    include <cstddef>
#    include <Common/Exception.h>


#    include <Core/NamesAndTypes.h>
#    include <Core/Settings.h>
#    include <Databases/DataLake/Common.h>
#    include <Databases/DataLake/ICatalog.h>
#    include <Disks/ObjectStorages/StoredObject.h>
#    include <Formats/FormatFactory.h>
#    include <IO/ReadBufferFromFileBase.h>
#    include <IO/ReadBufferFromString.h>
#    include <IO/ReadHelpers.h>
#    include <Interpreters/Context.h>

#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>

#    include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>


namespace DB::Iceberg
{

Iceberg::ManifestFilePtr getManifestFile(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    IcebergMetadataFilesCachePtr iceberg_metadata_cache,
    IcebergSchemaProcessorPtr schema_processor,
    Int32 format_version,
    String table_location,
    ContextPtr local_context,
    LoggerPtr log,
    const String & filename,
    Int64 inherited_sequence_number,
    Int64 inherited_snapshot_id);


ManifestFileCacheKeys getManifestList(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationWeakPtr configuration,
    Int32 format_version,
    String table_location,
    IcebergMetadataFilesCachePtr iceberg_metadata_cache,
    ContextPtr local_context,
    const String & filename,
    LoggerPtr log);

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV1Method(const Poco::JSON::Object::Ptr & metadata_object);
std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV2Method(const Poco::JSON::Object::Ptr & metadata_object);
}

#endif
