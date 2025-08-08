
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


namespace DB
{

Iceberg::ManifestFilePtr getManifestFile(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    IcebergMetadataFilesCachePtr iceberg_metadata_cache,
    ContextPtr local_context,
    const String & filename,
    Int64 inherited_sequence_number,
    Int64 inherited_snapshot_id);


ManifestFileCacheKeys getManifestList(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    IcebergMetadataFilesCachePtr iceberg_metadata_cache,
    ContextPtr local_context,
    const String & filename);

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV2Method(const Poco::JSON::Object::Ptr & metadata_object)
{
    Poco::JSON::Object::Ptr schema;
    if (!metadata_object->has(f_current_schema_id))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is missing in metadata", f_current_schema_id);
    auto current_schema_id = metadata_object->getValue<int>(f_current_schema_id);
    if (!metadata_object->has(f_schemas))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is missing in metadata", f_schemas);
    auto schemas = metadata_object->get(f_schemas).extract<Poco::JSON::Array::Ptr>();
    if (schemas->size() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is empty", f_schemas);
    for (uint32_t i = 0; i != schemas->size(); ++i)
    {
        auto current_schema = schemas->getObject(i);
        if (!current_schema->has(f_schema_id))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is missing in schema", f_schema_id);
        }
        if (current_schema->getValue<int>(f_schema_id) == current_schema_id)
        {
            schema = current_schema;
        }
    }

    if (!schema)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, R"(There is no schema with "{}" that matches "{}" in metadata)", f_schema_id, f_current_schema_id);
    if (schema->getValue<int>(f_schema_id) != current_schema_id)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, R"(Field "{}" of the schema doesn't match "{}" in metadata)", f_schema_id, f_current_schema_id);
    return {schema, current_schema_id};
}

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV1Method(const Poco::JSON::Object::Ptr & metadata_object)
{
    if (!metadata_object->has(f_schema))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is missing in metadata", f_schema);
    Poco::JSON::Object::Ptr schema = metadata_object->getObject(f_schema);
    if (!metadata_object->has(f_schema_id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is missing in schema", f_schema_id);
    auto current_schema_id = schema->getValue<int>(f_schema_id);
    return {schema, current_schema_id};
}


}

#endif
