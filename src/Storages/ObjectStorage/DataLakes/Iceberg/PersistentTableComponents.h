#pragma once
#include "config.h"

#if USE_AVRO

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

namespace DB::Iceberg
{

// All fields in this struct should be either thread-safe or immutable, because it can be used by several queries
struct PersistentTableComponents
{
    IcebergSchemaProcessorPtr schema_processor;
    IcebergMetadataFilesCachePtr metadata_cache;
    const Int32 format_version;
    const String table_location;
    const CompressionMethod metadata_compression_method;
    const String table_path;
    const std::optional<String> table_uuid;
    const IcebergPathResolver path_resolver;

    /// Invalidate cached metadata for this table under both keys we may have used to cache it
    /// (`table_path` and `table_uuid`).
    void invalidateMetadataCache() const
    {
        if (!metadata_cache)
            return;
        metadata_cache->remove(table_path);
        if (table_uuid.has_value())
            metadata_cache->remove(*table_uuid);
    }
};

}

#endif
