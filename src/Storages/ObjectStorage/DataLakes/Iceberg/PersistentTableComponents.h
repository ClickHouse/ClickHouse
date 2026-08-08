#pragma once
#include "config.h"

#if USE_AVRO

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

namespace DB::Iceberg
{

// All fields in this struct should be either thread-safe or immutable, because it can be used by several queries.
// `format_version` is the value observed when the table was opened; the authoritative version
// for parsing a specific manifest list / manifest file is taken from that file's own Avro metadata,
// so an external upgrade (e.g. Spark v1 -> v2) between queries does not corrupt this cache.
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
    /// Canonical identity of the physical backend (e.g. `IObjectStorageConfiguration::getDataSourceDescription`),
    /// mixed into the metadata content cache key so that two different backends (different `S3` endpoints,
    /// different `Azure` storage accounts) that happen to share a bucket/container name and table path can
    /// never collide on the same cache entry, even under a stale `catalog_uuid_hint`.
    const String data_source_description;

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
