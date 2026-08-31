#pragma once
#include "config.h"

#if USE_AVRO

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/TrustedTableUuid.h>

namespace DB::Iceberg
{

// All fields in this struct should be either thread-safe or immutable, because it can be used by several queries.
// `format_version` is the value observed when the table was opened; the authoritative version
// for parsing a specific manifest list / manifest file is taken from that file's own Avro metadata,
// so an external upgrade (e.g. Spark v1 -> v2) between queries does not corrupt this cache.
struct PersistentTableComponents
{
    /// Shared, not copied, between all copies of this struct describing the same table, so that
    /// a replacement observed by one query installs a fresh processor for the ones that follow.
    const SharedSchemaProcessorPtr shared_schema_processor;
    IcebergMetadataFilesCachePtr metadata_cache;
    const Int32 format_version;
    const String table_location;
    const CompressionMethod metadata_compression_method;
    const String table_path;
    /// Shared, not copied, between all copies of this struct describing the same table, because
    /// `IcebergMetadata::update` refreshes it when the table may have been replaced in place.
    const TrustedTableUuidPtr trusted_table_uuid;
    const IcebergPathResolver path_resolver;

    /// The `table-uuid` currently trusted as a metadata content cache key. Always read it
    /// through this accessor - the value can be refreshed by `IcebergMetadata::update`.
    std::optional<String> getTableUuid() const { return trusted_table_uuid->get(); }

    /// The schema processor of the current incarnation of the table. Hold on to the returned
    /// pointer for as long as the schemas are used: `IcebergMetadata::update` installs a fresh
    /// processor when the table is replaced, and the previous one stays alive for its holders.
    IcebergSchemaProcessorPtr getSchemaProcessor() const { return shared_schema_processor->get(); }

    /// Invalidate cached metadata for this table under both keys we may have used to cache it
    /// (`table_path` and `table_uuid`).
    void invalidateMetadataCache() const
    {
        if (!metadata_cache)
            return;
        metadata_cache->remove(table_path);
        if (auto table_uuid = getTableUuid())
            metadata_cache->remove(*table_uuid);
    }
};

}

#endif
