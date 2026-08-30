#pragma once
#include "config.h"

#if USE_AVRO

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

namespace DB::Iceberg
{

/// Identity of the physical storage a table lives on. It serves two purposes:
///  - `data_source_description` (e.g. `IObjectStorageConfiguration::getDataSourceDescription`) is
///    mixed into every metadata cache key, so two different backends (different `S3` endpoints,
///    different `Azure` storage accounts) that happen to share a bucket/container name and table
///    path can never collide on the same cache entry, even under a stale `catalog_uuid_hint`;
///  - `table_namespace` and `backend_type` let a cache hit obtained under a caller-supplied,
///    not-yet-validated UUID be checked against the `location` recorded in the cached metadata,
///    see `cachedLocationMatchesTableRoot`.
struct TableStorageIdentity
{
    String data_source_description;
    /// From `deriveTableNamespaceForLocationCheck(getNamespace(), getRawURI())`.
    String table_namespace;
    /// From `IObjectStorageConfiguration::getTypeName()`.
    String backend_type;
};

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
    /// Canonical identity of the physical storage this table lives on, see `TableStorageIdentity`.
    const TableStorageIdentity table_identity;

    /// Invalidate the cached latest-metadata selection for this table, keyed by `table_path`
    /// namespaced by `table_identity.data_source_description` via
    /// `IcebergMetadataFilesCache::getLatestVersionKey`.
    void invalidateMetadataCache() const
    {
        if (!metadata_cache)
            return;
        metadata_cache->remove(
            IcebergMetadataFilesCache::getLatestVersionKey(table_identity.data_source_description, table_path));
    }
};

}

#endif
