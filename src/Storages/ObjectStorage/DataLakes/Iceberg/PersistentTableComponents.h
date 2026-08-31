#pragma once
#include "config.h"

#if USE_AVRO

#include <string_view>

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

    /// True when the resolver works against a table root deeper than `table_path`. Operations scoped
    /// to `table_path` then reach outside this table, so they must refuse to run.
    ///
    /// Derived from the resolver instead of being stored alongside it: a stored flag lets a
    /// construction site hand the resolver a deeper root and still report `false`, and the
    /// permissive value is the one aggregate initialization picks for an omitted member.
    /// `IcebergPathResolver::deriveTableRoot` returns the queried path unchanged unless it adopted a
    /// descendant of it, so the roots differ exactly when the derivation fired.
    bool tableRootWasDerived() const
    {
        std::string_view queried = table_path;
        /// The resolver trims trailing slashes off the root it was given; `table_path` keeps them.
        while (!queried.empty() && queried.back() == '/')
            queried.remove_suffix(1);
        return std::string_view(path_resolver.getTableRoot()) != queried;
    }

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
