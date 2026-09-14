#pragma once
#include "config.h"

#if USE_AVRO

#include <Poco/JSON/Object.h>

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
    /// True when the resolver works against a table root deeper than `table_path`. Operations scoped
    /// to `table_path` then reach outside this table, so they must refuse to run.
    const bool table_root_was_derived;

    /// The `table-uuid` currently trusted as a metadata content cache key. Always read it
    /// through this accessor - the value can be refreshed by `IcebergMetadata::update`.
    std::optional<String> getTableUuid() const { return trusted_table_uuid->get(); }

    /// The schema processor of the current incarnation of the table. Hold on to the returned
    /// pointer for as long as the schemas are used: `IcebergMetadata::update` installs a fresh
    /// processor when the table is replaced, and the previous one stays alive for its holders.
    IcebergSchemaProcessorPtr getSchemaProcessor() const { return shared_schema_processor->get(); }

    /// The current schema processor and the incarnation it describes, observed as one.
    std::pair<IcebergSchemaProcessorPtr, UInt64> getSchemaProcessorWithIncarnation() const
    {
        return shared_schema_processor->getWithIncarnation();
    }

    /// The schema processor of the incarnation a query was validated against, which is the one
    /// every execution-time schema lookup of that query has to go through. Throws when the table
    /// was replaced in the meantime. See `SharedSchemaProcessor::getForPinnedIncarnation`.
    IcebergSchemaProcessorPtr getSchemaProcessorForPinnedIncarnation(std::optional<UInt64> pinned_incarnation) const
    {
        return shared_schema_processor->getForPinnedIncarnation(pinned_incarnation);
    }

    /// Refuse to continue when the table was dropped and recreated at the same root since the
    /// statement was validated. A statement plans against one incarnation - its sample block, its
    /// column ids, its reachability set all describe that table - so continuing against the
    /// replacement would rewrite or delete another table's files. `operation` names the statement
    /// in the message. A statement carrying no validated incarnation, such as one deserialized on
    /// another server, has nothing to compare against and is let through.
    void checkTableWasNotReplaced(std::optional<UInt64> validated_incarnation, std::string_view operation) const;

    /// Refuse to continue when the metadata file that was just read does not belong to the table
    /// the statement was validated against.
    ///
    /// The incarnation counter only moves when a replacement is observed through
    /// `IcebergMetadata::update`, so it cannot be the only authority here: a metadata read that
    /// misses the cache goes straight to object storage, and it can select and parse the
    /// replacement's file before any `update` has run. The file's own `table-uuid` is what settles
    /// it, and it is compared against the UUID of the validated incarnation. A file with no
    /// `table-uuid`, or a statement with no validated incarnation, has nothing to compare.
    void checkMetadataBelongsToValidatedTable(
        const Poco::JSON::Object::Ptr & metadata_object,
        std::optional<UInt64> validated_incarnation,
        std::string_view operation) const;

    /// Refuse to continue when the metadata file a statement pinned no longer carries the content
    /// it carried when the statement was analysed.
    ///
    /// An Iceberg metadata file is immutable: a commit writes a new one, it never rewrites an
    /// existing one. So the same path answering with different content is not a table that moved
    /// on, it is a different table that took the path over. This is the only replacement token a
    /// format-version 1 table that omits `table-uuid` has, and unlike the UUID check it needs no
    /// listing, so the pinned reopens on the read path can afford it.
    ///
    /// A statement carrying no pinned token, such as one deserialized on another server, has
    /// nothing to compare.
    void checkMetadataMatchesPinnedState(
        const Poco::JSON::Object::Ptr & metadata_object, std::optional<UInt64> pinned_token, std::string_view operation) const;

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

/// A fingerprint of the identifying, top-level content of a metadata file, taken when a statement
/// pins the file and compared when the statement reopens it. See `checkMetadataMatchesPinnedState`.
UInt64 computeMetadataContentToken(const Poco::JSON::Object::Ptr & metadata_object);


}

#endif
