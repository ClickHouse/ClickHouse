#pragma once
#include <config.h>

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonSchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonStreamState.h>

namespace DB
{


/// Persistent components for Paimon table.
/// All fields must be either thread-safe or immutable.
/// Can be safely shared by multiple queries.
/// Similar to Iceberg's PersistentTableComponents.
struct PaimonPersistentComponents
{
    PaimonSchemaProcessorPtr schema_processor;

    /// Controlled by use_paimon_metadata_files_cache setting.
    /// Shared with the global Context; may be nullptr if the cache is disabled
    /// (either at session level or at server-startup level).
    PaimonMetadataFilesCachePtr metadata_cache;

    /// Stream state for incremental read (optional, can be nullptr)
    /// Controlled by paimon_incremental_read setting
    /// Stores committed snapshot ID in Keeper for at-most-once delivery
    PaimonStreamStatePtr stream_state;

    /// Immutable table location (e.g., s3://bucket/path/to/table)
    const String table_location;

    /// Immutable table path (relative path within storage)
    const String table_path;

    /// Immutable cache-key prefix that uniquely identifies this Paimon table instance.
    /// Used together with the manifest/manifest-list path to form the cache key,
    /// so that two tables that ever shared the same path (e.g. after DROP + re-CREATE)
    /// or live on different storage backends never collide in cache.
    const String table_cache_key_prefix;

    /// Optional: partition default value from table options
    const String partition_default_name;

    /// Whether incremental read is enabled
    const bool incremental_read_enabled;

    /// Background metadata refresh interval (seconds). 0 means disabled.
    const Int64 metadata_refresh_interval_sec;

    /// Creation timestamp of schema-0, used to detect external table recreation.
    /// If the underlying Paimon table is dropped and recreated at the same path,
    /// schema-0 will have a different timeMillis.
    const Int64 schema0_time_millis;

    PaimonPersistentComponents(
        PaimonSchemaProcessorPtr schema_processor_,
        PaimonMetadataFilesCachePtr metadata_cache_,  /// Can be nullptr if cache is disabled
        PaimonStreamStatePtr stream_state_,      /// Can be nullptr if incremental read is disabled
        String table_location_,
        String table_path_,
        String table_cache_key_prefix_,
        String partition_default_name_ = "__DEFAULT_PARTITION__",
        bool incremental_read_enabled_ = false,
        Int64 metadata_refresh_interval_sec_ = 0,
        Int64 schema0_time_millis_ = 0)
        : schema_processor(std::move(schema_processor_))
        , metadata_cache(std::move(metadata_cache_))
        , stream_state(std::move(stream_state_))
        , table_location(std::move(table_location_))
        , table_path(std::move(table_path_))
        , table_cache_key_prefix(std::move(table_cache_key_prefix_))
        , partition_default_name(std::move(partition_default_name_))
        , incremental_read_enabled(incremental_read_enabled_)
        , metadata_refresh_interval_sec(metadata_refresh_interval_sec_)
        , schema0_time_millis(schema0_time_millis_)
    {
    }

    /// Whether the metadata cache is actually usable right now.
    /// The server-level capacity (paimon_metadata_files_cache_size) is a runtime setting that can
    /// be changed via SYSTEM RELOAD CONFIG, so it is evaluated dynamically rather than latched at
    /// create time.  Read paths must use this predicate so that a zero-capacity cache neither
    /// disables the filesystem cache nor records cache misses for entries that can never be kept.
    bool isMetadataCacheActive() const { return metadata_cache != nullptr && metadata_cache->maxSizeInBytes() > 0; }

    bool hasStreamState() const { return stream_state != nullptr && incremental_read_enabled; }
};

}


#endif
