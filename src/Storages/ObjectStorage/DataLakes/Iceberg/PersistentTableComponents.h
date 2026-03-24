#pragma once
#include "config.h"

#if USE_AVRO

#include <atomic>
#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

namespace DB::Iceberg
{

// All fields in this struct should be either thread-safe or immutable, because it can be used by several queries
struct PersistentTableComponents
{
    IcebergSchemaProcessorPtr schema_processor;
    IcebergMetadataFilesCachePtr metadata_cache;
    /// format_version is atomic because external tools (e.g. Spark) can upgrade the
    /// Iceberg format version between queries, and we update it when reading new metadata.
    /// It can be read concurrently from multiple queries.
    mutable std::atomic<Int32> format_version;
    const String table_location;
    const CompressionMethod metadata_compression_method;
    const String table_path;
    const std::optional<String> table_uuid;

    PersistentTableComponents(
        IcebergSchemaProcessorPtr schema_processor_,
        IcebergMetadataFilesCachePtr metadata_cache_,
        Int32 format_version_,
        String table_location_,
        CompressionMethod metadata_compression_method_,
        String table_path_,
        std::optional<String> table_uuid_)
        : schema_processor(std::move(schema_processor_))
        , metadata_cache(std::move(metadata_cache_))
        , format_version(format_version_)
        , table_location(std::move(table_location_))
        , metadata_compression_method(metadata_compression_method_)
        , table_path(std::move(table_path_))
        , table_uuid(std::move(table_uuid_))
    {
    }

    PersistentTableComponents(const PersistentTableComponents & other)
        : schema_processor(other.schema_processor)
        , metadata_cache(other.metadata_cache)
        , format_version(other.format_version.load(std::memory_order_relaxed))
        , table_location(other.table_location)
        , metadata_compression_method(other.metadata_compression_method)
        , table_path(other.table_path)
        , table_uuid(other.table_uuid)
    {
    }

    PersistentTableComponents(PersistentTableComponents && other) noexcept
        : schema_processor(std::move(other.schema_processor))
        , metadata_cache(std::move(other.metadata_cache))
        , format_version(other.format_version.load(std::memory_order_relaxed))
        , table_location(other.table_location)
        , metadata_compression_method(other.metadata_compression_method)
        , table_path(other.table_path)
        , table_uuid(other.table_uuid)
    {
    }

    PersistentTableComponents & operator=(const PersistentTableComponents &) = delete;
    PersistentTableComponents & operator=(PersistentTableComponents &&) = delete;
};

}

#endif
