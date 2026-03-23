#pragma once
#include "config.h"

#if USE_AVRO

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
    /// format_version is mutable because external tools (e.g. Spark) can upgrade the
    /// Iceberg format version between queries, and we update it when reading new metadata.
    /// Concurrent writes are safe because they are idempotent (always set to the value from the latest metadata file).
    mutable Int32 format_version;
    const String table_location;
    const CompressionMethod metadata_compression_method;
    const String table_path;
    const std::optional<String> table_uuid;
};

}

#endif
