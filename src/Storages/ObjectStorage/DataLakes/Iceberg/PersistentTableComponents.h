#pragma once
#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>

namespace DB::Iceberg
{

// All fields in this struct should be either thread-safe or immutable, because it can be used by several queries
struct PersistentTableComponents
{
    IcebergSchemaProcessorPtr schema_processor;
    IcebergMetadataFilesCachePtr metadata_cache;
    const Int32 format_version;
    const String table_location;
};

}

#endif
