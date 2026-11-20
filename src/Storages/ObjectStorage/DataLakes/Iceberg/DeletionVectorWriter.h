#pragma once

#include "config.h"

#if USE_AVRO

#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>

#include <roaring/roaring64map.hh>

#include <map>
#include <unordered_set>

namespace DB
{
class IObjectStorage;
using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;
}

namespace DB::Iceberg
{

struct WrittenDeletionVector
{
    DeletionVectorLocation location;
    UInt64 cardinality = 0;
};

struct WrittenDeletionVectorsFile
{
    IcebergPathFromMetadata path;
    Int64 size_in_bytes = 0;
    std::map<String, WrittenDeletionVector> vectors;
};

WrittenDeletionVectorsFile writeDeletionVectorsFile(
    std::map<String, roaring::Roaring64Map> & positions_by_data_file,
    FileNamesGenerator & filename_generator,
    const IcebergPathResolver & path_resolver,
    ObjectStoragePtr object_storage,
    ContextPtr context);

struct RewrittenDeleteManifest
{
    DeleteManifestListEntry manifest_list_entry;
    Int64 removed_files = 0;
    Int64 removed_rows = 0;
};

RewrittenDeleteManifest rewriteDeleteManifest(
    const ManifestFileCacheKey & source,
    const std::unordered_set<String> & data_files_with_replaced_vectors,
    Int64 new_snapshot_id,
    FileNamesGenerator & filename_generator,
    const IcebergPathResolver & path_resolver,
    ObjectStoragePtr object_storage,
    ContextPtr context);

}

#endif
