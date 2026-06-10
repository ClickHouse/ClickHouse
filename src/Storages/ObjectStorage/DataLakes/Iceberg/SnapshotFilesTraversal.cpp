#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotFilesTraversal.h>

#include <functional>

#include <Poco/JSON/Object.h>

#include <Common/logger_useful.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/Utils.h>

namespace DB::Iceberg
{

SnapshotReferencedFiles collectSnapshotReferencedFiles(
    const Poco::JSON::Array::Ptr & snapshots,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id,
    SecondaryStorages & secondary_storages)
{
    SnapshotReferencedFiles files;

    for (UInt32 i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(i);
        if (!snapshot->has(Iceberg::f_manifest_list))
            continue;

        auto manifest_list_path = IcebergPathFromMetadata::deserialize(snapshot->getValue<String>(Iceberg::f_manifest_list));
        files.manifest_list_paths.insert(manifest_list_path);

        auto manifest_keys = getManifestList(
            object_storage, persistent_table_components, context, manifest_list_path, log, secondary_storages);

        for (const auto & manifest_entry : manifest_keys)
        {
            files.manifest_paths.insert(manifest_entry.manifest_file_path);

            auto entries_handle = getManifestFileEntriesHandle(
                object_storage, persistent_table_components, context, log, manifest_entry, current_schema_id, secondary_storages);

            for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::DATA))
                files.data_file_paths.insert(entry->parsed_entry->file_path_key);
            for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
                files.data_file_paths.insert(entry->parsed_entry->file_path_key);
            for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::EQUALITY_DELETE))
                files.data_file_paths.insert(entry->parsed_entry->file_path_key);
        }
    }

    return files;
}

namespace
{

using InsertReachableFn = std::function<void(const IcebergPathFromMetadata &)>;

void collectStatisticsPaths(
    const Poco::JSON::Object::Ptr & metadata,
    const char * field_name,
    const InsertReachableFn & insert_reachable)
{
    if (!metadata->has(field_name))
        return;
    auto arr = metadata->get(field_name).extract<Poco::JSON::Array::Ptr>();
    if (!arr)
        return;
    for (UInt32 j = 0; j < arr->size(); ++j)
    {
        auto entry = arr->getObject(j);
        if (entry->has(f_statistics_path))
        {
            String stat_path = entry->getValue<String>(f_statistics_path);
            insert_reachable(IcebergPathFromMetadata::deserialize(stat_path));
        }
    }
}

/// Collect files reachable directly from the metadata JSON root:
/// the current metadata file, historical metadata files from metadata-log,
/// statistics, partition-statistics, and version-hint files.
void collectMetadataRootFiles(
    const String & metadata_path,
    const Poco::JSON::Object::Ptr & metadata,
    const IcebergPathResolver & resolver,
    const InsertReachableFn & insert_reachable,
    std::unordered_set<String> & out)
{
    out.insert(metadata_path);

    /// `getTableLocation` has no trailing '/' (unlike `FileNamesGenerator`, which appends one).
    auto version_hint = IcebergPathFromMetadata::deserialize(fmt::format("{}/metadata/version-hint.text", resolver.getTableLocation()));
    insert_reachable(version_hint);

    if (metadata->has(f_metadata_log))
    {
        auto metadata_log = metadata->get(f_metadata_log).extract<Poco::JSON::Array::Ptr>();
        if (metadata_log)
        {
            for (UInt32 i = 0; i < metadata_log->size(); ++i)
            {
                auto entry = metadata_log->getObject(i);
                if (entry->has(f_metadata_file))
                {
                    String mf_path = entry->getValue<String>(f_metadata_file);
                    insert_reachable(IcebergPathFromMetadata::deserialize(mf_path));
                }
            }
        }
    }

    collectStatisticsPaths(metadata, f_statistics, insert_reachable);
    collectStatisticsPaths(metadata, f_partition_statistics, insert_reachable);
}

}


ReachableFilesResult collectReachableFiles(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    const DataLakeStorageSettings & data_lake_settings,
    ContextPtr context,
    LoggerPtr log,
    SecondaryStorages & secondary_storages)
{
    auto [version, metadata_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
        object_storage,
        persistent_table_components.table_path,
        data_lake_settings,
        persistent_table_components.metadata_cache,
        context,
        log.get(),
        persistent_table_components.table_uuid,
        persistent_table_components.metadata_compression_method,
        /* force_fetch_latest_metadata */ true,
        /* ignore_explicit_metadata_file_path */ true);

    auto metadata = getMetadataJSONObject(
        metadata_path,
        object_storage,
        persistent_table_components.metadata_cache,
        context,
        log,
        compression_method,
        persistent_table_components.table_uuid);

    std::unordered_set<String> reachable;
    const auto & resolver = persistent_table_components.path_resolver;

    /// `reachable` is matched against the base `object_storage` listing, so it must contain only
    /// base-storage keys. Resolve each reference to its (storage, key) identity and skip files that
    /// live on a secondary storage: collapsing an external URI (e.g. `s3a://other_bucket/...`) into
    /// a base key would wrongly mark an unrelated base-storage object with the same key as reachable.
    auto insert_if_base_storage = [&](const IcebergPathFromMetadata & path)
    {
        auto [storage, key] = resolveObjectStorageForPath(
            persistent_table_components.table_location, path.serialize(), object_storage, secondary_storages, context, resolver);
        if (storage.get() == object_storage.get())
            reachable.insert(std::move(key));
    };

    collectMetadataRootFiles(
        metadata_path, metadata,
        resolver,
        insert_if_base_storage,
        reachable);

    if (!metadata->has(f_snapshots))
    {
        LOG_INFO(log, "No snapshots in metadata, reachable set contains only metadata-root files");
        return {std::move(reachable), version};
    }

    auto snapshots = metadata->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    if (!snapshots || snapshots->size() == 0)
    {
        LOG_INFO(log, "Empty snapshots array, reachable set contains only metadata-root files");
        return {std::move(reachable), version};
    }

    Int32 current_schema_id = metadata->getValue<Int32>(f_current_schema_id);

    auto snapshot_files = collectSnapshotReferencedFiles(
        snapshots, object_storage, persistent_table_components, context, log, current_schema_id, secondary_storages);

    for (const auto & path : snapshot_files.manifest_list_paths)
        insert_if_base_storage(path);
    for (const auto & path : snapshot_files.manifest_paths)
        insert_if_base_storage(path);
    for (const auto & path : snapshot_files.data_file_paths)
        insert_if_base_storage(path);

    LOG_INFO(log, "Collected {} reachable files from metadata graph", reachable.size());
    return {std::move(reachable), version};
}

}

#endif
