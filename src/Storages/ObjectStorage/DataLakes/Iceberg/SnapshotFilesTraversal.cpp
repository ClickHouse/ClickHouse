#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotFilesTraversal.h>

#include <functional>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include <Poco/JSON/Object.h>

#include <filesystem>

#include <Common/logger_useful.h>

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExternalPathResolver.h>

namespace DB::Iceberg
{

SnapshotReferencedFiles collectSnapshotReferencedFiles(
    const Poco::JSON::Array::Ptr & snapshots,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id,
    ExternalStorageCache & external_storages)
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
            object_storage, persistent_table_components, context, manifest_list_path, log, external_storages);

        for (const auto & manifest_entry : manifest_keys)
        {
            files.manifest_paths.insert(manifest_entry.manifest_file_path);

            auto entries_handle = getManifestFileEntriesHandle(
                object_storage, persistent_table_components, context, log, manifest_entry, current_schema_id, external_storages);

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

using VisitPathFn = std::function<void(const IcebergPathFromMetadata &)>;

void collectStatisticsPaths(
    const Poco::JSON::Object::Ptr & metadata,
    const char * field_name,
    const VisitPathFn & visit)
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
            visit(IcebergPathFromMetadata::deserialize(stat_path));
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
    const VisitPathFn & visit,
    std::unordered_set<String> & out)
{
    /// `metadata_path` is already a storage key; passing it through URI resolution would misparse it.
    out.insert(metadata_path);

    /// version-hint.text is not a metadata path: it is a fixed object under the storage root.
    out.insert(std::filesystem::path(resolver.getTableRoot()) / "metadata" / "version-hint.text");

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
                    visit(IcebergPathFromMetadata::deserialize(mf_path));
                }
            }
        }
    }

    collectStatisticsPaths(metadata, f_statistics, visit);
    collectStatisticsPaths(metadata, f_partition_statistics, visit);
}

/// Ignore deleted historical metadata: `remove_orphan_files` may have removed it on a previous run.
/// Other errors propagate because they leave the referenced paths unknown.
void collectHistoricalReferences(
    const Poco::JSON::Object::Ptr & current_metadata,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    ExternalStorageCache & external_storages,
    Int32 current_schema_id,
    const VisitPathFn & visit_history)
{
    const auto & resolver = persistent_table_components.path_resolver;

    std::vector<IcebergPathFromMetadata> metadata_worklist;
    std::unordered_set<String> seen_metadata_paths;

    auto enqueue_log_entries = [&](const Poco::JSON::Object::Ptr & metadata)
    {
        if (!metadata->has(f_metadata_log))
            return;
        auto metadata_log = metadata->get(f_metadata_log).extract<Poco::JSON::Array::Ptr>();
        if (!metadata_log)
            return;
        for (UInt32 i = 0; i < metadata_log->size(); ++i)
        {
            auto entry = metadata_log->getObject(i);
            if (!entry->has(f_metadata_file))
                continue;
            String mf_path = entry->getValue<String>(f_metadata_file);
            if (seen_metadata_paths.emplace(mf_path).second)
                metadata_worklist.push_back(IcebergPathFromMetadata::deserialize(mf_path));
        }
    };

    enqueue_log_entries(current_metadata);

    std::unordered_set<IcebergPathFromMetadata> traversed_manifest_lists;

    using ResolvedPath = std::pair<ObjectStoragePtr, String>;
    auto resolve_if_present = [&](const IcebergPathFromMetadata & path) -> std::optional<ResolvedPath>
    {
        auto resolved = resolveObjectStorageForPath(
            persistent_table_components.table_location, path.serialize(),
            object_storage, external_storages, context, resolver);
        if (!resolved.first->exists(StoredObject(resolved.second)))
            return std::nullopt;
        return resolved;
    };

    while (!metadata_worklist.empty())
    {
        auto historical_metadata_path = metadata_worklist.back();
        metadata_worklist.pop_back();
        visit_history(historical_metadata_path);

        auto resolved_metadata = resolve_if_present(historical_metadata_path);
        if (!resolved_metadata)
        {
            LOG_WARNING(
                log,
                "Historical metadata file {} is already deleted from storage, so the paths it referenced "
                "cannot be checked for locations outside the table's base directory",
                historical_metadata_path);
            continue;
        }

        auto & [historical_metadata_storage, historical_metadata_key] = *resolved_metadata;
        /// The metadata cache is keyed by path alone, which is not unique across storages.
        auto historical_metadata = getMetadataJSONObject(
            historical_metadata_key,
            historical_metadata_storage,
            /* metadata_cache */ nullptr,
            context,
            log,
            getCompressionMethodFromMetadataFile(historical_metadata_key),
            persistent_table_components.table_uuid);

        enqueue_log_entries(historical_metadata);
        collectStatisticsPaths(historical_metadata, f_statistics, visit_history);
        collectStatisticsPaths(historical_metadata, f_partition_statistics, visit_history);

        if (!historical_metadata->has(f_snapshots))
            continue;
        auto snapshots = historical_metadata->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
        if (!snapshots)
            continue;

        for (UInt32 i = 0; i < snapshots->size(); ++i)
        {
            auto snapshot = snapshots->getObject(i);
            if (!snapshot->has(f_manifest_list))
                continue;

            auto manifest_list_path = IcebergPathFromMetadata::deserialize(snapshot->getValue<String>(f_manifest_list));
            if (!traversed_manifest_lists.emplace(manifest_list_path).second)
                continue;
            visit_history(manifest_list_path);

            if (!resolve_if_present(manifest_list_path))
            {
                LOG_WARNING(
                    log,
                    "Manifest list {} of historical metadata file {} is already deleted from storage, so the "
                    "files it referenced cannot be checked for locations outside the table's base directory",
                    manifest_list_path,
                    historical_metadata_path);
                continue;
            }

            auto manifest_keys = getManifestList(
                object_storage, persistent_table_components, context, manifest_list_path, log, external_storages);
            for (const auto & manifest_entry : manifest_keys)
            {
                visit_history(manifest_entry.manifest_file_path);
                if (!resolve_if_present(manifest_entry.manifest_file_path))
                {
                    LOG_WARNING(
                        log,
                        "Manifest file {} of historical metadata file {} is already deleted from storage, so the "
                        "files it referenced cannot be checked for locations outside the table's base directory",
                        manifest_entry.manifest_file_path,
                        historical_metadata_path);
                    continue;
                }

                auto entries_handle = getManifestFileEntriesHandle(
                    object_storage, persistent_table_components, context, log, manifest_entry, current_schema_id, external_storages);
                for (auto content_type : {FileContentType::DATA, FileContentType::POSITION_DELETE, FileContentType::EQUALITY_DELETE})
                    for (const auto & entry : entries_handle.getFilesWithoutDeleted(content_type))
                        visit_history(entry->parsed_entry->file_path_key);
            }
        }
    }
}

}

ReachableFilesResult collectReachableFiles(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    const DataLakeStorageSettings & data_lake_settings,
    ContextPtr context,
    LoggerPtr log,
    ExternalStorageCache & external_storages,
    const std::shared_ptr<DataLake::ICatalog> & catalog,
    const String & table_identifier,
    bool scan_metadata_log_history,
    bool ignore_explicit_metadata_file_path)
{
    /// A higher metadata version may be uncommitted; follow the catalog head.
    auto [version, metadata_path, compression_method] = getLatestMetadataFileAndVersionWithCatalog(
        object_storage,
        catalog,
        table_identifier,
        persistent_table_components.table_path,
        data_lake_settings,
        persistent_table_components.metadata_cache,
        context,
        log.get(),
        persistent_table_components.table_uuid,
        persistent_table_components.metadata_compression_method,
        ignore_explicit_metadata_file_path);

    auto metadata = getMetadataJSONObject(
        metadata_path,
        object_storage,
        persistent_table_components.metadata_cache,
        context,
        log,
        compression_method,
        persistent_table_components.table_uuid);

    std::unordered_set<String> reachable;
    std::vector<std::pair<ObjectStoragePtr, String>> external_files;
    std::set<std::pair<const IObjectStorage *, String>> seen_external;
    const auto & resolver = persistent_table_components.path_resolver;

    String base_subtree_prefix = persistent_table_components.table_path;
    if (!base_subtree_prefix.empty() && base_subtree_prefix.back() != '/')
        base_subtree_prefix += '/';

    chassert(metadata_path.starts_with(base_subtree_prefix));

    auto visit = [&](const IcebergPathFromMetadata & path)
    {
        auto [storage, key] = resolveObjectStorageForPath(
            persistent_table_components.table_location, path.serialize(), object_storage, external_storages, context, resolver);
        if (storage.get() == object_storage.get() && key.starts_with(base_subtree_prefix))
            reachable.insert(std::move(key));
        else if (seen_external.emplace(storage.get(), key).second)
            external_files.emplace_back(std::move(storage), std::move(key));
    };

    collectMetadataRootFiles(
        metadata_path, metadata,
        resolver,
        visit,
        reachable);

    if (scan_metadata_log_history)
    {
        /// History contributes only existing external files; expired in-table files must remain eligible as orphans.
        std::set<std::pair<const IObjectStorage *, String>> missing_external;
        auto visit_history = [&](const IcebergPathFromMetadata & path)
        {
            auto [storage, key] = resolveObjectStorageForPath(
                persistent_table_components.table_location, path.serialize(), object_storage, external_storages, context, resolver);
            if (storage.get() == object_storage.get() && key.starts_with(base_subtree_prefix))
                return;

            const std::pair<const IObjectStorage *, String> external_id{storage.get(), key};
            if (seen_external.contains(external_id) || missing_external.contains(external_id))
                return;

            if (!storage->exists(StoredObject(key)))
            {
                missing_external.insert(external_id);
                return;
            }

            seen_external.insert(external_id);
            external_files.emplace_back(std::move(storage), std::move(key));
        };
        collectHistoricalReferences(
            metadata, object_storage, persistent_table_components, context, log, external_storages,
            metadata->getValue<Int32>(f_current_schema_id), visit_history);
    }

    if (!metadata->has(f_snapshots))
    {
        LOG_INFO(log, "No snapshots in metadata, reachable set contains only metadata-root files");
        return {std::move(reachable), version, metadata_path, std::move(external_files)};
    }

    auto snapshots = metadata->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    if (!snapshots || snapshots->size() == 0)
    {
        LOG_INFO(log, "Empty snapshots array, reachable set contains only metadata-root files");
        return {std::move(reachable), version, metadata_path, std::move(external_files)};
    }

    Int32 current_schema_id = metadata->getValue<Int32>(f_current_schema_id);

    auto snapshot_files = collectSnapshotReferencedFiles(
        snapshots, object_storage, persistent_table_components, context, log, current_schema_id, external_storages);

    for (const auto & path : snapshot_files.manifest_list_paths)
        visit(path);
    for (const auto & path : snapshot_files.manifest_paths)
        visit(path);
    for (const auto & path : snapshot_files.data_file_paths)
        visit(path);

    LOG_INFO(log, "Collected {} reachable files from metadata graph ({} outside the base subtree)",
        reachable.size(), external_files.size());
    return {std::move(reachable), version, metadata_path, std::move(external_files)};
}

}

#endif
