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
    /// `metadata_path` bypasses `visit`: it is already a base-storage key, not a URI-style path from
    /// metadata contents, so the resolver inside `visit` would misparse it.
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

/// Walk the historical metadata versions listed in `metadata-log` (recursively) and feed every path
/// they reference through `visit_history`, leaves included: the visitor decides what each one means.
/// History is inspected only to detect references outside the base subtree, so the visitor must not
/// extend the reachable set -- files under `table_path` referenced solely by expired history stay
/// eligible as orphans.
///
/// A historical metadata file, manifest list or manifest already deleted from storage is skipped with a
/// warning, because `remove_orphan_files` deletes exactly such files itself and refusing to run on them
/// would make its first successful run break every later one. Every other failure propagates: unlike a
/// deleted object it says nothing about what the file referenced.
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

    /// Hand back the resolved (storage, key) only while the object is still in storage, so the walk can
    /// tell an already-deleted historical reference from a failure that must propagate.
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
        /// Bypass the metadata cache: it is keyed by path only, and the same key means a different
        /// object on a secondary storage.
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
    /// The highest `v*.metadata.json` in storage can be a version the catalog never committed, and
    /// traversing it would report the files of the committed head as unreachable.
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

    /// `reachable` is matched against a base-storage listing of `table_path`, so keep only base-storage
    /// keys under that prefix and send everything else to `external_files`. The callers list `table_path`
    /// with a trailing '/', so normalize the prefix once and match that.
    String base_subtree_prefix = persistent_table_components.table_path;
    if (!base_subtree_prefix.empty() && base_subtree_prefix.back() != '/')
        base_subtree_prefix += '/';

    /// Every branch of `getLatestOrExplicitMetadataFileAndVersion` yields a base-storage key under
    /// `table_path/metadata/`, never an external path, which is what lets `collectMetadataRootFiles`
    /// insert it into `reachable` directly.
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
        /// History extends `external_files` only, never `reachable`: base-subtree files referenced
        /// solely by expired history stay eligible as orphans. And only an external object still in
        /// storage counts, since a deleted one leaves nothing outside `table_path` to clean; a missing
        /// one is remembered so the paths the walk repeats across versions cost one probe in total.
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
        /// Historical manifests are parsed with the current schema id, as `collectSnapshotReferencedFiles`
        /// does for pre-schema-change snapshots.
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
