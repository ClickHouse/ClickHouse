#include "config.h"
#if USE_AVRO

#include <chrono>
#include <unordered_set>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ExecuteCommandArgs.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/RemoveOrphanFilesExecute.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotFilesTraversal.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Setting
{
extern const SettingsUInt64 iceberg_orphan_files_older_than_seconds;
}

namespace Iceberg
{

struct RemoveOrphanFilesParams
{
    std::optional<time_t> older_than;
    std::optional<String> location;
    bool dry_run = false;
};

struct RemoveOrphanFilesResult
{
    Int64 deleted_data_files_count = 0;
    Int64 deleted_position_delete_files_count = 0;
    Int64 deleted_equality_delete_files_count = 0;
    Int64 deleted_manifest_files_count = 0;
    Int64 deleted_manifest_lists_count = 0;
    Int64 deleted_metadata_files_count = 0;
    Int64 deleted_statistics_files_count = 0;
    Int64 skipped_missing_metadata_count = 0;
    Int64 failed_deletions_count = 0;
};

struct OrphanScanResult
{
    std::vector<String> orphan_paths;
    Int64 skipped_missing_metadata = 0;
};

namespace
{

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

ExecuteCommandArgs makeSchema()
{
    ExecuteCommandArgs schema("remove_orphan_files");
    schema.addPositional("older_than", Field::Types::String);
    schema.addNamed("location", Field::Types::String);
    schema.addNamed("dry_run", Field::Types::UInt64);
    schema.addDefault("dry_run", Field(UInt64(0)));
    return schema;
}

// ---------------------------------------------------------------------------
// Orphan file detection
// ---------------------------------------------------------------------------

String resolveScanPath(const String & table_path, const RemoveOrphanFilesParams & params)
{
    String scan_path = table_path;
    if (!scan_path.ends_with('/'))
        scan_path += '/';

    if (params.location.has_value())
    {
        String loc = *params.location;
        if (loc.find("..") != String::npos || loc.starts_with('/'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "location must be a relative path under the table root, got '{}'", loc);

        while (loc.starts_with("./"))
            loc = loc.substr(2);

        scan_path += loc;

        if (!scan_path.ends_with('/'))
            scan_path += '/';
    }
    return scan_path;
}

OrphanScanResult findOrphanFiles(
    const RelativePathsWithMetadata & actual_files,
    const std::unordered_set<String> & reachable,
    time_t older_than_threshold,
    ObjectStoragePtr object_storage,
    LoggerPtr log)
{
    OrphanScanResult scan;

    for (const auto & file_ptr : actual_files)
    {
        const String & path = file_ptr->relative_path;

        if (reachable.contains(path))
            continue;

        std::optional<ObjectMetadata> fetched_metadata;
        if (!file_ptr->metadata.has_value())
        {
            fetched_metadata = object_storage->tryGetObjectMetadata(path, /* with_tags = */ false);
            if (!fetched_metadata)
            {
                ++scan.skipped_missing_metadata;
                LOG_DEBUG(log, "Skipping file without metadata (no last_modified): {}", path);
                continue;
            }
        }

        const auto & metadata = file_ptr->metadata.has_value() ? *file_ptr->metadata : *fetched_metadata;
        auto file_modified = metadata.last_modified.epochTime();
        if (static_cast<time_t>(file_modified) >= older_than_threshold)
            continue;

        LOG_DEBUG(log, "Orphan file: {}", path);
        scan.orphan_paths.push_back(path);
    }

    if (scan.skipped_missing_metadata > 0)
        LOG_WARNING(log, "Skipped {} unreferenced file(s) because last_modified metadata was unavailable; "
            "these files could not be age-checked and were conservatively kept", scan.skipped_missing_metadata);

    return scan;
}

// ---------------------------------------------------------------------------
// Orphan file deletion
// ---------------------------------------------------------------------------

struct DeleteOrphanResult
{
    std::vector<String> deleted_paths;
    Int64 failed_count = 0;
};

DeleteOrphanResult deleteOrphanFiles(
    const std::vector<String> & orphan_paths,
    ObjectStoragePtr object_storage,
    LoggerPtr log)
{
    DeleteOrphanResult result;

    for (const auto & path : orphan_paths)
    {
        try
        {
            object_storage->removeObjectIfExists(StoredObject(path));
            LOG_DEBUG(log, "Deleted orphan file {}", path);
            result.deleted_paths.push_back(path);
        }
        catch (...)
        {
            ++result.failed_count;
            LOG_WARNING(log, "Failed to delete orphan file {}: {}", path, getCurrentExceptionMessage(false));
        }
    }

    return result;
}

// ---------------------------------------------------------------------------
// Result aggregation
// ---------------------------------------------------------------------------

RemoveOrphanFilesResult tallyByCategory(const std::vector<String> & paths, Int64 skipped_missing_metadata)
{
    RemoveOrphanFilesResult r;
    for (const auto & path : paths)
    {
        switch (inspectFileCategory(path))
        {
            case FileCategory::DATA_FILE:                ++r.deleted_data_files_count; break;
            case FileCategory::POSITION_DELETE_FILE:      ++r.deleted_position_delete_files_count; break;
            case FileCategory::EQUALITY_DELETE_FILE:      ++r.deleted_equality_delete_files_count; break;
            case FileCategory::MANIFEST_FILE:             ++r.deleted_manifest_files_count; break;
            case FileCategory::MANIFEST_LIST:             ++r.deleted_manifest_lists_count; break;
            case FileCategory::METADATA_JSON:             ++r.deleted_metadata_files_count; break;
            case FileCategory::STATISTICS_FILE:           ++r.deleted_statistics_files_count; break;
        }
    }
    r.skipped_missing_metadata_count = skipped_missing_metadata;
    return r;
}

// ---------------------------------------------------------------------------
// Result → Pipe
// ---------------------------------------------------------------------------

Pipe resultToPipe(const RemoveOrphanFilesResult & result)
{
    Block header{
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "metric_name"),
        ColumnWithTypeAndName(std::make_shared<DataTypeInt64>(), "metric_value"),
    };

    MutableColumns columns = header.cloneEmptyColumns();

    auto add = [&](const char * name, Int64 value)
    {
        columns[0]->insert(String(name));
        columns[1]->insert(value);
    };

    add("deleted_data_files_count", result.deleted_data_files_count);
    add("deleted_position_delete_files_count", result.deleted_position_delete_files_count);
    add("deleted_equality_delete_files_count", result.deleted_equality_delete_files_count);
    add("deleted_manifest_files_count", result.deleted_manifest_files_count);
    add("deleted_manifest_lists_count", result.deleted_manifest_lists_count);
    add("deleted_metadata_files_count", result.deleted_metadata_files_count);
    add("deleted_statistics_files_count", result.deleted_statistics_files_count);
    add("skipped_missing_metadata_count", result.skipped_missing_metadata_count);
    add("failed_deletions_count", result.failed_deletions_count);

    const size_t rows = columns[0]->size();
    Chunk chunk(std::move(columns), rows);
    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(header)), std::move(chunk)));
}

// ---------------------------------------------------------------------------
// Core orchestration
// ---------------------------------------------------------------------------

RemoveOrphanFilesResult removeOrphanFiles(
    const RemoveOrphanFilesParams & params,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components)
{
    auto log = getLogger("IcebergRemoveOrphanFiles");

    auto [reachable, metadata_version] = collectReachableFiles(
        object_storage, persistent_table_components, data_lake_settings, context, log);

    String scan_path = resolveScanPath(persistent_table_components.table_path, params);
    if (!object_storage->existsOrHasAnyChild(scan_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Scan path '{}' does not exist", scan_path);
    RelativePathsWithMetadata actual_files;
    object_storage->listObjects(scan_path, actual_files, /* max_keys = */ 0);
    LOG_INFO(log, "Found {} actual files under scan path '{}'", actual_files.size(), scan_path);

    chassert(params.older_than.has_value());
    auto scan = findOrphanFiles(actual_files, reachable, *params.older_than, object_storage, log);
    LOG_INFO(log, "Found {} orphan files (dry_run={})", scan.orphan_paths.size(), params.dry_run);

    if (params.dry_run || scan.orphan_paths.empty())
        return tallyByCategory(scan.orphan_paths, scan.skipped_missing_metadata);

    auto [_recheck_files, recheck_version] = collectReachableFiles(
        object_storage, persistent_table_components, data_lake_settings, context, log);
    if (recheck_version != metadata_version)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Metadata version changed during orphan scan (v{} -> v{}); "
            "aborting to avoid deleting files referenced by a concurrent commit",
            metadata_version, recheck_version);

    auto delete_result = deleteOrphanFiles(scan.orphan_paths, object_storage, log);
    LOG_INFO(log, "Deleted {}/{} orphan files ({} failed)",
        delete_result.deleted_paths.size(), scan.orphan_paths.size(), delete_result.failed_count);

    auto result = tallyByCategory(delete_result.deleted_paths, scan.skipped_missing_metadata);
    result.failed_deletions_count = delete_result.failed_count;
    return result;
}

}


// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

Pipe executeRemoveOrphanFiles(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_components)
{
    if (persistent_components.format_version < 2)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "remove_orphan_files requires Iceberg format version >= 2, "
            "but this table uses format version {}",
            persistent_components.format_version);

    auto parsed = makeSchema().parse(args);

    RemoveOrphanFilesParams params;
    if (parsed.has("older_than"))
    {
        String older_than_str = parsed.getAs<String>("older_than");
        ReadBufferFromString buf(older_than_str);
        time_t ts;
        readDateTimeText(ts, buf);

        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        if (ts > now)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "older_than must not be in the future; a future value would bypass the in-progress-write safety window");

        params.older_than = ts;
    }
    else
    {
        UInt64 threshold_seconds = context->getSettingsRef()[Setting::iceberg_orphan_files_older_than_seconds].value;
        auto now = std::chrono::system_clock::now();
        params.older_than = std::chrono::system_clock::to_time_t(now - std::chrono::seconds(threshold_seconds));
    }
    if (parsed.has("location"))
        params.location = parsed.getAs<String>("location");
    params.dry_run = parsed.getAs<UInt64>("dry_run") != 0;

    auto result = removeOrphanFiles(params, context, object_storage, data_lake_settings, persistent_components);

    return resultToPipe(result);
}

}
}

#endif
