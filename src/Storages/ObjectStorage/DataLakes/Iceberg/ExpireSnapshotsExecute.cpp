#include "config.h"
#if USE_AVRO

#include <limits>
#include <set>
#include <unordered_set>

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DataLake/Common.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExecuteCommandArgs.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExpireSnapshotsExecute.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExpireSnapshotsTypes.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergFieldParseHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExternalPathResolver.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int LIMIT_EXCEEDED;
extern const int NOT_IMPLEMENTED;
}

namespace DataLakeStorageSetting
{
extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace Setting
{
extern const SettingsInt64 iceberg_expire_default_min_snapshots_to_keep;
extern const SettingsInt64 iceberg_expire_default_max_snapshot_age_ms;
extern const SettingsInt64 iceberg_expire_default_max_ref_age_ms;
}

namespace Iceberg
{

namespace
{

constexpr auto MAX_TRANSACTION_RETRIES = 1000;

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

ExecuteCommandArgs makeSchema()
{
    ExecuteCommandArgs schema("expire_snapshots");
    schema.addPositional("expire_before", Field::Types::String);
    schema.addNamed("retention_period");
    schema.addNamed("retain_last");
    schema.addNamed("snapshot_ids");
    schema.addNamed("dry_run");
    schema.addDefault("dry_run", Field(UInt64(0)));
    return schema;
}

ExpireSnapshotsOptions buildOptions(const ExecuteCommandArgs::Result & parsed)
{
    ExpireSnapshotsOptions options;
    static constexpr std::string_view cmd = "expire_snapshots";

    if (parsed.has("expire_before"))
    {
        String ts = parsed.getAs<String>("expire_before");
        ReadBufferFromString buf(ts);
        time_t expire_time = 0;
        readDateTimeText(expire_time, buf);
        options.expire_before_ms = static_cast<Int64>(expire_time) * 1000;
    }

    if (parsed.has("retention_period"))
        options.retention_period_ms = fieldToPeriodMs(parsed.get("retention_period"), cmd, "retention_period");

    if (parsed.has("retain_last"))
    {
        Int64 retain_last = fieldToInt64(parsed.get("retain_last"), cmd, "retain_last");
        if (retain_last <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects 'retain_last' to be positive");
        if (retain_last > std::numeric_limits<Int32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots 'retain_last' is too large: {}", retain_last);
        options.retain_last = static_cast<Int32>(retain_last);
    }

    if (parsed.has("snapshot_ids"))
        options.snapshot_ids = fieldToInt64Array(parsed.get("snapshot_ids"), cmd, "snapshot_ids");

    if (parsed.has("dry_run"))
        options.dry_run = fieldToBool(parsed.get("dry_run"), cmd, "dry_run");

    if (options.snapshot_ids && (options.retention_period_ms || options.retain_last))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "expire_snapshots argument 'snapshot_ids' cannot be combined with 'retention_period' or 'retain_last'");

    return options;
}

Pipe resultToPipe(const ExpireSnapshotsResult & result)
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
    add("deleted_statistics_files_count", result.deleted_statistics_files_count);
    add("failed_deletions_count", result.failed_deletions_count);
    add("dry_run", result.dry_run ? 1 : 0);

    const size_t rows = columns[0]->size();
    Chunk chunk(std::move(columns), rows);
    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(header)), std::move(chunk)));
}

// ---------------------------------------------------------------------------
// Retention policy
// ---------------------------------------------------------------------------

struct RetentionPolicy
{
    Int32 min_snapshots_to_keep = Iceberg::default_min_snapshots_to_keep;
    Int64 max_snapshot_age_ms = Iceberg::default_max_snapshot_age_ms;
    Int64 max_ref_age_ms = Iceberg::default_max_ref_age_ms;
};

RetentionPolicy readRetentionPolicy(const Poco::JSON::Object::Ptr & metadata, ContextPtr context, const ExpireSnapshotsOptions & options)
{
    RetentionPolicy policy;
    const auto & settings = context->getSettingsRef();
    Int64 min_keep_from_settings = settings[Setting::iceberg_expire_default_min_snapshots_to_keep].value;
    Int64 max_snapshot_age_from_settings = settings[Setting::iceberg_expire_default_max_snapshot_age_ms].value;
    Int64 max_ref_age_from_settings = settings[Setting::iceberg_expire_default_max_ref_age_ms].value;

    if (min_keep_from_settings <= 0 || min_keep_from_settings > std::numeric_limits<Int32>::max())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "iceberg_expire_default_min_snapshots_to_keep must be in range [1, {}], got {}",
            std::numeric_limits<Int32>::max(),
            min_keep_from_settings);
    if (max_snapshot_age_from_settings < 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "iceberg_expire_default_max_snapshot_age_ms must be non-negative, got {}",
            max_snapshot_age_from_settings);
    if (max_ref_age_from_settings < 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "iceberg_expire_default_max_ref_age_ms must be non-negative, got {}",
            max_ref_age_from_settings);

    policy.min_snapshots_to_keep = static_cast<Int32>(min_keep_from_settings);
    policy.max_snapshot_age_ms = max_snapshot_age_from_settings;
    policy.max_ref_age_ms = max_ref_age_from_settings;

    if (metadata->has(Iceberg::f_properties))
    {
        auto props = metadata->getObject(Iceberg::f_properties);
        if (props->has(Iceberg::f_min_snapshots_to_keep))
            policy.min_snapshots_to_keep = std::stoi(props->getValue<String>(Iceberg::f_min_snapshots_to_keep));
        if (props->has(Iceberg::f_max_snapshot_age_ms))
            policy.max_snapshot_age_ms = std::stoll(props->getValue<String>(Iceberg::f_max_snapshot_age_ms));
        if (props->has(Iceberg::f_max_ref_age_ms))
            policy.max_ref_age_ms = std::stoll(props->getValue<String>(Iceberg::f_max_ref_age_ms));
    }

    if (options.retain_last.has_value())
        policy.min_snapshots_to_keep = *options.retain_last;
    if (options.retention_period_ms.has_value())
        policy.max_snapshot_age_ms = *options.retention_period_ms;

    return policy;
}

// ---------------------------------------------------------------------------
// Snapshot graph + retention policy application
// ---------------------------------------------------------------------------

class SnapshotGraph
{
public:
    explicit SnapshotGraph(const Poco::JSON::Array::Ptr & snapshots)
    {
        for (UInt32 i = 0; i < snapshots->size(); ++i)
        {
            auto snapshot = snapshots->getObject(i);
            Int64 snap_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
            timestamps[snap_id] = snapshot->getValue<Int64>(Iceberg::f_timestamp_ms);
            if (snapshot->has(Iceberg::f_parent_snapshot_id) && !snapshot->isNull(Iceberg::f_parent_snapshot_id))
                parent_chain[snap_id] = snapshot->getValue<Int64>(Iceberg::f_parent_snapshot_id);
        }
    }

    bool hasSnapshot(Int64 snap_id) const { return timestamps.contains(snap_id); }

    Int64 getTimestamp(Int64 snap_id) const { return timestamps.at(snap_id); }

    std::optional<Int64> getParent(Int64 snap_id) const
    {
        auto it = parent_chain.find(snap_id);
        return it != parent_chain.end() ? std::optional(it->second) : std::nullopt;
    }

    void walkBranchAncestors(Int64 now_ms, Int64 head_id, Int32 min_keep, Int64 max_age_ms, std::set<Int64> & retained) const
    {
        Int64 walk_id = head_id;
        Int32 count = 0;
        while (hasSnapshot(walk_id))
        {
            bool within_min_keep = (count < min_keep);
            bool within_max_age = (now_ms - getTimestamp(walk_id) <= max_age_ms);
            if (!within_min_keep && !within_max_age)
                break;
            retained.insert(walk_id);
            ++count;
            auto parent = getParent(walk_id);
            if (!parent)
                break;
            walk_id = *parent;
        }
    }

private:
    std::unordered_map<Int64, Int64> parent_chain;
    std::unordered_map<Int64, Int64> timestamps;
};

std::pair<std::set<Int64>, Strings> applyRetentionPolicy(
    const Poco::JSON::Object::Ptr & metadata,
    Int64 current_snapshot_id,
    const SnapshotGraph & graph,
    const RetentionPolicy & policy,
    Int64 now_ms)
{
    std::set<Int64> retained;
    Strings expired_ref_names;
    bool main_branch_walked = false;
    if (metadata->has(Iceberg::f_refs))
    {
        auto refs = metadata->getObject(Iceberg::f_refs);
        for (const auto & ref_name : refs->getNames())
        {
            auto ref_obj = refs->getObject(ref_name);
            Int64 ref_snap_id = ref_obj->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
            String ref_type = ref_obj->getValue<String>(Iceberg::f_type);

            Int64 ref_max_ref_age = ref_obj->has(Iceberg::f_ref_max_ref_age_ms)
                ? ref_obj->getValue<Int64>(Iceberg::f_ref_max_ref_age_ms)
                : policy.max_ref_age_ms;

            bool is_main = (ref_name == Iceberg::f_main);

            if (!graph.hasSnapshot(ref_snap_id))
            {
                if (!is_main)
                {
                    LOG_WARNING(getLogger("IcebergExpireSnapshots"),
                        "Removing invalid ref {}: snapshot {} does not exist", ref_name, ref_snap_id);
                    expired_ref_names.push_back(ref_name);
                }
                else
                {
                    LOG_WARNING(getLogger("IcebergExpireSnapshots"),
                        "Main ref points to missing snapshot {}; falling back to current_snapshot_id walk", ref_snap_id);
                }
                continue;
            }

            bool ref_expired = !is_main && (now_ms - graph.getTimestamp(ref_snap_id)) > ref_max_ref_age;

            if (ref_expired)
            {
                expired_ref_names.push_back(ref_name);
                continue;
            }

            if (ref_type == Iceberg::f_branch)
            {
                Int32 min_keep = ref_obj->has(Iceberg::f_ref_min_snapshots_to_keep)
                    ? ref_obj->getValue<Int32>(Iceberg::f_ref_min_snapshots_to_keep)
                    : policy.min_snapshots_to_keep;
                Int64 max_age = ref_obj->has(Iceberg::f_ref_max_snapshot_age_ms)
                    ? ref_obj->getValue<Int64>(Iceberg::f_ref_max_snapshot_age_ms)
                    : policy.max_snapshot_age_ms;
                graph.walkBranchAncestors(now_ms, ref_snap_id, min_keep, max_age, retained);
                if (is_main)
                    main_branch_walked = true;
            }
            else if (ref_type == Iceberg::f_tag)
            {
                retained.insert(ref_snap_id);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "expire_snapshots: unexpected ref type '{}' for ref '{}'", ref_type, ref_name);
            }
        }
    }

    if (!main_branch_walked)
        graph.walkBranchAncestors(now_ms, current_snapshot_id, policy.min_snapshots_to_keep, policy.max_snapshot_age_ms, retained);

    return {retained, expired_ref_names};
}

// ---------------------------------------------------------------------------
// File collection helpers
// ---------------------------------------------------------------------------

/// Resolve a metadata path to the identity of the object it points at, so files spelled
/// differently (s3:// vs s3a:// vs https) but pointing at the same object compare equal.
Iceberg::IcebergPathFromMetadata resolveFileIdentity(
    const Iceberg::IcebergPathFromMetadata & path,
    const ObjectStoragePtr & object_storage,
    const PersistentTableComponents & persistent_table_components,
    const ContextPtr & context,
    ExternalStorageCache & external_storages)
{
    auto [storage, key] = resolveObjectStorageForPath(
        persistent_table_components.path_resolver.getTableLocation(),
        path.serialize(), object_storage, external_storages, context,
        persistent_table_components.path_resolver);
    return Iceberg::IcebergPathFromMetadata::makeStorageIdentity(storage, key);
}

/// True when the object is definitively absent. `expireSnapshots` commits the new metadata - which
/// drops the last reference to the expired subtree - before it deletes anything, so a subtree that
/// `collectExpiredFiles` skips is leaked permanently and is not even reflected in
/// `failed_deletions_count`. The only failure that is safe to ignore is therefore a file a previous
/// interrupted expiration already deleted; everything else must fail closed. A probe that cannot
/// answer returns `false`, so the original error propagates.
bool isAlreadyGone(
    const Iceberg::IcebergPathFromMetadata & path,
    const ObjectStoragePtr & object_storage,
    const PersistentTableComponents & persistent_table_components,
    const ContextPtr & context,
    ExternalStorageCache & external_storages)
{
    try
    {
        auto [storage, key] = resolveObjectStorageForPath(
            persistent_table_components.path_resolver.getTableLocation(),
            path.serialize(), object_storage, external_storages, context,
            persistent_table_components.path_resolver);
        return !storage->exists(StoredObject(key));
    }
    catch (...) /// Ok: the probe cannot answer, so the original error is rethrown by the caller
    {
        return false;
    }
}

void collectAllFilePaths(
    const Iceberg::ManifestFileIterator::ManifestFileEntriesHandle & entries_handle,
    const ObjectStoragePtr & object_storage,
    const PersistentTableComponents & persistent_table_components,
    const ContextPtr & context,
    ExternalStorageCache & external_storages,
    std::set<Iceberg::IcebergPathFromMetadata> & out)
{
    for (auto content_type : {FileContentType::DATA, FileContentType::POSITION_DELETE, FileContentType::EQUALITY_DELETE})
        for (const auto & entry : entries_handle.getFilesWithoutDeleted(content_type))
            out.insert(resolveFileIdentity(
                entry->parsed_entry->file_path_key, object_storage, persistent_table_components, context, external_storages));
}

void collectRetainedFiles(
    const Poco::JSON::Array::Ptr & retained_snapshots,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id,
    std::set<Iceberg::IcebergPathFromMetadata> & retained_manifest_paths,
    std::set<Iceberg::IcebergPathFromMetadata> & retained_data_file_paths,
    std::set<Iceberg::IcebergPathFromMetadata> & retained_manifest_list_paths,
    ExternalStorageCache & external_storages)
{
    for (UInt32 i = 0; i < retained_snapshots->size(); ++i)
    {
        auto snapshot = retained_snapshots->getObject(i);
        if (!snapshot->has(Iceberg::f_manifest_list))
            continue;

        auto manifest_list_path = IcebergPathFromMetadata::deserialize(snapshot->getValue<String>(Iceberg::f_manifest_list));
        retained_manifest_list_paths.insert(
            resolveFileIdentity(manifest_list_path, object_storage, persistent_table_components, context, external_storages));

        auto manifest_keys = getManifestList(
            object_storage, persistent_table_components, context, manifest_list_path, log, external_storages);

        for (const auto & manifest_entry : manifest_keys)
        {
            retained_manifest_paths.insert(
                resolveFileIdentity(manifest_entry.manifest_file_path, object_storage, persistent_table_components, context, external_storages));
            auto entries_handle = getManifestFileEntriesHandle(
                object_storage, persistent_table_components, context, log,
                manifest_entry, current_schema_id, external_storages);
            collectAllFilePaths(
                entries_handle, object_storage, persistent_table_components, context, external_storages, retained_data_file_paths);
        }
    }
}

/// A manifest may be deleted only once every entry it lists is gone, and a manifest list only once
/// every manifest it lists is gone; otherwise a leaf that survived is left with nothing naming it.
/// The leaf roles double as the categories the command counts in.
enum class ExpiredFileRole : uint8_t
{
    DataFile,
    PositionDelete,
    EqualityDelete,
    Manifest,
    ManifestList,
};

struct ExpiredFile
{
    Iceberg::IcebergPathFromMetadata path;
    ExpiredFileRole role;
};

/// Per-category counts, of the files a run plans to delete and of the ones it did delete.
struct ExpiredFileCounts
{
    Int64 data_files = 0;
    Int64 position_delete_files = 0;
    Int64 equality_delete_files = 0;
    Int64 manifest_files = 0;
    Int64 manifest_lists = 0;

    Int64 & of(ExpiredFileRole role)
    {
        switch (role)
        {
            case ExpiredFileRole::DataFile: return data_files;
            case ExpiredFileRole::PositionDelete: return position_delete_files;
            case ExpiredFileRole::EqualityDelete: return equality_delete_files;
            case ExpiredFileRole::Manifest: return manifest_files;
            case ExpiredFileRole::ManifestList: return manifest_lists;
        }
    }
};

struct ExpiredFiles
{
    /// Leaf-first: a manifest's entries, then the manifest; a manifest list after all its manifests.
    std::vector<ExpiredFile> all_paths;
    ExpiredFileCounts planned;
};

ExpiredFiles collectExpiredFiles(
    const std::vector<Iceberg::IcebergPathFromMetadata> & expired_manifest_list_paths,
    const std::set<Iceberg::IcebergPathFromMetadata> & retained_manifest_list_paths,
    const std::set<Iceberg::IcebergPathFromMetadata> & retained_manifest_paths,
    const std::set<Iceberg::IcebergPathFromMetadata> & retained_data_file_paths,
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr context,
    LoggerPtr log,
    Int32 current_schema_id,
    ExternalStorageCache & external_storages)
{
    ExpiredFiles result;
    std::set<Iceberg::IcebergPathFromMetadata> seen_expired_manifest_list_paths;
    std::set<Iceberg::IcebergPathFromMetadata> seen_expired_manifest_paths;
    for (const auto & manifest_list_path : expired_manifest_list_paths)
    {
        Iceberg::IcebergPathFromMetadata manifest_list_id;
        try
        {
            manifest_list_id = resolveFileIdentity(manifest_list_path, object_storage, persistent_table_components, context, external_storages);
        }
        catch (...)
        {
            /// Fail closed: an unresolvable manifest list cannot even be probed for existence, and
            /// skipping it would leak its whole subtree once the new metadata is committed below.
            tryLogCurrentException(log, fmt::format("Failed to resolve manifest list {}", manifest_list_path));
            throw;
        }
        if (retained_manifest_list_paths.contains(manifest_list_id))
            continue;

        if (seen_expired_manifest_list_paths.contains(manifest_list_id))
            continue;

        ManifestFileCacheKeys manifest_keys;
        try
        {
            manifest_keys = getManifestList(object_storage, persistent_table_components, context, manifest_list_path, log, external_storages);
        }
        catch (...)
        {
            if (!isAlreadyGone(manifest_list_path, object_storage, persistent_table_components, context, external_storages))
            {
                tryLogCurrentException(log, fmt::format("Failed to read manifest list {}", manifest_list_path));
                throw;
            }
            LOG_WARNING(log, "Manifest list {} is already gone, skipping", manifest_list_path);
            continue;
        }

        for (const auto & manifest_entry : manifest_keys)
        {
            Iceberg::IcebergPathFromMetadata manifest_id;
            try
            {
                manifest_id = resolveFileIdentity(manifest_entry.manifest_file_path, object_storage, persistent_table_components, context, external_storages);
            }
            catch (...)
            {
                /// Fail closed, as for the manifest list above.
                tryLogCurrentException(log, fmt::format("Failed to resolve manifest file {}", manifest_entry.manifest_file_path));
                throw;
            }
            if (retained_manifest_paths.contains(manifest_id))
                continue;

            if (seen_expired_manifest_paths.contains(manifest_id))
                continue;

            try
            {
                auto entries_handle = getManifestFileEntriesHandle(
                    object_storage, persistent_table_components, context, log,
                    manifest_entry, current_schema_id, external_storages);

                for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::DATA))
                    if (!retained_data_file_paths.contains(resolveFileIdentity(entry->parsed_entry->file_path_key, object_storage, persistent_table_components, context, external_storages)))
                    {
                        result.all_paths.push_back({entry->parsed_entry->file_path_key, ExpiredFileRole::DataFile});
                        ++result.planned.data_files;
                    }
                for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE))
                    if (!retained_data_file_paths.contains(resolveFileIdentity(entry->parsed_entry->file_path_key, object_storage, persistent_table_components, context, external_storages)))
                    {
                        result.all_paths.push_back({entry->parsed_entry->file_path_key, ExpiredFileRole::PositionDelete});
                        ++result.planned.position_delete_files;
                    }
                for (const auto & entry : entries_handle.getFilesWithoutDeleted(FileContentType::EQUALITY_DELETE))
                    if (!retained_data_file_paths.contains(resolveFileIdentity(entry->parsed_entry->file_path_key, object_storage, persistent_table_components, context, external_storages)))
                    {
                        result.all_paths.push_back({entry->parsed_entry->file_path_key, ExpiredFileRole::EqualityDelete});
                        ++result.planned.equality_delete_files;
                    }
            }
            catch (...)
            {
                if (!isAlreadyGone(
                        manifest_entry.manifest_file_path, object_storage, persistent_table_components, context, external_storages))
                {
                    tryLogCurrentException(log, fmt::format("Failed to read manifest file {}", manifest_entry.manifest_file_path));
                    throw;
                }
                LOG_WARNING(log, "Manifest file {} is already gone, skipping", manifest_entry.manifest_file_path);
                continue;
            }

            seen_expired_manifest_paths.insert(manifest_id);
            result.all_paths.push_back({manifest_entry.manifest_file_path, ExpiredFileRole::Manifest});
            ++result.planned.manifest_files;
        }

        seen_expired_manifest_list_paths.insert(manifest_list_id);
        result.all_paths.push_back({manifest_list_path, ExpiredFileRole::ManifestList});
        ++result.planned.manifest_lists;
    }
    return result;
}

// ---------------------------------------------------------------------------
// Snapshot partitioning
// ---------------------------------------------------------------------------

void trimSnapshotLog(
    Poco::JSON::Object::Ptr metadata,
    const std::set<Int64> & expired_snapshot_ids)
{
    if (!metadata->has(Iceberg::f_snapshot_log))
        return;

    auto snapshot_log = metadata->get(Iceberg::f_snapshot_log).extract<Poco::JSON::Array::Ptr>();
    Int32 suffix_start = static_cast<Int32>(snapshot_log->size());
    for (Int32 j = static_cast<Int32>(snapshot_log->size()) - 1; j >= 0; --j)
    {
        auto entry = snapshot_log->getObject(static_cast<UInt32>(j));
        Int64 snap_id = entry->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        if (expired_snapshot_ids.contains(snap_id))
            break;
        suffix_start = j;
    }
    Poco::JSON::Array::Ptr retained_log = new Poco::JSON::Array;
    for (UInt32 j = static_cast<UInt32>(suffix_start); j < snapshot_log->size(); ++j)
        retained_log->add(snapshot_log->getObject(j));
    metadata->set(Iceberg::f_snapshot_log, retained_log);
}

struct SnapshotPartition
{
    Poco::JSON::Array::Ptr retained_snapshots = new Poco::JSON::Array;
    std::set<Int64> expired_snapshot_ids;
    std::vector<Iceberg::IcebergPathFromMetadata> expired_manifest_list_paths;
};

SnapshotPartition partitionSnapshots(
    const Poco::JSON::Array::Ptr & snapshots,
    const std::set<Int64> & retention_retained_ids,
    std::optional<Int64> expire_before_ms)
{
    SnapshotPartition result;
    for (UInt32 i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(i);
        Int64 snap_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        Int64 snap_ts = snapshot->getValue<Int64>(Iceberg::f_timestamp_ms);

        bool is_retained_by_policy = retention_retained_ids.contains(snap_id);
        bool is_protected_by_fuse = expire_before_ms.has_value() && (snap_ts >= *expire_before_ms);

        if (is_retained_by_policy || is_protected_by_fuse)
        {
            result.retained_snapshots->add(snapshot);
        }
        else
        {
            result.expired_snapshot_ids.insert(snap_id);
            if (snapshot->has(Iceberg::f_manifest_list))
                result.expired_manifest_list_paths.push_back(
                    Iceberg::IcebergPathFromMetadata::deserialize(snapshot->getValue<String>(Iceberg::f_manifest_list)));
        }
    }
    return result;
}

SnapshotPartition partitionSnapshotsByIds(
    const Poco::JSON::Object::Ptr & metadata,
    const Poco::JSON::Array::Ptr & snapshots,
    const std::vector<Int64> & snapshot_ids,
    Int64 current_snapshot_id,
    std::optional<Int64> expire_before_ms)
{
    std::unordered_set<Int64> requested_ids(snapshot_ids.begin(), snapshot_ids.end());
    std::unordered_set<Int64> existing_ids;
    std::unordered_set<Int64> ref_protected_ids;
    SnapshotPartition result;

    if (metadata->has(Iceberg::f_refs))
    {
        auto refs = metadata->getObject(Iceberg::f_refs);
        for (const auto & ref_name : refs->getNames())
        {
            auto ref = refs->getObject(ref_name);
            if (ref->has(Iceberg::f_metadata_snapshot_id))
                ref_protected_ids.insert(ref->getValue<Int64>(Iceberg::f_metadata_snapshot_id));
        }
    }

    ref_protected_ids.insert(current_snapshot_id);

    for (UInt32 i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(i);
        Int64 snap_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        Int64 snap_ts = snapshot->getValue<Int64>(Iceberg::f_timestamp_ms);

        existing_ids.insert(snap_id);
        bool requested = requested_ids.contains(snap_id);
        bool is_protected_by_fuse = expire_before_ms.has_value() && (snap_ts >= *expire_before_ms);

        if (requested && ref_protected_ids.contains(snap_id))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "expire_snapshots cannot expire snapshot {} because it is referenced by current snapshot, branch, or tag",
                snap_id);

        if (requested && !is_protected_by_fuse)
        {
            result.expired_snapshot_ids.insert(snap_id);
            if (snapshot->has(Iceberg::f_manifest_list))
                result.expired_manifest_list_paths.push_back(Iceberg::IcebergPathFromMetadata::deserialize(snapshot->getValue<String>(Iceberg::f_manifest_list)));
        }
        else
        {
            result.retained_snapshots->add(snapshot);
        }
    }

    for (Int64 requested_id : requested_ids)
    {
        if (!existing_ids.contains(requested_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots snapshot id {} does not exist", requested_id);
    }

    return result;
}

// ---------------------------------------------------------------------------
// Metadata mutation + file deletion
// ---------------------------------------------------------------------------

void updateMetadataForExpiration(
    Poco::JSON::Object::Ptr metadata,
    const Strings & expired_ref_names,
    const Poco::JSON::Array::Ptr & retained_snapshots,
    const std::set<Int64> & expired_snapshot_ids)
{
    for (const auto & ref_name : expired_ref_names)
        metadata->getObject(Iceberg::f_refs)->remove(ref_name);

    metadata->set(Iceberg::f_snapshots, retained_snapshots);
    trimSnapshotLog(metadata, expired_snapshot_ids);

    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    metadata->set(Iceberg::f_last_updated_ms, ms.count());
}

struct DeletionOutcome
{
    ExpiredFileCounts deleted;
    /// Files expired from the metadata that stayed in the object storage.
    Int64 failed = 0;
};

/// Deletes the expired files and reports per category what was really removed. The new metadata is
/// already committed, so a manifest is all that still names its entries and a manifest list all that
/// still names its manifests: keep either whenever something below it survived, counting it as a
/// failed deletion rather than a deleted one.
DeletionOutcome deleteExpiredFiles(
    const std::vector<ExpiredFile> & files_to_delete,
    const Iceberg::IcebergPathResolver & path_resolver,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    LoggerPtr log,
    ExternalStorageCache & external_storages)
{
    DeletionOutcome outcome;
    /// Whether something under the manifest, resp. the manifest list, being walked survived.
    bool manifest_incomplete = false;
    bool manifest_list_incomplete = false;

    for (const auto & [file_path, role] : files_to_delete)
    {
        const bool keep_anchor
            = (role == ExpiredFileRole::Manifest && manifest_incomplete)
            || (role == ExpiredFileRole::ManifestList && manifest_list_incomplete);

        if (keep_anchor)
        {
            ++outcome.failed;
            LOG_WARNING(log, "Keeping {}: a file it references could not be deleted", file_path);
        }
        else
        {
            try
            {
                auto [storage_to_use, key_in_storage] = resolveObjectStorageForPath(
                    path_resolver.getTableLocation(), file_path.serialize(), object_storage, external_storages, context,
                    path_resolver);
                storage_to_use->removeObjectIfExists(StoredObject(key_in_storage));
                ++outcome.deleted.of(role);
                LOG_DEBUG(log, "Deleted expired file {}", file_path);
            }
            catch (...)
            {
                ++outcome.failed;
                LOG_WARNING(log, "Failed to delete file {}: {}", file_path, getCurrentExceptionMessage(false));

                /// A leaf marks both, so a kept manifest keeps its manifest list without a further flag.
                if (role != ExpiredFileRole::Manifest && role != ExpiredFileRole::ManifestList)
                    manifest_incomplete = true;
                manifest_list_incomplete = true;
            }
        }

        if (role == ExpiredFileRole::Manifest)
            manifest_incomplete = false;
        else if (role == ExpiredFileRole::ManifestList)
            manifest_list_incomplete = false;
    }
    return outcome;
}

}


// ---------------------------------------------------------------------------
// Public: expireSnapshots orchestration
// ---------------------------------------------------------------------------

ExpireSnapshotsResult expireSnapshots(
    const ExpireSnapshotsOptions & options,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name,
    ExternalStorageCache & external_storages)
{
    auto common_path = persistent_table_components.table_path;
    if (!common_path.starts_with('/'))
        common_path = "/" + common_path;

    if (catalog && catalog->isTransactional())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "expire_snapshots is not supported for Iceberg tables backed by a transactional catalog");

    int max_retries = MAX_TRANSACTION_RETRIES;
    while (--max_retries > 0)
    {
        FileNamesGenerator filename_generator(persistent_table_components.path_resolver.getTableLocation(), false, CompressionMethod::None, write_format);
        auto log = getLogger("IcebergExpireSnapshots");
        auto [last_version, metadata_path, compression_method] = getLatestMetadataFileAndVersionWithCatalog(
            object_storage,
            catalog,
            table_name,
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method);

        filename_generator.setVersion(last_version + 1);
        filename_generator.setCompressionMethod(compression_method);

        auto metadata = getMetadataJSONObject(
            metadata_path,
            object_storage,
            persistent_table_components.metadata_cache,
            context,
            log,
            compression_method,
            persistent_table_components.table_uuid);

        if (metadata->getValue<Int32>(f_format_version) < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots is supported only for the second version of iceberg format");

        if (!metadata->has(Iceberg::f_current_snapshot_id))
        {
            LOG_INFO(log, "No snapshots to expire (table has no current snapshot)");
            return {.dry_run = options.dry_run};
        }

        Int64 current_snapshot_id = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);
        if (current_snapshot_id < 0)
        {
            LOG_INFO(log, "No snapshots to expire (table has no current snapshot)");
            return {.dry_run = options.dry_run};
        }

        auto snapshots = metadata->get(Iceberg::f_snapshots).extract<Poco::JSON::Array::Ptr>();
        auto now_ms = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        Strings expired_ref_names;
        SnapshotPartition partition;
        if (options.snapshot_ids.has_value())
        {
            partition = partitionSnapshotsByIds(metadata, snapshots, *options.snapshot_ids, current_snapshot_id, options.expire_before_ms);
        }
        else
        {
            auto policy = readRetentionPolicy(metadata, context, options);
            SnapshotGraph graph(snapshots);
            auto [retention_retained_ids, retention_expired_ref_names] = applyRetentionPolicy(metadata, current_snapshot_id, graph, policy, now_ms);
            expired_ref_names = std::move(retention_expired_ref_names);
            partition = partitionSnapshots(snapshots, retention_retained_ids, options.expire_before_ms);
        }

        if (partition.expired_snapshot_ids.empty())
        {
            LOG_INFO(log, "No snapshots to expire");
            return {.dry_run = options.dry_run};
        }
        LOG_INFO(log, "Expiring {} snapshots", partition.expired_snapshot_ids.size());

        Int32 current_schema_id = metadata->getValue<Int32>(Iceberg::f_current_schema_id);

        std::set<Iceberg::IcebergPathFromMetadata> retained_manifest_paths;
        std::set<Iceberg::IcebergPathFromMetadata> retained_data_file_paths;
        std::set<Iceberg::IcebergPathFromMetadata> retained_manifest_list_paths;
        collectRetainedFiles(
            partition.retained_snapshots, object_storage, persistent_table_components, context, log,
            current_schema_id, retained_manifest_paths, retained_data_file_paths, retained_manifest_list_paths,
            external_storages);
        auto expired_files = collectExpiredFiles(
            partition.expired_manifest_list_paths, retained_manifest_list_paths, retained_manifest_paths, retained_data_file_paths,
            object_storage, persistent_table_components, context, log, current_schema_id,
            external_storages);

        if (options.dry_run)
        {
            LOG_INFO(log, "Dry-run mode: skip metadata commit and file deletion");
            return ExpireSnapshotsResult{
                .deleted_data_files_count = expired_files.planned.data_files,
                .deleted_position_delete_files_count = expired_files.planned.position_delete_files,
                .deleted_equality_delete_files_count = expired_files.planned.equality_delete_files,
                .deleted_manifest_files_count = expired_files.planned.manifest_files,
                .deleted_manifest_lists_count = expired_files.planned.manifest_lists,
                .dry_run = true,
            };
        }

        updateMetadataForExpiration(metadata, expired_ref_names, partition.retained_snapshots, partition.expired_snapshot_ids);

        std::string json_representation = stringifyJSON(metadata, 4);
        auto metadata_info = filename_generator.generateMetadataPathWithInfo();
        auto hint_path = filename_generator.generateVersionHint();
        if (!writeMetadataFileAndVersionHint(
                persistent_table_components.path_resolver,
                metadata_info,
                json_representation,
                hint_path,
                object_storage,
                context,
                data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
        {
            LOG_WARNING(log, "Metadata commit conflict during expire_snapshots, retrying ({} retries left)", max_retries);
            continue;
        }

        if (catalog)
        {
            auto catalog_filename = persistent_table_components.path_resolver.resolveForCatalog(metadata_info.path);
            const auto & [namespace_name, parsed_table_name] = DataLake::parseTableName(table_name);
            if (!catalog->updateMetadata(namespace_name, parsed_table_name, catalog_filename, nullptr))
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Failed to update catalog metadata after writing new metadata file. "
                    "The table metadata may be in an inconsistent state");
            }
        }

        LOG_INFO(log, "Deleting {} expired files for {} expired snapshots", expired_files.all_paths.size(), partition.expired_snapshot_ids.size());
        auto outcome = deleteExpiredFiles(
            expired_files.all_paths, persistent_table_components.path_resolver, object_storage, context, log, external_storages);
        LOG_INFO(
            log,
            "Expired {} snapshots, deleted {} files, failed to delete {} files",
            partition.expired_snapshot_ids.size(),
            static_cast<Int64>(expired_files.all_paths.size()) - outcome.failed,
            outcome.failed);

        return ExpireSnapshotsResult{
            .deleted_data_files_count = outcome.deleted.data_files,
            .deleted_position_delete_files_count = outcome.deleted.position_delete_files,
            .deleted_equality_delete_files_count = outcome.deleted.equality_delete_files,
            .deleted_manifest_files_count = outcome.deleted.manifest_files,
            .deleted_manifest_lists_count = outcome.deleted.manifest_lists,
            .failed_deletions_count = outcome.failed,
            .dry_run = false,
        };
    }

    if (max_retries == 0)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessful retries to expire iceberg snapshots");

    UNREACHABLE();
}


// ---------------------------------------------------------------------------
// Public: executeExpireSnapshots (entry point from ALTER TABLE ... EXECUTE)
// ---------------------------------------------------------------------------

Pipe executeExpireSnapshots(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name,
    ExternalStorageCache & external_storages)
{
    auto parsed = makeSchema().parse(args);
    auto options = buildOptions(parsed);

    auto result = expireSnapshots(
        options,
        context,
        object_storage,
        data_lake_settings,
        persistent_components,
        write_format,
        catalog,
        table_name,
        external_storages);

    return resultToPipe(result);
}

}
}

#endif
