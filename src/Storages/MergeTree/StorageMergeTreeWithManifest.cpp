#include <Storages/MergeTree/StorageMergeTreeWithManifest.h>

#include <filesystem>
#include <Interpreters/Context.h>
#include <Common/escapeForFileName.h>
#include <Disks/DiskManifest.h>
#include <Storages/MergeTree/Manifest/IManifestStorage.h>
#include <Storages/MergeTree/Manifest/ManifestStorageFactory.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <fmt/format.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}

namespace MergeTreeSetting
{
extern const MergeTreeSettingsBool assign_part_uuids;
}

StorageMergeTreeWithManifest::StorageMergeTreeWithManifest(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata,
    LoadingStrictnessLevel mode,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_,
    const String & manifest_storage_type_)
    : StorageMergeTree(table_id_, metadata, mode, context_, date_column_name, merging_params_,
                       [&settings_, &manifest_storage_type_]() -> std::unique_ptr<MergeTreeSettings>
                       {
                           if (!manifest_storage_type_.empty() && settings_)
                               (*settings_)[MergeTreeSetting::assign_part_uuids] = true;
                           return std::move(settings_);
                       }())
    , manifest_storage_type(manifest_storage_type_)
{
    initializeDirectoriesAndFormatVersion(relative_data_path_, LoadingStrictnessLevel::ATTACH <= mode, date_column_name);

    initializeManifestIfNeeded();

    loadDataParts(LoadingStrictnessLevel::FORCE_RESTORE <= mode, std::nullopt);

    increment.set(getMaxBlockNumber());

    loadMutations();
    loadDeduplicationLog();

    prewarmCaches(getActivePartsLoadingThreadPool().get(), getMarkCacheToPrewarm(0), getPrimaryIndexCacheToPrewarm(0));
}

void StorageMergeTreeWithManifest::initializeManifestIfNeeded()
{
    if (!manifest_storage_type.empty())
    {
        initializeManifestStorage();
        initializeManifest();
    }
}

void StorageMergeTreeWithManifest::drop()
{
    StorageMergeTree::drop();

    manifest_disk.reset();

    if (manifest_storage)
    {
        manifest_storage->shutdown();
        manifest_storage->drop();
        manifest_storage.reset();
    }
}

void StorageMergeTreeWithManifest::initializeManifestStorage()
{
    if (manifest_storage_path.empty())
        manifest_storage_path = getManifestStoragePath();

    std::filesystem::create_directories(std::filesystem::path(manifest_storage_path));

    LOG_DEBUG(
        log,
        "Initializing manifest storage, type: {}, path: {}",
        manifest_storage_type,
        manifest_storage_path);

    manifest_storage = ManifestStorageFactory::create(manifest_storage_type, manifest_storage_path);
}

void StorageMergeTreeWithManifest::initializeManifest()
{
    if (!manifest_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ManifestStorage is not initialized");

    auto name = fmt::format("{}_manifest", getStorageID().getTableName());
    manifest_disk
        = std::make_shared<DiskManifest>(name, manifest_storage_path, getRelativeDataPath(), manifest_storage, format_version);
}

String StorageMergeTreeWithManifest::getManifestStoragePath() const
{
    String server_path = getContext()->getPath();
    String database_name = escapeForFileName(getStorageID().database_name);
    String table_name = escapeForFileName(getStorageID().table_name);

    std::filesystem::path fs_path = std::filesystem::path(server_path) / "manifest" / database_name / table_name;

    if (fs_path.is_relative())
        fs_path = std::filesystem::absolute(fs_path).lexically_normal();

    return fs_path.string();
}

void StorageMergeTreeWithManifest::commitToManifest(const DataPartPtr & part, ManifestOpType op_type) const
{
    if (!manifest_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ManifestStorage is not initialized");

    if (part->uuid == UUIDHelpers::Nil)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Part '{}' has no UUID assigned. Manifest requires UUID. "
                        "Please ensure assign_part_uuids setting is enabled.",
                        part->name);
    }

    auto entry = serialize(part, op_type);
    entry.part_uuid = toString(part->uuid);
    String value = entry.toString();

    throwIfNotOK(manifest_storage->put(toString(part->uuid), value), "commit part to manifest", part->name);
}

void StorageMergeTreeWithManifest::removeFromManifest(const String & part_uuid) const
{
    if (!manifest_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ManifestStorage is not initialized");

    throwIfNotOKOrNotFound(manifest_storage->del(part_uuid), "remove part from manifest", part_uuid);
}

MergeTreeData::LoadPartResult StorageMergeTreeWithManifest::loadDataPart(
    const MergeTreePartInfo & part_info,
    const String & part_name,
    const DiskPtr & part_disk,
    MergeTreeDataPartState to_state,
    DB::SharedMutex & part_loading_mutex)
{
    if (part_disk != manifest_disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Manifest-based part loading requires manifest_disk, but got disk '{}'. Part: '{}'",
                        part_disk ? part_disk->getName() : "nullptr",
                        part_name);

    auto it = manifest_disk->part_map.find(part_name);
    if (it == manifest_disk->part_map.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Part '{}' not found in manifest. It's a bug", part_name);

    const BaseManifestEntry & entry = it->second;

    String disk_name = entry.disk_name;
    DiskPtr actual_disk = nullptr;

    if (!disk_name.empty())
    {
        actual_disk = getStoragePolicy()->tryGetDiskByName(disk_name);
        if (actual_disk)
        {
            LOG_DEBUG(
                log,
                "Found disk '{}' by name for part '{}'",
                disk_name,
                part_name);
        }
    }

    if (!actual_disk)
    {
        LOG_DEBUG(
            log,
            "Disk '{}' not found by name for part '{}', falling back to native list disk approach",
            entry.disk_name,
            part_name);

        for (const auto & disk : getStoragePolicy()->getDisks())
        {
            if (disk->isBroken())
                continue;

            String part_path = std::filesystem::path(getRelativeDataPath()) / part_name;
            if (disk->existsDirectory(part_path))
            {
                actual_disk = disk;
                LOG_DEBUG(
                    log,
                    "Found part '{}' on disk '{}' using native list approach, updating manifest",
                    part_name,
                    disk->getName());
                break;
            }
        }
    }

    if (!actual_disk)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Disk not found for part '{}' (name: '{}'). Part may have been moved or disk configuration changed.",
                        part_name,
                        entry.disk_name);

    if (actual_disk->getName() != entry.disk_name)
    {
        LOG_INFO(
            log,
            "Part '{}' disk name changed from '{}' to '{}', updating manifest",
            part_name,
            entry.disk_name,
            actual_disk->getName());

        if (!entry.part_uuid.empty())
        {
            BaseManifestEntry updated_entry = entry;
            updated_entry.disk_name = actual_disk->getName();
            updated_entry.disk_path = actual_disk->getPath();
            try
            {
                throwIfNotOK(manifest_storage->put(entry.part_uuid, updated_entry.toString()), "update manifest for part", part_name);
            }
            catch (const Exception & e)
            {
                LOG_WARNING(
                    log,
                    "Failed to update manifest for part '{}': {}",
                    part_name,
                    e.message());
            }
        }
    }

    return MergeTreeData::loadDataPart(part_info, part_name, actual_disk, to_state, part_loading_mutex);
}

BaseManifestEntry StorageMergeTreeWithManifest::serialize(const DataPartPtr & part, ManifestOpType op_type) const
{
    BaseManifestEntry entry;
    entry.name = part->name;

    if (op_type == ManifestOpType::Commit)
        entry.state = static_cast<Int32>(MergeTreeDataPartState::Active);
    else if (op_type == ManifestOpType::PreDetach)
        entry.state = static_cast<Int32>(MergeTreeDataPartState::PreActive);
    else
        entry.state = static_cast<Int32>(part->getState());

    entry.disk_name = part->getDataPartStorage().getDiskName();
    entry.disk_path = part->getDataPartStorage().getDiskPath();

    return entry;
}

}
