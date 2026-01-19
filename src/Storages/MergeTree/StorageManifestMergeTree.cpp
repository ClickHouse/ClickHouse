#include <Storages/MergeTree/StorageManifestMergeTree.h>

#include <filesystem>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Disks/DiskManifest.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/Manifest/ManifestStorageFactory.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <base/JSON.h>
#include <fmt/format.h>
#include <Poco/Logger.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/TransactionID.h>
#include <Common/logger_useful.h>
#include "manifest.pb.h"

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int BAD_TTL_FILE;
extern const int CANNOT_READ_ALL_DATA;
extern const int DUPLICATE_DATA_PART;
}

namespace MergeTreeSetting
{
extern const MergeTreeSettingsFloat primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns;
}

StorageManifestMergeTree::StorageManifestMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata,
    LoadingStrictnessLevel mode,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_,
    const String & manifest_storage_type_)
    : StorageMergeTree(table_id_, relative_data_path_, metadata, mode, context_, date_column_name, merging_params_, std::move(settings_))
    , manifest_storage_type(manifest_storage_type_)
{
    // Initialize manifest storage if type is specified
    if (!manifest_storage_type.empty())
    {
        initializeManifestStorage();
        initializeManifest();
    }
}

void StorageManifestMergeTree::startup()
{
    StorageMergeTree::startup();
    // Manifest-specific startup logic can be added here if needed
}

void StorageManifestMergeTree::shutdown(bool is_drop)
{
    StorageMergeTree::shutdown(is_drop);

    if (manifest_storage)
    {
        manifest_storage->shutdown();
        manifest_storage.reset();
    }
}

void StorageManifestMergeTree::initializeManifestStorage()
{
    manifest_storage_path = getManifestStoragePath();
    std::filesystem::create_directories(std::filesystem::path(manifest_storage_path));

    LOG_DEBUG(
        &Poco::Logger::get("StorageManifestMergeTree"),
        "Initializing manifest storage, type: {}, path: {}",
        manifest_storage_type,
        manifest_storage_path);

    manifest_storage = ManifestStorageFactory::create(manifest_storage_type, manifest_storage_path);
}

void StorageManifestMergeTree::initializeManifest()
{
    if (!manifest_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ManifestStorage is not initialized");

    auto disks = getDisks();
    if (disks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No disks available in storage policy");

    const DiskPtr & disk = disks[0];
    String local_disk_path = std::filesystem::path(disk->getPath()) / getRelativeDataPath();
    String remote_disk_path; // TODO: Get remote disk path if available

    auto name = fmt::format("{}_manifest", getStorageID().getTableName());
    manifest_disk
        = std::make_shared<DiskManifest>(name, manifest_storage_path, local_disk_path, remote_disk_path, manifest_storage, format_version);
}

String StorageManifestMergeTree::getManifestStoragePath() const
{
    auto disks = getDisks();
    if (disks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No disks available in storage policy");

    const DiskPtr & disk = disks[0];
    return fmt::format("{}/manifest/{}", disk->getPath(), getRelativeDataPath());
}

void StorageManifestMergeTree::commitToManifest(
    const DataPartPtr & part, ManifestOpType op_type, const std::optional<UUID> & part_uuid, const std::optional<String> & part_name) const
{
    String value;
    auto obj = serialize(part, op_type, part_uuid, part_name);
    obj.SerializeToString(&value);

    // part_uuid is optional, use it if provided, otherwise use part->uuid
    UUID uuid = part_uuid.has_value() ? *part_uuid : part->uuid;
    manifest_storage->put(toString(uuid), value);
}

void StorageManifestMergeTree::removeFromManifest(const String & part_uuid) const
{
    manifest_storage->del(part_uuid);
}

static inline Manifest::UUIDInfo convert(const UUID & uuid)
{
    Manifest::UUIDInfo res;
    res.set_low(uuid.toUnderType().items[0]);
    res.set_high(uuid.toUnderType().items[1]);
    return res;
}

static inline UUID convert(const Manifest::UUIDInfo & uuid)
{
    return UUID(UInt128{uuid.low(), uuid.high()});
}

static inline MergeTreeDataPartType convertType(Int32 v)
{
    if (v == MergeTreeDataPartType::Compact)
        return MergeTreeDataPartType::Compact;
    else if (v == MergeTreeDataPartType::Wide)
        return MergeTreeDataPartType::Wide;
    return MergeTreeDataPartType::Unknown;
}

Manifest::PartObject StorageManifestMergeTree::serialize(
    const DataPartPtr & part, ManifestOpType op_type, const std::optional<UUID> & part_uuid, const std::optional<String> & part_name) const
{
    Manifest::PartObject obj;
    obj.set_name(part_name.has_value() ? *part_name : part->name);
    *(obj.mutable_uuid()) = part_uuid.has_value() ? convert(*part_uuid) : convert(part->uuid);

    switch (op_type)
    {
        case ManifestOpType::PreCommit: {
            obj.set_state(static_cast<Int32>(DataPartState::Temporary));
            break;
        }
        case ManifestOpType::PreRemove: {
            obj.set_state(static_cast<Int32>(DataPartState::Deleting));
            break;
        }
        case ManifestOpType::Commit: {
            obj.set_state(static_cast<Int32>(DataPartState::Active));
            obj.set_type(static_cast<Int32>(part->getType().getValue()));
            // No need to access the disk
            std::time_t current_time = std::time(nullptr);
            obj.set_last_modify_time(current_time);
            obj.set_path(fs::path(relative_data_path) / part->name);
            obj.set_bytes(part->getBytesOnDisk());
            obj.set_rows(part->rows_count);
            obj.set_version(0);
            *(obj.mutable_table_uuid()) = convert(getStorageID().uuid);
            if (part->getParentPart())
                obj.set_projection_name(part->name);
            /// Disk information for multi-disk support
            /// Save both disk_name and disk_path for robustness:
            /// - disk_name: primary identifier (may change if config changes)
            /// - disk_path: fallback identifier (more stable, used for matching if name fails)
            {
                String disk_name = part->getDataPartStorage().getDiskName();
                obj.set_disk_name(disk_name);
                
                // Get disk path for fallback matching
                auto storage_policy = getStoragePolicy();
                DiskPtr disk = storage_policy->tryGetDiskByName(disk_name);
                if (disk)
                    obj.set_disk_path(disk->getPath());
                
                // Set disk_type based on whether it's a remote disk
                if (part->getDataPartStorage().isStoredOnRemoteDisk())
                    obj.set_disk_type(Manifest::DiskType::REMOTE);
                else
                    obj.set_disk_type(Manifest::DiskType::LOCAL);
            }
            /// Columns
            {
                auto * mutable_columns = obj.mutable_columns();
                WriteBufferFromOwnString columns_buffer;
                part->getColumns().writeText(columns_buffer);
                columns_buffer.finalize();

                WriteBufferFromOwnString serialization_buffer;
                part->getSerializationInfos().writeJSON(serialization_buffer);
                serialization_buffer.finalize();

                mutable_columns->set_columns(columns_buffer.str());
                mutable_columns->set_serialization_infos(serialization_buffer.str());
                mutable_columns->set_metadata_version(part->getMetadataVersion());
            }
            /// TTL information
            if (!part->ttl_infos.empty())
            {
                WriteBufferFromOwnString ttl_infos_buffer;
                part->ttl_infos.write(ttl_infos_buffer);
                ttl_infos_buffer.finalize();
                obj.set_ttl_infos(ttl_infos_buffer.str());
            }
            /// Default compression codec
            {
                obj.set_compress_codec(part->default_codec->getCodecDesc()->formatWithSecretsOneLine());
            }
            /// Checksums
            {
                auto * checksums = obj.mutable_checksums();

                for (const auto & [file_name, checksum] : part->checksums.files)
                {
                    Manifest::SimpleChecksum * simple_checksum = checksums->add_files();

                    simple_checksum->set_file_name(file_name);
                    simple_checksum->set_size(checksum.file_size);

                    auto * file_hash = simple_checksum->mutable_file_hash();
                    file_hash->set_high64(checksum.file_hash.high64);
                    file_hash->set_low64(checksum.file_hash.low64);

                    simple_checksum->set_is_compressed(checksum.is_compressed);
                    if (checksum.is_compressed)
                    {
                        simple_checksum->set_uncompressed_size(checksum.uncompressed_size);
                        auto * uncompressed_hash = simple_checksum->mutable_uncompressed_hash();
                        uncompressed_hash->set_high64(checksum.uncompressed_hash.high64);
                        uncompressed_hash->set_low64(checksum.uncompressed_hash.low64);
                    }
                }
            }
            /// Partition
            {
                WriteBufferFromOwnString partition_buffer;
                part->partition.store(part->storage, partition_buffer);
                partition_buffer.finalize();
                obj.set_partition(partition_buffer.str());
            }
            /// MinMax
            if (!part->isEmpty() && !part->getParentPart())
            {
                WriteBufferFromOwnString minmax_buffer;
                part->minmax_idx->store(part->storage, {}, minmax_buffer);
                minmax_buffer.finalize();
                obj.set_minmax(minmax_buffer.str());
            }
            /// Index Granularity
            {
                auto * index_granularity = obj.mutable_index_granularity();
                if (part->index_granularity_info.mark_type.adaptive)
                {
                    index_granularity->mutable_marks_rows_partial_sums()->Add(
                        part->index_granularity->getMarksRowsPartialSums().begin(),
                        part->index_granularity->getMarksRowsPartialSums().end());
                }
                else
                {
                    index_granularity->set_is_constant(true);
                    index_granularity->add_marks_rows_partial_sums(part->index_granularity->getConstantGranularity().value());
                    index_granularity->add_marks_rows_partial_sums(part->index_granularity->getLastMarkRows());
                    index_granularity->add_marks_rows_partial_sums(part->index_granularity->getLastNonFinalMarkRows());
                    index_granularity->add_marks_rows_partial_sums(part->index_granularity->hasFinalMark());
                }
            }
            /// Index Granularity Info
            {
                auto * info = obj.mutable_index_granularity()->mutable_info();
                info->set_fixed_index_granularity(part->index_granularity_info.fixed_index_granularity);
                info->set_index_granularity_bytes(part->index_granularity_info.index_granularity_bytes);
                info->set_adaptive(part->index_granularity_info.mark_type.adaptive);
                info->set_compressed(part->index_granularity_info.mark_type.compressed);
            }
            /// Version
            {
                auto * version_data = obj.mutable_version_metadata();
                version_data->mutable_creation_tid()->set_local_tid(part->version.creation_tid.local_tid);
                version_data->mutable_creation_tid()->set_start_csn(part->version.creation_tid.start_csn);
                *(version_data->mutable_creation_tid()->mutable_host_id()) = convert(part->version.creation_tid.host_id);

                version_data->mutable_removal_tid()->set_local_tid(part->version.removal_tid.local_tid);
                version_data->mutable_removal_tid()->set_start_csn(part->version.removal_tid.start_csn);
                *(version_data->mutable_removal_tid()->mutable_host_id()) = convert(part->version.removal_tid.host_id);

                version_data->set_removal_tid_lock(part->version.removal_tid_lock);
                version_data->set_creation_csn(part->version.creation_csn);
                version_data->set_removal_csn(part->version.removal_csn);
            }

            if (!part->getProjectionParts().empty())
            {
                for (const auto & proj_part : part->getProjectionParts())
                {
                    auto * proj_part_obj = obj.mutable_projections()->Add();
                    *proj_part_obj = serialize(proj_part.second, ManifestOpType::Commit, std::nullopt, std::nullopt);
                }
            }

            /// Primary Index
            {
                String index_extension = ".idx";
                if (part->getDataPartStorage().existsFile("primary.cidx"))
                    index_extension = ".cidx";

                String primary_index_file_name = "primary" + index_extension;
                auto primary_index_data = [&]() -> std::string
                {
                    std::string content;
                    if (part->getDataPartStorage().existsFile(primary_index_file_name))
                    {
                        size_t file_size = part->getDataPartStorage().getFileSize(primary_index_file_name);
                        auto buffer = part->getDataPartStorage().readFile(
                            primary_index_file_name, ReadSettings().adjustBufferSize(file_size), std::optional<size_t>(file_size));
                        readStringUntilEOF(content, *buffer);
                    }
                    return content;
                }();

                bool primary_index_is_compressed = (index_extension == ".cidx");
                obj.set_primary(primary_index_data);
                obj.set_primary_compressed(primary_index_is_compressed);
            }

            break;
        }
    }

    return obj;
}

DiskPtr StorageManifestMergeTree::findDiskForPart(
    const Manifest::PartObject & part_object,
    const String & part_name,
    const String & part_uuid_str) const
{
    /// Find disk using manifest information with minimal IO:
    /// 1. Fast path: try saved disk_name (no IO, just lookup)
    /// 2. Fallback: try saved disk_path matching (no IO, just string comparison)
    /// 3. Last resort: search all disks by UUID (only if both above fail, rare case)
    /// 
    /// This preserves manifest's advantage of avoiding disk iteration in most cases.
    /// Also includes self-healing: updates manifest if disk info changed.
    DiskPtr part_disk_ptr = nullptr;
    auto storage_policy = getStoragePolicy();
    
    // Fast path 1: Try by saved disk_name (no IO)
    if (!part_object.disk_name().empty())
    {
        part_disk_ptr = storage_policy->tryGetDiskByName(part_object.disk_name());
        if (part_disk_ptr)
        {
            LOG_TRACE(log, "Found disk '{}' for part {} by saved name", part_disk_ptr->getName(), part_name);
        }
    }
    
    // Fast path 2: If name lookup failed, try by saved disk_path (no IO, just string comparison)
    if (!part_disk_ptr && !part_object.disk_path().empty())
    {
        String saved_disk_path = part_object.disk_path();
        // Normalize paths for comparison
        if (!saved_disk_path.empty() && saved_disk_path.back() == '/')
            saved_disk_path.pop_back();
            
        for (const auto & disk : storage_policy->getDisks())
        {
            if (disk->isBroken())
                continue;
                
            String disk_path = disk->getPath();
            if (!disk_path.empty() && disk_path.back() == '/')
                disk_path.pop_back();
                
            if (disk_path == saved_disk_path)
            {
                part_disk_ptr = disk;
                LOG_INFO(log, "Found disk '{}' for part {} by saved path '{}' (name changed from '{}')", 
                         disk->getName(), part_name, saved_disk_path,
                         part_object.disk_name().empty() ? "<none>" : part_object.disk_name());
                
                // Update manifest with new disk name (self-healing: fix manifest for next time)
                if (!part_object.disk_name().empty() && part_object.disk_name() != disk->getName())
                {
                    try
                    {
                        Manifest::PartObject updated_obj = part_object;
                        updated_obj.set_disk_name(disk->getName());
                        updated_obj.set_disk_path(disk->getPath());
                        String updated_value;
                        updated_obj.SerializeToString(&updated_value);
                        manifest_storage->put(part_uuid_str, updated_value);
                        LOG_INFO(log, "Updated manifest for part {}: disk name '{}' -> '{}'", 
                                part_name, part_object.disk_name(), disk->getName());
                    }
                    catch (...)
                    {
                        // Don't fail part loading if manifest update fails
                        LOG_WARNING(log, "Failed to update manifest for part {}: {}", 
                                   part_name, getCurrentExceptionMessage(false));
                    }
                }
                break;
            }
        }
    }
    
    // Last resort: Search all disks by UUID directory (only if both fast paths failed)
    // This should be rare (only when both name and path changed)
    if (!part_disk_ptr)
    {
        LOG_WARNING(log, "Fast paths failed for part {} (name: '{}', path: '{}'), searching all disks by UUID", 
                   part_name,
                   part_object.disk_name().empty() ? "<none>" : part_object.disk_name(),
                   part_object.disk_path().empty() ? "<none>" : part_object.disk_path());
                   
        String uuid_path = fs::path(relative_data_path) / part_uuid_str;
        for (const auto & disk : storage_policy->getDisks())
        {
            if (disk->isBroken())
                continue;
                
            if (disk->existsDirectory(uuid_path))
            {
                part_disk_ptr = disk;
                LOG_INFO(log, "Found part {} on disk '{}' by UUID search", part_name, disk->getName());
                
                // Update manifest with new disk name and path (self-healing: fix manifest for next time)
                // This handles cases where both name and path changed
                bool needs_update = false;
                if (part_object.disk_name().empty() || part_object.disk_name() != disk->getName())
                    needs_update = true;
                if (part_object.disk_path().empty() || part_object.disk_path() != disk->getPath())
                    needs_update = true;
                    
                if (needs_update)
                {
                    try
                    {
                        Manifest::PartObject updated_obj = part_object;
                        updated_obj.set_disk_name(disk->getName());
                        updated_obj.set_disk_path(disk->getPath());
                        String updated_value;
                        updated_obj.SerializeToString(&updated_value);
                        manifest_storage->put(part_uuid_str, updated_value);
                        LOG_INFO(log, "Updated manifest for part {}: disk '{}' (path: '{}')", 
                                part_name, disk->getName(), disk->getPath());
                    }
                    catch (...)
                    {
                        // Don't fail part loading if manifest update fails
                        LOG_WARNING(log, "Failed to update manifest for part {}: {}", 
                                   part_name, getCurrentExceptionMessage(false));
                    }
                }
                break;
            }
        }
    }
    
    // If still not found, the part data is missing
    if (!part_disk_ptr)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
                       "Part {} (UUID: {}) not found on any disk in storage policy. "
                       "Saved disk name: '{}', path: '{}'",
                       part_name, part_uuid_str,
                       part_object.disk_name().empty() ? "<none>" : part_object.disk_name(),
                       part_object.disk_path().empty() ? "<none>" : part_object.disk_path());
    }
    
    return part_disk_ptr;
}

void StorageManifestMergeTree::deserialize(const Manifest::PartObject & obj, MutableDataPartPtr & part) const
{
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    /// loadUUID
    part->uuid = convert(obj.uuid());

    /// loadChecksums
    {
        MergeTreeDataPartChecksums checksums;

        for (const auto & file : obj.checksums().files())
        {
            MergeTreeDataPartChecksum checksum;

            checksum.file_size = file.size();
            checksum.file_hash.high64 = file.file_hash().high64();
            checksum.file_hash.low64 = file.file_hash().low64();
            checksum.is_compressed = file.is_compressed();
            checksum.uncompressed_size = file.uncompressed_size();
            checksum.uncompressed_hash.high64 = file.uncompressed_hash().high64();
            checksum.uncompressed_hash.low64 = file.uncompressed_hash().low64();

            checksums.files.insert({file.file_name(), checksum});
        }
        part->checksums = checksums;
    }

    /// loadProjections
    if (!part->getParentPart())
    {
        if (!obj.projections().empty())
        {
            auto data_storage = part->getDataPartStoragePtr();
            for (const auto & proj : obj.projections())
            {
                auto path = proj.projection_name() + ".proj";
                if (!data_storage->existsFile(path))
                    continue;
                auto proj_storage = part->getDataPartStorage().getProjection(path);
                MergeTreeDataPartBuilder builder(part->storage, proj.projection_name(), proj_storage, getReadSettings());
                builder.withPartInfo({"all", 0, 0, 0}).withParentPart(part.get()).withPartFormat(part->getFormat());
                auto proj_part = builder.build();
                deserialize(proj, proj_part);
                proj_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
                proj_part->version.creation_csn = Tx::PrehistoricCSN;
                part->addProjectionPart(proj.projection_name(), std::move(proj_part));
            }
        }
    }

    part->setBytesOnDisk(obj.bytes());

    /// loadColumns
    {
        auto metadata_snapshot = part->storage.getInMemoryMetadataPtr();
        if (part->getParentPart())
            metadata_snapshot = metadata_snapshot->projections.get(obj.projection_name()).metadata;

        const auto & columns_obj = obj.columns();
        NamesAndTypesList loaded_columns;
        {
            ReadBufferFromString in(columns_obj.columns());
            loaded_columns.readText(in);

            for (auto & column : loaded_columns)
                setVersionToAggregateFunctions(column.type, true);
        }

        SerializationInfoByName infos({});
        if (!columns_obj.serialization_infos().empty())
        {
            ReadBufferFromString in(columns_obj.serialization_infos());
            infos = SerializationInfoByName::readJSON(loaded_columns, in);
        }

        int32_t loaded_metadata_version = columns_obj.metadata_version();
        part->setColumns(loaded_columns, infos, loaded_metadata_version);
    }

    /// loadIndexGranularity
    {
        /// IndexGranularityInfo
        {
            const auto & index_granularity = obj.index_granularity();
            part->index_granularity_info.mark_type.part_type = convertType(obj.type()).getValue();
            part->index_granularity_info.mark_type.adaptive = index_granularity.info().adaptive();
            part->index_granularity_info.mark_type.compressed = index_granularity.info().compressed();
            part->index_granularity_info.index_granularity_bytes = index_granularity.info().index_granularity_bytes();
            part->index_granularity_info.fixed_index_granularity = index_granularity.info().fixed_index_granularity();
        }
        /// IndexGranularity
        {
            const auto & index_granularity = obj.index_granularity();
            if (index_granularity.is_constant())
            {
                size_t constant_gran = index_granularity.marks_rows_partial_sums(0);
                size_t last_mark = index_granularity.marks_rows_partial_sums(1);
                size_t last_non_final = index_granularity.marks_rows_partial_sums(2);
                bool has_final = index_granularity.marks_rows_partial_sums(3) != 0;

                part->index_granularity
                    = std::make_shared<MergeTreeIndexGranularityConstant>(constant_gran, last_mark, last_non_final, has_final);
            }
            else
            {
                std::vector<size_t> sums;
                sums.reserve(index_granularity.marks_rows_partial_sums_size());
                for (int i = 0; i < index_granularity.marks_rows_partial_sums_size(); ++i)
                {
                    sums.push_back(index_granularity.marks_rows_partial_sums(i));
                }
                part->index_granularity = std::make_shared<MergeTreeIndexGranularityAdaptive>(sums);
            }
        }
    }

    part->calculateColumnsAndSecondaryIndicesSizesOnDisk();

    /// loadIndex
    {
        auto metadata_snapshot = getInMemoryMetadataPtr();
        if (part->getParentPart())
            metadata_snapshot = metadata_snapshot->projections.get(part->name).metadata;
        const auto & primary_key = metadata_snapshot->getPrimaryKey();
        size_t key_size = primary_key.column_names.size();

        if (key_size)
        {
            MutableColumns loaded_index;
            loaded_index.resize(key_size);

            for (size_t i = 0; i < key_size; ++i)
            {
                loaded_index[i] = primary_key.data_types[i]->createColumn();
                loaded_index[i]->reserve(part->index_granularity->getMarksCount());
            }

            auto create_read_buffer = [&](auto & obj_) -> std::unique_ptr<ReadBuffer>
            {
                auto res = std::make_unique<ReadBufferFromMemory>(obj_.primary());
                if (obj_.primary_compressed())
                    return std::make_unique<CompressedReadBufferFromFile>(std::make_unique<ReadBufferFromFileDecorator>(std::move(res)));
                return res;
            };
            auto index_file = create_read_buffer(obj);
            size_t marks_count = part->index_granularity->getMarksCount();
            Serializations key_serializations(key_size);
            for (size_t j = 0; j < key_size; ++j)
                key_serializations[j] = primary_key.data_types[j]->getDefaultSerialization();

            for (size_t i = 0; i < marks_count; ++i)
                for (size_t j = 0; j < key_size; ++j)
                    key_serializations[j]->deserializeBinary(*loaded_index[j], *index_file, {});

            for (size_t i = 0; i < key_size; ++i)
            {
                loaded_index[i]->shrinkToFit();
                loaded_index[i]->protect();
                if (loaded_index[i]->size() != marks_count)
                    throw Exception(
                        ErrorCodes::CANNOT_READ_ALL_DATA,
                        "Cannot read all data from index file {}(expected size: "
                        "{}, read: {})",
                        obj.name(),
                        marks_count,
                        loaded_index[i]->size());
            }
            Columns index_columns;
            index_columns.reserve(key_size);
            for (auto & column : loaded_index)
                index_columns.push_back(std::move(column));
            part->setIndex(std::move(index_columns));
        }
    }

    /// loadRowsCount
    part->rows_count = obj.rows();

    /// loadPartitionAndMinMaxIndex
    {
        if (part->storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING && !part->getParentPart())
        {
            DayNum min_date;
            DayNum max_date;
            MergeTreePartInfo::parseMinMaxDatesFromPartName(part->name, min_date, max_date);

            const auto & date_lut = DateLUT::serverTimezoneInstance();
            part->partition = MergeTreePartition(date_lut.toNumYYYYMM(min_date));
            part->minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>(min_date, max_date);
        }
        else
        {
            if (!part->getParentPart())
            {
                auto metadata_snapshot = part->storage.getInMemoryMetadataPtr();
                if (metadata_snapshot->hasPartitionKey())
                {
                    const auto & partition_key_sample
                        = MergeTreePartition::adjustPartitionKey(metadata_snapshot, part->storage.getContext()).sample_block;
                    ReadBufferFromString file(obj.partition());
                    part->partition.value.resize(partition_key_sample.columns());
                    for (size_t i = 0; i < partition_key_sample.columns(); ++i)
                        partition_key_sample.getByPosition(i).type->getDefaultSerialization()->deserializeBinary(
                            part->partition.value[i], file, {});
                }
            }

            if (!part->isEmpty())
            {
                if (part->getParentPart())
                    // projection parts don't have minmax_idx, and it's always initialized
                    part->minmax_idx->initialized = true;
                else
                {
                    auto metadata_snapshot = getInMemoryMetadataPtr();
                    const auto & partition_key = metadata_snapshot->getPartitionKey();

                    auto minmax_column_names = getMinMaxColumnsNames(partition_key);
                    auto minmax_column_types = getMinMaxColumnsTypes(partition_key);
                    size_t minmax_idx_size = minmax_column_types.size();

                    part->minmax_idx->hyperrectangle.reserve(minmax_idx_size);
                    ReadBufferFromString file(obj.minmax());
                    for (int i = 0; i < static_cast<int>(minmax_idx_size); ++i)
                    {
                        auto serialization = minmax_column_types[i]->getDefaultSerialization();

                        Field min_val;
                        serialization->deserializeBinary(min_val, file, {});
                        Field max_val;
                        serialization->deserializeBinary(max_val, file, {});

                        // NULL_LAST
                        if (min_val.isNull())
                            min_val = POSITIVE_INFINITY;
                        if (max_val.isNull())
                            max_val = POSITIVE_INFINITY;

                        part->minmax_idx->hyperrectangle.emplace_back(min_val, true, max_val, true);
                    }
                    part->minmax_idx->initialized = true;
                }
            }
        }

        if (!part->getParentPart())
        {
            auto metadata_snapshot = part->storage.getInMemoryMetadataPtr();
            String calculated_partition_id = part->partition.getID(metadata_snapshot->getPartitionKey().sample_block);
            if (calculated_partition_id != part->info.getPartitionId())
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "While loading part {}: calculated partition ID: {} differs from partition ID in part name: {}",
                    obj.name(),
                    calculated_partition_id,
                    part->info.getPartitionId());
        }
    }

    if (!part->getParentPart())
    {
        /// loadTTLInfos
        {
            if (obj.has_ttl_infos() && !obj.ttl_infos().empty())
            {
                ReadBufferFromString in(obj.ttl_infos());
                assertString("ttl format version: ", in);
                size_t format_version;
                readText(format_version, in);
                assertChar('\n', in);

                if (format_version == 1)
                {
                    try
                    {
                        part->ttl_infos.read(in);
                    }
                    catch (const JSONException &)
                    {
                        throw Exception(ErrorCodes::BAD_TTL_FILE, "Error while parsing file ttl.txt in part: {}", part->name);
                    }
                }
                else
                    throw Exception(ErrorCodes::BAD_TTL_FILE, "Unknown ttl format version: {}", toString(format_version));
            }
        }
    }

    /// loadCompressionCodec
    {
        const auto & codec_line = obj.compress_codec();
        try
        {
            part->default_codec = CompressionCodecFactory::instance().get(codec_line);
        }
        catch (const DB::Exception &)
        {
            throw;
        }
    }

    /// loadVersionMetadata
    {
        const auto & version_data = obj.version_metadata();
        part->version.creation_tid.local_tid = version_data.creation_tid().local_tid();
        part->version.creation_tid.start_csn = version_data.creation_tid().start_csn();
        part->version.creation_tid.host_id = convert(version_data.creation_tid().host_id());

        part->version.removal_tid.local_tid = version_data.removal_tid().local_tid();
        part->version.removal_tid.start_csn = version_data.removal_tid().start_csn();
        part->version.removal_tid.host_id = convert(version_data.removal_tid().host_id());

        part->version.removal_tid_lock = version_data.removal_tid_lock();
        part->version.creation_csn = version_data.creation_csn();
        part->version.removal_csn = version_data.removal_csn();
    }
}

MergeTreeData::LoadPartResult StorageManifestMergeTree::loadDataPart(
    const MergeTreePartInfo & part_info,
    const String & part_name,
    const DiskPtr & part_disk,
    MergeTreeDataPartState to_state,
    DB::SharedMutex & part_loading_mutex)
{
    LoadPartResult res;
    auto disk_manifest = std::dynamic_pointer_cast<DiskManifest>(part_disk);
    auto part_object_iter = disk_manifest->part_map.find(part_name);

    const Manifest::PartObject & part_object = part_object_iter->second;
    UUID part_uuid = convert(part_object.uuid());
    String part_uuid_str = toString(part_uuid);

    DiskPtr part_disk_ptr = findDiskForPart(part_object, part_name, part_uuid_str);

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, part_disk_ptr, 0);
    auto data_part_storage = std::make_shared<DataPartStorageOnDiskFull>(single_disk_volume, relative_data_path, toString(part_uuid));
    auto mark_broken = [&]
    {
        if (!res.part)
        {
            /// Build a fake part and mark it as broken in case of filesystem error.
            res.part = getDataPartBuilder(part_name, single_disk_volume, toString(part_uuid), getReadSettings())
                           .withPartStorageType(MergeTreeDataPartStorageType::Full)
                           .withPartType(MergeTreeDataPartType::Wide)
                           .build();
        }

        res.part->uuid = part_uuid;
        res.is_broken = true;
        res.size_of_part = part_object.bytes();
    };

    try
    {
        res.part = getDataPartBuilder(part_name, single_disk_volume, toString(part_uuid), getReadSettings())
                       .withPartInfo(part_info)
                       .withPartFormatFromDisk()
                       .build();
    }
    catch (...)
    {
        /// Don't count the part as broken if there was a retryalbe error
        /// during loading, such as "not enough memory" or network error.
        if (isRetryableException(std::current_exception()))
            throw;
        LOG_DEBUG(log, "Failed to load data part {}, unknown exception", part_name);
        mark_broken();
        return res;
    }

    try
    {
        auto state = magic_enum::enum_cast<MergeTreeDataPartState>(part_object.state());
        if (!state.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted manifest part state: {}", part_object.state());

        switch (state.value())
        {
            case DataPartState::Active: {
                /// NOTE: This is needed or else part cannot be cleaned with "Part maybe visible for transactions"
                res.part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
                res.part->version.creation_csn = Tx::PrehistoricCSN;
                res.part->setState(to_state);
                res.size_of_part = part_object.bytes();
                res.part->modification_time = part_object.last_modify_time();
                deserialize(part_object, res.part);

                break;
            }
            case DataPartState::Temporary: {
                res.part->loadColumnsChecksumsIndexes(require_part_metadata, true);
                /// Ensure part restored via redo is fully correct
                bool is_broken_projection = false;
                checkDataPart(res.part, true, is_broken_projection);
                commitToManifest(res.part, ManifestOpType::Commit, std::nullopt, std::nullopt);

                res.part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
                res.part->version.creation_csn = Tx::PrehistoricCSN;
                res.part->setState(to_state);

                res.part->modification_time
                    = part_disk_ptr->getLastModified(fs::path(relative_data_path) / toString(part_uuid)).epochTime();
                break;
            }
            default: {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Corrupted manifest part state: {}", magic_enum::enum_name(state.value()));
            }
        }
    }
    catch (...)
    {
        /// Don't count the part as broken if there was a retryalbe error
        /// during loading, such as "not enough memory" or network error.
        if (isRetryableException(std::current_exception()))
            throw;

        LOG_WARNING(log, "Part {}/{} is considered broken and will be removed from manifest and storage", part_uuid, res.part->name);
        tryLogCurrentException(__PRETTY_FUNCTION__);

        mark_broken();
        return res;
    }

    DataPartIteratorByInfo it;
    bool inserted;

    {
        std::lock_guard lock(part_loading_mutex);
        LOG_TEST(log, "loadDataPart: inserting {} into data_parts_indexes", res.part->getNameWithState());
        std::tie(it, inserted) = data_parts_indexes.insert(res.part);
    }

    /// Remove duplicate parts with the same checksum.
    if (!inserted)
    {
        if ((*it)->checksums.getTotalChecksumHex() == res.part->checksums.getTotalChecksumHex())
        {
            LOG_ERROR(log, "Remove duplicate part {}", data_part_storage->getFullPath());
            res.part->is_duplicate = true;
            return res;
        }
        else
            throw Exception(ErrorCodes::DUPLICATE_DATA_PART, "Part {} already exists but with different checksums", res.part->name);
    }

    if (res.part->getState() == DataPartState::Active)
    {
        addPartContributionToDataVolume(res.part);
        addPartContributionToUncompressedBytesInPatches(res.part);
    }

    if (res.part->hasLightweightDelete())
        has_lightweight_delete_parts.store(true);

    return res;
}

} // namespace DB
