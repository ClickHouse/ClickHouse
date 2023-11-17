#pragma once

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct MergeTreePartition;
class IMergeTreeDataPart;

class MergeTreeDataPartCloner
{
public:

    using DataPart = IMergeTreeDataPart;
    using MutableDataPartPtr = std::shared_ptr<DataPart>;
    using DataPartPtr = std::shared_ptr<const DataPart>;

    static std::pair<MutableDataPartPtr, scope_guard> clone(
        MergeTreeData * merge_tree_data,
        const DataPartPtr & src_part,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreePartInfo & dst_part_info,
        const String & tmp_part_prefix,
        bool require_part_metadata,
        const IDataPartStorage::ClonePartParams & params,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings
    );

    static std::pair<MutableDataPartPtr, scope_guard> cloneWithDistinctPartitionExpression(
        MergeTreeData * merge_tree_data,
        const DataPartPtr & src_part,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreePartInfo & dst_part_info,
        const String & tmp_part_prefix,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        const MergeTreePartition & new_partition,
        const IMergeTreeDataPart::MinMaxIndex & new_min_max_index,
        bool sync_new_files,
        const IDataPartStorage::ClonePartParams & params
    );

private:
    /// Check that the storage policy contains the disk where the src_part is located.
    static bool doesStoragePolicyAllowSameDisk(
        MergeTreeData * merge_tree_data,
        const DataPartPtr & src_part
    );

    static std::pair<MutableDataPartPtr , scope_guard> cloneSourcePart(
        MergeTreeData * merge_tree_data,
        const DataPartPtr & src_part,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreePartInfo & dst_part_info,
        const String & tmp_part_prefix,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        const DB::IDataPartStorage::ClonePartParams & params
    );

    static void reserveSpaceOnDisk(const DataPartPtr & src_part);

    /// If source part is in memory, flush it to disk and clone it already in on-disk format
    static DataPartStoragePtr flushPartStorageToDiskIfInMemory(
        MergeTreeData * merge_tree_data,
        const DataPartPtr & src_part,
        const StorageMetadataPtr & metadata_snapshot,
        const String & tmp_part_prefix,
        const String & tmp_dst_part_name,
        scope_guard & src_flushed_tmp_dir_lock,
        MutableDataPartPtr src_flushed_tmp_part
    );

    static std::shared_ptr<IDataPartStorage> hardlinkAllFiles(
        MergeTreeData * merge_tree_data,
        const DB::ReadSettings & read_settings,
        const DB::WriteSettings & write_settings,
        const DataPartStoragePtr & storage,
        const String & path,
        const DB::IDataPartStorage::ClonePartParams & params
    );

    static void handleHardLinkedParameterFiles(
        const DataPartPtr & src_part,
        const DB::IDataPartStorage::ClonePartParams & params
    );

    static void handleProjections(
        const DataPartPtr & src_part,
        const DB::IDataPartStorage::ClonePartParams & params
    );

    static MutableDataPartPtr finalizePart(
        const MutableDataPartPtr & dst_part,
        const DB::IDataPartStorage::ClonePartParams & params,
        bool require_part_metadata
    );

    static Poco::Logger * log;
};

}
