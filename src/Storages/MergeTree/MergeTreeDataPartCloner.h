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
        const WriteSettings & write_settings);

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
        const IDataPartStorage::ClonePartParams & params);
};

}
