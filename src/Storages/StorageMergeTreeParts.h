#pragma once
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;


/// Reads a set of MergeTree data parts described explicitly (by path, type and mark ranges)
/// from a disk, without any of the table metadata that `MergeTree` normally keeps.
/// It is the storage behind the `mergeTreeParts` table function.
class StorageMergeTreeParts final : public IStorage, public WithContext
{
public:
    /// Persistent virtual columns of MergeTree parts.
    static VirtualColumnsDescription createVirtuals();

    struct ReadFromPartsInfo
    {
        struct ReadFromPart
        {
            MergeTreeDataPartType type;
            std::string relative_path;
            std::string partition_id;
            size_t marks_count = 0;
            bool has_lightweight_delete = false;
            MarkRanges ranges;
        };

        using ReadFromParts = std::vector<ReadFromPart>;

        ReadFromParts parts;
        DiskPtr disk;

        size_t index_granularity_bytes = 0;
    };

    StorageMergeTreeParts(
        const ReadFromPartsInfo & read_from_parts_info_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_);

    std::string getName() const override { return "MergeTreeParts"; }

    bool supportsPrewhere() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    ReadFromPartsInfo read_from_parts_info;
};

}
