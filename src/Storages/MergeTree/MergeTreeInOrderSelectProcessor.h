#pragma once
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

namespace DB
{


/// Used to read data from single part with select query in order of primary key.
/// Cares about PREWHERE, virtual columns, indexes etc.
/// To read data from multiple parts, Storage (MergeTree) creates multiple such objects.
class MergeTreeInOrderSelectProcessor : public MergeTreeSelectProcessor
{
public:
    MergeTreeInOrderSelectProcessor(
        const MergeTreeData & storage,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData::DataPartPtr & owned_data_part,
        UInt64 max_block_size_rows,
        size_t preferred_block_size_bytes,
        size_t preferred_max_column_in_block_size_bytes,
        Names required_columns_,
        MarkRanges mark_ranges,
        bool use_uncompressed_cache,
        const PrewhereInfoPtr & prewhere_info,
        ExpressionActionsSettings actions_settings,
        bool check_columns,
        const MergeTreeReaderSettings & reader_settings,
        const Names & virt_column_names = {},
        bool has_limit_below_one_block_ = false,
        bool quiet = false);

        String getName() const override { return "MergeTreeInOrder"; }

private:
    bool getNewTask() override;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeInOrderSelectProcessor");
};

}
