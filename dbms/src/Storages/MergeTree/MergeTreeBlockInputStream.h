#pragma once
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeThreadBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>

namespace DB
{


/// Used to read data from single part.
/// To read data from multiple parts, a Storage creates multiple such objects.
/// TODO: Make special lightweight version of the reader for merges and other utilites, remove this from SelectExecutor.
class MergeTreeBlockInputStream : public MergeTreeBaseBlockInputStream
{
public:
    MergeTreeBlockInputStream(
        MergeTreeData & storage,
        const MergeTreeData::DataPartPtr & owned_data_part,
        size_t max_block_size_rows,
        size_t preferred_block_size_bytes,
        size_t preferred_max_column_in_block_size_bytes,
        Names column_names,
        const MarkRanges & mark_ranges,
        bool use_uncompressed_cache,
        ExpressionActionsPtr prewhere_actions,
        String prewhere_column,
        bool check_columns,
        size_t min_bytes_to_use_direct_io,
        size_t max_read_buffer_size,
        bool save_marks_in_cache,
        const Names & virt_column_names = {},
        size_t part_index_in_query = 0,
        bool quiet = false);

    ~MergeTreeBlockInputStream() override;

    String getName() const override { return "MergeTree"; }

    Block getHeader() const override;

    /// Closes readers and unlock part locks
    void finish();

protected:

    bool getNewTask() override;

private:
    Block header;

    /// Used by Task
    Names ordered_names;
    NameSet column_name_set;
    NamesAndTypesList columns;
    NamesAndTypesList pre_columns;

    /// Data part will not be removed if the pointer owns it
    MergeTreeData::DataPartPtr data_part;
    /// Forbids to change columns list of the part during reading
    std::shared_lock<std::shared_mutex> part_columns_lock;

    /// Mark ranges we should read (in ascending order)
    MarkRanges all_mark_ranges;
    /// Value of _part_index virtual column (used only in SelectExecutor)
    size_t part_index_in_query = 0;

    bool check_columns;
    String path;
    bool is_first_task = true;

    Logger * log = &Logger::get("MergeTreeBlockInputStream");
};

}
